// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/thread-pool.h"
#include "arrow/util/task-group.h"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "arrow/util/io-util.h"
#include "arrow/util/logging.h"

#include <tbb/tbb.h>

namespace arrow {
namespace internal {

struct ThreadPool::State {
  State(int capacity = 0) : desired_capacity_(capacity), tbb_arena_(capacity) {}

  // Desired number of threads
  int desired_capacity_;
  tbb::task_arena tbb_arena_;
};

ThreadPool::ThreadPool()
    : sp_state_(std::make_shared<ThreadPool::State>()),
      state_(sp_state_.get()),
      shutdown_on_destroy_(true) {
#ifndef _WIN32
  pid_ = getpid();
#endif
}

ThreadPool::~ThreadPool() {
  if (shutdown_on_destroy_) {
    ARROW_UNUSED(Shutdown(false /* wait */));
  }
}

void ThreadPool::ProtectAgainstFork() {
#ifndef _WIN32
  pid_t current_pid = getpid();
  if (pid_ != current_pid) {
    // Reinitialize internal state in child process after fork()
    // Ideally we would use pthread_at_fork(), but that doesn't allow
    // storing an argument, hence we'd need to maintain a list of all
    // existing ThreadPools.
    int capacity = state_->desired_capacity_;

    auto new_state = std::make_shared<ThreadPool::State>(capacity);

    pid_ = current_pid;
    sp_state_ = new_state;
    state_ = sp_state_.get();
  }
#endif
}

Status ThreadPool::SetCapacity(int threads) {
  if (threads <= 0) {
	return Status::Invalid("ThreadPool capacity must be > 0");
  }
  ProtectAgainstFork();
  auto new_state = std::make_shared<ThreadPool::State>(threads);
  sp_state_ = new_state;
  state_ = sp_state_.get();
  return Status::OK();
}

int ThreadPool::GetCapacity() {
  ProtectAgainstFork();
  return state_->desired_capacity_;
}

int ThreadPool::GetActualCapacity() {
  ProtectAgainstFork();
  return static_cast<int>(state_->tbb_arena_.max_concurrency());
}

Status ThreadPool::Shutdown(bool wait) {
  ProtectAgainstFork();
  state_->tbb_arena_.terminate();
  return Status::OK();
}

void ThreadPool::CollectFinishedWorkersUnlocked() {
}

void ThreadPool::LaunchWorkersUnlocked(int threads) {
  state_->tbb_arena_.enqueue([](){});
}

Status ThreadPool::SpawnReal(std::function<void()> task) {
  ProtectAgainstFork();
  state_->tbb_arena_.enqueue(std::move(task));
  return Status::OK();
}

Status ThreadPool::Make(int threads, std::shared_ptr<ThreadPool>* out) {
  auto pool = std::shared_ptr<ThreadPool>(new ThreadPool());
  RETURN_NOT_OK(pool->SetCapacity(threads));
  *out = std::move(pool);
  return Status::OK();
}

// ----------------------------------------------------------------------
// Global thread pool

int ThreadPool::DefaultCapacity() {
  return tbb::task_scheduler_init::default_num_threads();
}

// Helper for the singleton pattern
std::shared_ptr<ThreadPool> ThreadPool::MakeCpuThreadPool() {
  std::shared_ptr<ThreadPool> pool;
  DCHECK_OK(ThreadPool::Make(ThreadPool::DefaultCapacity(), &pool));
  // On Windows, the global ThreadPool destructor may be called after
  // non-main threads have been killed by the OS, and hang in a condition
  // variable.
  // On Unix, we want to avoid leak reports by Valgrind.
#ifdef _WIN32
  pool->shutdown_on_destroy_ = false;
#endif
  return pool;
}

ThreadPool* GetCpuThreadPool() {
  static std::shared_ptr<ThreadPool> singleton = ThreadPool::MakeCpuThreadPool();
  return singleton.get();
}

#if 0
////////////////////////////////////////////////////////////////////////
// Serial TaskGroup implementation

class SerialTaskGroup : public TaskGroup {
 public:
  void AppendReal(std::function<Status()> task) override {
    DCHECK(!finished_);
    if (status_.ok()) {
      status_ &= task();
    }
  }

  Status current_status() override { return status_; }

  bool ok() override { return status_.ok(); }

  Status Finish() override {
    if (!finished_) {
      finished_ = true;
      if (parent_) {
        parent_->status_ &= status_;
      }
    }
    return status_;
  }

  int parallelism() override { return 1; }

  std::shared_ptr<TaskGroup> MakeSubGroup() override {
    auto child = new SerialTaskGroup();
    child->parent_ = this;
    return std::shared_ptr<TaskGroup>(child);
  }

 protected:
  Status status_;
  bool finished_ = false;
  SerialTaskGroup* parent_ = nullptr;
};

////////////////////////////////////////////////////////////////////////
// Threaded TaskGroup implementation

class ThreadedTaskGroup : public TaskGroup {
 public:
  explicit ThreadedTaskGroup(ThreadPool* thread_pool)
      : thread_pool_(thread_pool), nremaining_(0), ok_(true) {}

  ~ThreadedTaskGroup() override {
    // Make sure all pending tasks are finished, so that dangling references
    // to this don't persist.
    ARROW_UNUSED(Finish());
  }

  void AppendReal(std::function<Status()> task) override {
    // The hot path is unlocked thanks to atomics
    // Only if an error occurs is the lock taken
    if (ok_.load(std::memory_order_acquire)) {
      nremaining_.fetch_add(1, std::memory_order_acquire);
      Status st = thread_pool_->Spawn([this, task]() {
        if (ok_.load(std::memory_order_acquire)) {
          // XXX what about exceptions?
          Status st = task();
          UpdateStatus(std::move(st));
        }
        OneTaskDone();
      });
      UpdateStatus(std::move(st));
    }
  }

  Status current_status() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return status_;
  }

  bool ok() override { return ok_.load(); }

  Status Finish() override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!finished_) {
      cv_.wait(lock, [&]() { return nremaining_.load() == 0; });
      // Current tasks may start other tasks, so only set this when done
      finished_ = true;
      if (parent_) {
        parent_->OneTaskDone();
      }
    }
    return status_;
  }

  int parallelism() override { return thread_pool_->GetCapacity(); }

  std::shared_ptr<TaskGroup> MakeSubGroup() override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto child = new ThreadedTaskGroup(thread_pool_);
    child->parent_ = this;
    nremaining_.fetch_add(1, std::memory_order_acquire);
    return std::shared_ptr<TaskGroup>(child);
  }

 protected:
  void UpdateStatus(Status&& st) {
    // Must be called unlocked, only locks on error
    if (ARROW_PREDICT_FALSE(!st.ok())) {
      std::lock_guard<std::mutex> lock(mutex_);
      ok_.store(false, std::memory_order_release);
      status_ &= std::move(st);
    }
  }

  void OneTaskDone() {
    // Can be called unlocked thanks to atomics
    auto nremaining = nremaining_.fetch_sub(1, std::memory_order_release) - 1;
    DCHECK_GE(nremaining, 0);
    if (nremaining == 0) {
      // Take the lock so that ~ThreadedTaskGroup cannot destroy cv
      // before cv.notify_one() has returned
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.notify_one();
    }
  }

  // These members are usable unlocked
  ThreadPool* thread_pool_;
  std::atomic<int32_t> nremaining_;
  std::atomic<bool> ok_;

  // These members use locking
  std::mutex mutex_;
  std::condition_variable cv_;
  Status status_;
  bool finished_ = false;
  ThreadedTaskGroup* parent_ = nullptr;
};

std::shared_ptr<TaskGroup> TaskGroup::MakeSerial() {
  return std::shared_ptr<TaskGroup>(new SerialTaskGroup);
}

std::shared_ptr<TaskGroup> TaskGroup::MakeThreaded(ThreadPool* thread_pool) {
  return std::shared_ptr<TaskGroup>(new ThreadedTaskGroup(thread_pool));
}
#endif
}  // namespace internal

int GetCpuThreadPoolCapacity() { return internal::GetCpuThreadPool()->GetCapacity(); }

Status SetCpuThreadPoolCapacity(int threads) {
  return internal::GetCpuThreadPool()->SetCapacity(threads);
}

}  // namespace arrow
