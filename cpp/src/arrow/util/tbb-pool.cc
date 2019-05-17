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

#define TBB_PREVIEW_LOCAL_OBSERVER 1
#include <tbb/tbb.h>

// XXX implement fork safety
#if TBB_INTERFACE_VERSION >= 9106
#define TSI_INIT(count) tbb::task_scheduler_init(count)
#define TSI_TERMINATE(tsi) tsi->blocking_terminate(std::nothrow)
#else
#if __TBB_SUPPORTS_WORKERS_WAITING_IN_TERMINATE
#define TSI_INIT(count) tbb::task_scheduler_init(count, 0, /*blocking termination*/true)
#define TSI_TERMINATE(tsi) tsi->terminate()
#else
#error This version of TBB does not support blocking terminate
#endif
#endif

namespace arrow {
namespace internal {

struct ThreadPool::State : tbb::task_arena, tbb::task_scheduler_observer {
  State(int capacity = 0)
  : tbb::task_arena(capacity)
  , tbb::task_scheduler_observer((tbb::task_arena&)*this)
  , desired_capacity_(capacity)
  , active_workers_task_(new( tbb::task::allocate_root() ) tbb::empty_task) {
    DCHECK(active_workers_task_);
    active_workers_task_->set_ref_count(1);
    observe(true);
  }

  ~State() {
    observe(false);
    // no notifications accessing this instance are allowed even if there are still threads in arena
    active_workers_task_->set_ref_count(0); // reset to avoid assert
    tbb::task::destroy(*active_workers_task_);
  }


  void on_scheduler_entry( bool /*worker*/ ) {
    active_workers_task_->increment_ref_count();
  }
  void on_scheduler_exit( bool /*worker*/ ) {
    active_workers_task_->decrement_ref_count();
  }

  // Desired number of threads
  int desired_capacity_;
  // task object for counting the number of active threads
  tbb::empty_task *active_workers_task_;
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
  return static_cast<int>(state_->max_concurrency());
}

Status ThreadPool::Shutdown(bool wait) {
  ProtectAgainstFork();
  if(state_->is_active()) {
    tbb::empty_task *t = state_->active_workers_task_;
    //printf("Shutting down with %d threads\n", int(t->ref_count()-1));
    if(wait) {
      t->increment_ref_count(); // wait for low-priority task to complete
      // warning: if there are any other low-priority tasks in the arena, it is not guaranteed they are waited
      // this is also not the most efficient implementation from TBB point of view, so better avoid relying on it for performance
      state_->enqueue([t]{t->decrement_ref_count();}, tbb::priority_low);
      while(t->ref_count() > 1)
        state_->execute([t]{
          t->decrement_ref_count(); // the waiting thread should not account for itself while waiting
          t->wait_for_all();       // cooperative waiting
          t->increment_ref_count();
        });
    }
    state_->terminate();
  }
  return Status::OK();
}

void ThreadPool::LaunchWorkersUnlocked(int threads) {
  state_->enqueue([](){});
}

Status ThreadPool::SpawnReal(std::function<void()> task) {
  ProtectAgainstFork();
  state_->enqueue(std::move(task));
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

////////////////////////////////////////////////////////////////////////
// TBB-based TaskGroup implementation

class ThreadedTaskGroup : protected tbb::task_group, public TaskGroup {
 public:
  explicit ThreadedTaskGroup(ThreadPool* thread_pool)
      : thread_pool_(thread_pool) {}

  ~ThreadedTaskGroup() noexcept {
    // Make sure all pending tasks are finished, so that dangling references
    // to this don't persist.
    ARROW_UNUSED(Finish());
  }

  void AppendReal(std::function<Status()> task) override {
    run([this, task]() {
        if( ok() ) {
          // exception will be caught in Finish()
          Status st = task();
          UpdateStatus(std::move(st));
        }
    });
  }

  Status current_status() override {
    if( ok() )
      return Status();
    tbb::spin_mutex::scoped_lock lock(mutex_);
    return status_;
  }

  bool ok() override { return status_.ok(); }

  Status Finish() override {
    /*auto s =*/ wait();
    //if(s == tbb::canceled)
    return status_;
  }

  int parallelism() override { return thread_pool_->GetCapacity(); }

  std::shared_ptr<TaskGroup> MakeSubGroup() override { return NULLPTR; }

protected:
  void UpdateStatus(Status&& st) {
    // Must be called unlocked, only locks on error
    if (ARROW_PREDICT_FALSE(!st.ok() && ok())) {
      tbb::spin_mutex::scoped_lock lock(mutex_);
      status_ &= std::move(st);
      cancel();
    }
  }

  // These members are usable unlocked
  ThreadPool* thread_pool_;
  tbb::spin_mutex mutex_;

  Status status_;  // uses lock for update
  //ThreadedTaskGroup* parent_ = nullptr;
};

std::shared_ptr<TaskGroup> TaskGroup::MakeThreaded(ThreadPool* thread_pool) {
  return std::shared_ptr<TaskGroup>(new ThreadedTaskGroup(thread_pool));
}

}  // namespace internal

int GetCpuThreadPoolCapacity() { return internal::GetCpuThreadPool()->GetCapacity(); }

Status SetCpuThreadPoolCapacity(int threads) {
  return internal::GetCpuThreadPool()->SetCapacity(threads);
}

}  // namespace arrow
