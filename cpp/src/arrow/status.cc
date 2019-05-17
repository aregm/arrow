// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#include "arrow/status.h"

#include <assert.h>
#include <sstream>

namespace arrow {

Status::Status(StatusCode code, const std::string& msg) {
  assert(code != StatusCode::OK);
  auto s = new State;
  s->code = code;
  s->msg = msg;
  state_.store(s, std::memory_order_relaxed);
}

void Status::CopyFrom(const Status& s) {
  delete state_.load(std::memory_order_relaxed);
  if (s.state_.load(std::memory_order_acquire) == nullptr) {
    state_.store(nullptr, std::memory_order_relaxed);
  } else {
    state_.store(new State(*s.state_), std::memory_order_relaxed);
  }
}

std::string Status::CodeAsString() const {
  if (state_.load(std::memory_order_acquire) == nullptr) {
    return "OK";
  }

  const char* type;
  switch (code()) {
    case StatusCode::OK:
      type = "OK";
      break;
    case StatusCode::OutOfMemory:
      type = "Out of memory";
      break;
    case StatusCode::KeyError:
      type = "Key error";
      break;
    case StatusCode::TypeError:
      type = "Type error";
      break;
    case StatusCode::Invalid:
      type = "Invalid";
      break;
    case StatusCode::IOError:
      type = "IOError";
      break;
    case StatusCode::CapacityError:
      type = "Capacity error";
      break;
    case StatusCode::UnknownError:
      type = "Unknown error";
      break;
    case StatusCode::NotImplemented:
      type = "NotImplemented";
      break;
    case StatusCode::SerializationError:
      type = "Serialization error";
      break;
    case StatusCode::PythonError:
      type = "Python error";
      break;
    case StatusCode::PlasmaObjectExists:
      type = "Plasma object exists";
      break;
    case StatusCode::PlasmaObjectNonexistent:
      type = "Plasma object is nonexistent";
      break;
    case StatusCode::PlasmaStoreFull:
      type = "Plasma store is full";
      break;
    case StatusCode::PlasmaObjectAlreadySealed:
      type = "Plasma object is already sealed";
      break;
    case StatusCode::CodeGenError:
      type = "CodeGenError in Gandiva";
      break;
    case StatusCode::ExpressionValidationError:
      type = "ExpressionValidationError";
      break;
    case StatusCode::ExecutionError:
      type = "ExecutionError in Gandiva";
      break;
    default:
      type = "Unknown";
      break;
  }
  return std::string(type);
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == NULL) {
    return result;
  }
  result += ": ";
  result += state_.load(std::memory_order_acquire)->msg;
  return result;
}

}  // namespace arrow
