/*
 * Copyright 2024 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// corresponding header
#include "dbgroup/lock/optiql.hpp"

// C++ standard libraries
#include <atomic>
#include <cstdint>
#include <thread>

// local sources
#include "dbgroup/lock/common.hpp"

namespace
{
/*##############################################################################
 * Local constants
 *############################################################################*/

/// @brief The `uintptr_t` of nullptr.
constexpr uint64_t kNull = 0;

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << 16UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 17UL;

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kIDMask = kSLock - 1UL;

/// @brief A bit mask for extracting a version value.
constexpr uint64_t kVersionMask = ~(kXLock | kSLock | kIDMask);

/// @brief A bit mask for extracting an X-lock state and version values.
constexpr uint64_t kXAndVersionMask = kVersionMask | kXLock;

}  // namespace

namespace dbgroup::lock
{
/*##############################################################################
 * Public APIs
 *############################################################################*/

auto
OptiQL::GetVersion() const  //
    -> uint64_t
{
  uint64_t cur;
  SpinWithBackoff(
      [](const std::atomic_uint64_t *lock, uint64_t &cur) -> bool {
        cur = lock->load(kAcquire);
        return (cur & kXLock) == kNoLocks;
      },
      &lock_, cur);
  return cur & kVersionMask;
}

auto
OptiQL::HasSameVersion(        //
    const uint64_t cur) const  //
    -> bool
{
  std::atomic_thread_fence(kRelease);
  return cur == (lock_.load(kRelaxed) & kXAndVersionMask);
}

auto
OptiQL::LockX()  //
    -> OptiQLGuard
{
  auto *qnode = new OptiQL{};

  // not implemented yet

  return OptiQLGuard{this, qnode};
}

/*##############################################################################
 * Internal APIs
 *############################################################################*/

void
OptiQL::UnlockX(  //
    OptiQL *qnode)
{
  // not implemented yet
}

/*##############################################################################
 * Public inner classes
 *############################################################################*/

OptiQL::OptiQLGuard::OptiQLGuard(  //
    OptiQLGuard &&obj) noexcept
{
  lock_ = obj.lock_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
}

auto
OptiQL::OptiQLGuard::operator=(  //
    OptiQLGuard &&obj) noexcept  //
    -> OptiQLGuard &
{
  if (qnode_) {
    lock_->UnlockX(qnode_);
  }
  lock_ = obj.lock_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
  return *this;
}

OptiQL::OptiQLGuard::~OptiQLGuard()
{
  if (qnode_) {
    lock_->UnlockX(qnode_);
  }
}

}  // namespace dbgroup::lock
