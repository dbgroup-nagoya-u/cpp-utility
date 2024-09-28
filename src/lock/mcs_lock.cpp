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
#include "dbgroup/lock/mcs_lock.hpp"

// C++ standard libraries
#include <atomic>
#include <cstdint>
#include <thread>

// temp
#include <stdexcept>

// local sources
#include "dbgroup/lock/common.hpp"

namespace
{
/*##############################################################################
 * Local constants
 *############################################################################*/

/// @brief The `uintptr_t` of nullptr.
constexpr uint64_t kNull = 0;

/// @brief The begin bit position of shared locks.
constexpr uint64_t kSPos = 47;

/// @brief The bet position of an exclusive lock.
constexpr uint64_t kXPos = 63;

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << kSPos;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << kXPos;

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kPtrMask = kSLock - 1UL;

/// @brief A bit mask for extracting a lock state.
constexpr uint64_t kLockMask = ~kPtrMask;

/// @brief A bit mask for extracting a sharedlock state.
constexpr uint64_t kSMask = kLockMask & ~kXLock;

}  // namespace

namespace dbgroup::lock
{
/*##############################################################################
 * Public APIs
 *############################################################################*/

auto
MCSLock::LockS()  //
    -> MCSLock *
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::UnlockS(  //
    MCSLock *qnode)
{
  throw std::runtime_error{"not implemented yet."};
}

auto
MCSLock::LockX()  //
    -> MCSLock *
{
  auto *qnode = tls_node_ ? tls_node_.release() : new MCSLock{};
  const auto new_tail = reinterpret_cast<uint64_t>(qnode) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    qnode->lock_.store((cur & kSMask) | kXLock, kRelaxed);
    if (lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  auto *tail = reinterpret_cast<MCSLock *>(cur & kPtrMask);
  if (tail != nullptr) {  // wait until predecessor gives up the lock
    tail->lock_.fetch_add(new_tail & kPtrMask, kRelease);
    SpinWithBackoff(
        [](std::atomic_uint64_t *lock) -> bool { return (lock->load(kAcquire) & kLockMask) == 0; },
        &(qnode->lock_));
  }

  return qnode;
}

void
MCSLock::UnlockX(  //
    MCSLock *qnode)
{
  const auto this_ptr = reinterpret_cast<uint64_t>(qnode);
  auto next_ptr = qnode->lock_.load(kAcquire) & kPtrMask;
  if (next_ptr == kNull) {
    auto cur = lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      if (cur & kSMask) {
        if (lock_.compare_exchange_weak(cur, cur & ~kXLock, kRelease, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, kNull, kRelease, kRelaxed)) {
        tls_node_.reset(qnode);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next_ptr = qnode->lock_.load(kAcquire) & kPtrMask;
      if (next_ptr) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  auto *next = reinterpret_cast<MCSLock *>(next_ptr);
  if ((next->lock_.fetch_sub(kXLock, kRelease) & kSMask) == 0) {
    tls_node_.reset(qnode);
  }
}

}  // namespace dbgroup::lock
