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
constexpr uint64_t kSLock = 1UL << 47UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 63UL;

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
    -> MCSLockSGuard
{
  auto *qnode = tls_node_ ? tls_node_.release() : new MCSLock{};
  qnode->lock_.store(kNull, kRelaxed);
  auto tail_ptr = reinterpret_cast<uint64_t>(qnode) | kSLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    if (cur) {  // there are successors
      if (lock_.compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed)) break;
    } else if (lock_.compare_exchange_weak(cur, tail_ptr, kAcquire, kRelaxed)) {
      goto end;
    }
    CPP_UTILITY_SPINLOCK_HINT
  }

  // wait until predecessor gives up the lock
  tls_node_.reset(qnode);
  tail_ptr = cur & kPtrMask;
  qnode = reinterpret_cast<MCSLock *>(tail_ptr);
  if (cur & kXLock) {
    SpinWithBackoff(
        [](std::atomic_uint64_t *lock, uint64_t &cur, uint64_t tail) -> bool {
          cur = lock->load(kAcquire);
          return (cur & kPtrMask) != tail || (cur & kXLock) == kNoLocks;
        },
        &lock_, cur, tail_ptr);
    if ((cur & kPtrMask) != tail_ptr) {
      qnode = reinterpret_cast<MCSLock *>(tail_ptr);
      while (true) {
        tail_ptr = qnode->lock_.load(kRelaxed) & kPtrMask;
        if (tail_ptr) break;
        CPP_UTILITY_SPINLOCK_HINT
      }
      SpinWithBackoff(
          [](std::atomic_uint64_t *lock) -> bool {
            return (lock->load(kAcquire) & kXLock) == kNoLocks;
          },
          &(reinterpret_cast<MCSLock *>(tail_ptr)->lock_));
    }
  }
end:
  return MCSLockSGuard{this, qnode};
}

auto
MCSLock::LockX()  //
    -> MCSLockXGuard
{
  auto *qnode = tls_node_ ? tls_node_.release() : new MCSLock{};
  const auto new_tail = reinterpret_cast<uint64_t>(qnode) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    qnode->lock_.store(cur & kLockMask, kRelaxed);
    if (lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  auto *tail = reinterpret_cast<MCSLock *>(cur & kPtrMask);
  if (tail != nullptr) {  // wait until predecessor gives up the lock
    tail->lock_.fetch_add(new_tail & kPtrMask, kRelaxed);
    SpinWithBackoff(
        [](std::atomic_uint64_t *lock) -> bool {
          return (lock->load(kAcquire) & kLockMask) == kNoLocks;
        },
        &(qnode->lock_));
  }

  return MCSLockXGuard{this, qnode};
}

/*##############################################################################
 * Internal APIs
 *############################################################################*/

void
MCSLock::UnlockS(  //
    MCSLock *qnode)
{
  const auto this_ptr = reinterpret_cast<uint64_t>(qnode);
  auto next_ptr = qnode->lock_.load(kRelaxed) & kPtrMask;
  if (next_ptr == kNull) {
    auto cur = lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      const auto unlock = cur - kSLock;
      if (unlock & kSMask) {
        if (lock_.compare_exchange_weak(cur, unlock, kRelaxed, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, kNull, kRelaxed, kRelaxed)) {
        tls_node_.reset(qnode);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next_ptr = qnode->lock_.load(kRelaxed) & kPtrMask;
      if (next_ptr) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  auto *next = reinterpret_cast<MCSLock *>(next_ptr);
  if ((next->lock_.fetch_sub(kSLock, kRelease) & kSMask) == kNoLocks) {
    tls_node_.reset(qnode);
  }
}

void
MCSLock::UnlockX(  //
    MCSLock *qnode)
{
  const auto this_ptr = reinterpret_cast<uint64_t>(qnode);
  auto next_ptr = qnode->lock_.load(kRelaxed) & kPtrMask;
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
      next_ptr = qnode->lock_.load(kRelaxed) & kPtrMask;
      if (next_ptr) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  auto *next = reinterpret_cast<MCSLock *>(next_ptr);
  if ((next->lock_.fetch_sub(kXLock, kRelease) & kSMask) == kNoLocks) {
    tls_node_.reset(qnode);
  }
}

/*##############################################################################
 * Public inner classes
 *############################################################################*/

MCSLock::MCSLockSGuard::MCSLockSGuard(  //
    MCSLockSGuard &&obj) noexcept
{
  lock_ = obj.lock_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
}

auto
MCSLock::MCSLockSGuard::operator=(  //
    MCSLockSGuard &&obj) noexcept   //
    -> MCSLockSGuard &
{
  if (qnode_) {
    lock_->UnlockS(qnode_);
  }
  lock_ = obj.lock_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
  return *this;
}

MCSLock::MCSLockSGuard::~MCSLockSGuard()
{
  if (qnode_) {
    lock_->UnlockS(qnode_);
  }
}

MCSLock::MCSLockXGuard::MCSLockXGuard(  //
    MCSLockXGuard &&obj) noexcept
{
  lock_ = obj.lock_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
}

auto
MCSLock::MCSLockXGuard::operator=(  //
    MCSLockXGuard &&obj) noexcept   //
    -> MCSLockXGuard &
{
  if (qnode_) {
    lock_->UnlockX(qnode_);
  }
  lock_ = obj.lock_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
  return *this;
}

MCSLock::MCSLockXGuard::~MCSLockXGuard()
{
  if (qnode_) {
    lock_->UnlockX(qnode_);
  }
}

}  // namespace dbgroup::lock
