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
#include <bitset>
#include <cstdint>
#include <thread>
#include <utility>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
namespace
{
/*############################################################################*
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
constexpr uint64_t kSMask = kLockMask ^ kXLock;

/// @brief A thread local queue node container.
thread_local std::unique_ptr<MCSLock> _tls{};  // NOLINT

}  // namespace

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
MCSLock::LockS()  //
    -> SGuard
{
  auto *qnode = _tls ? _tls.release() : new MCSLock{};
  qnode->lock_.store(kNull, kRelaxed);
  auto ptr = std::bit_cast<uint64_t>(qnode) | kSLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    if (cur) {  // there is the predecessor
      if (lock_.compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed)) break;
    } else if (lock_.compare_exchange_weak(cur, ptr, kAcquire, kRelaxed)) {
      goto end;  // the initial shared lock
    }
    CPP_UTILITY_SPINLOCK_HINT
  }

  _tls.reset(qnode);
  ptr = cur & kPtrMask;
  qnode = std::bit_cast<MCSLock *>(ptr);
  if (cur & kXLock) {  // wait for the predecessor to release the lock
    while ((cur & kPtrMask) == ptr && (cur & kXLock)) {
      std::this_thread::yield();
      cur = lock_.load(kAcquire);
    }
    if ((cur & kPtrMask) != ptr) {
      while (true) {  // wait for the successor to fill its pointer
        ptr = qnode->lock_.load(kAcquire) & kPtrMask;
        if (ptr) break;
        CPP_UTILITY_SPINLOCK_HINT
      }
      auto *next = std::bit_cast<MCSLock *>(ptr);
      while (next->lock_.load(kAcquire) & kXLock) {
        std::this_thread::yield();
      }
    }
  }
end:
  return SGuard{this, qnode};
}

auto
MCSLock::LockSIX()  //
    -> SIXGuard
{
  auto *qnode = _tls ? _tls.release() : new MCSLock{};
  const auto new_tail = std::bit_cast<uint64_t>(qnode) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    qnode->lock_.store(cur & kLockMask, kRelaxed);
    if (lock_.compare_exchange_weak(cur, new_tail, kAcqRel, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  auto *tail = std::bit_cast<MCSLock *>(cur & kPtrMask);
  if (tail != nullptr) {  // wait for the predecessor to release the lock
    tail->lock_.fetch_add(new_tail & kPtrMask, kRelease);
    while (qnode->lock_.load(kAcquire) & kXLock) {
      std::this_thread::yield();
    }
  }
  return SIXGuard{this, qnode};
}

auto
MCSLock::LockX()  //
    -> XGuard
{
  auto *qnode = _tls ? _tls.release() : new MCSLock{};
  const auto new_tail = std::bit_cast<uint64_t>(qnode) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    qnode->lock_.store(cur & kLockMask, kRelaxed);
    if (lock_.compare_exchange_weak(cur, new_tail, kAcqRel, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  auto *tail = std::bit_cast<MCSLock *>(cur & kPtrMask);
  if (tail != nullptr) {  // wait for the predecessor to release the lock
    tail->lock_.fetch_add(new_tail & kPtrMask, kRelease);
    while (qnode->lock_.load(kAcquire) & kLockMask) {
      std::this_thread::yield();
    }
  }
  return XGuard{this, qnode};
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

void
MCSLock::UnlockS(  //
    MCSLock *qnode)
{
  const auto this_ptr = std::bit_cast<uint64_t>(qnode);
  auto next_ptr = qnode->lock_.load(kAcquire) & kPtrMask;
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      const auto unlock = cur - kSLock;
      if (unlock & kLockMask) {
        if (lock_.compare_exchange_weak(cur, unlock, kRelaxed, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, kNull, kRelaxed, kRelaxed)) {
        _tls.reset(qnode);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait for the successor to fill its pointer
      next_ptr = qnode->lock_.load(kAcquire) & kPtrMask;
      if (next_ptr) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  auto *next = std::bit_cast<MCSLock *>(next_ptr);
  if ((next->lock_.fetch_sub(kSLock, kRelease) & kLockMask) == kSLock) {
    _tls.reset(qnode);
  }
}

void
MCSLock::UnlockSIX(  //
    MCSLock *qnode)
{
  // wait for shared lock holders to release their locks
  uint64_t next_ptr{};
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *next_ptr) -> bool {
        *next_ptr = lock->load(kAcquire);
        return (*next_ptr & kSMask) == kNoLocks;
      },
      &(qnode->lock_), &next_ptr);

  const auto this_ptr = std::bit_cast<uint64_t>(qnode);
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      if (cur & kSMask) {
        if (lock_.compare_exchange_weak(cur, cur ^ kXLock, kRelease, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, kNull, kRelease, kRelaxed)) {
        _tls.reset(qnode);
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

  auto *next = std::bit_cast<MCSLock *>(next_ptr);
  if ((next->lock_.fetch_xor(kXLock, kRelease) & kLockMask) == kXLock) {
    _tls.reset(qnode);
  }
}

void
MCSLock::UnlockX(  //
    MCSLock *qnode)
{
  const auto this_ptr = std::bit_cast<uint64_t>(qnode);
  auto next_ptr = qnode->lock_.load(kAcquire);
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      if (cur & kSMask) {
        if (lock_.compare_exchange_weak(cur, cur ^ kXLock, kRelease, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, kNull, kRelease, kRelaxed)) {
        _tls.reset(qnode);
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

  auto *next = std::bit_cast<MCSLock *>(next_ptr);
  if ((next->lock_.fetch_xor(kXLock, kRelease) & kLockMask) == kXLock) {
    _tls.reset(qnode);
  }
}

/*############################################################################*
 * Shared lock guards
 *############################################################################*/

auto
MCSLock::SGuard::operator=(  //
    SGuard &&rhs) noexcept   //
    -> SGuard &
{
  if (dest_) {
    dest_->UnlockS(qnode_);
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  qnode_ = rhs.qnode_;
  return *this;
}

MCSLock::SGuard::~SGuard()
{
  if (dest_) {
    dest_->UnlockS(qnode_);
  }
}

/*############################################################################*
 * Shared-with-intent-exclusive lock guards
 *############################################################################*/

auto
MCSLock::SIXGuard::operator=(  //
    SIXGuard &&rhs) noexcept   //
    -> SIXGuard &
{
  if (dest_) {
    dest_->UnlockSIX(qnode_);
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  qnode_ = rhs.qnode_;
  return *this;
}

MCSLock::SIXGuard::~SIXGuard()
{
  if (dest_) {
    dest_->UnlockSIX(qnode_);
  }
}

auto
MCSLock::SIXGuard::UpgradeToX()  //
    -> XGuard
{
  if (dest_ == nullptr) return XGuard{};

  uint64_t next_ptr;
  while (true) {  // wait for shared lock holders to release their locks
    next_ptr = qnode_->lock_.load(kRelaxed);
    if ((next_ptr & kSMask) == kNoLocks) break;
    std::this_thread::yield();
  }
  return XGuard{std::exchange(dest_, nullptr), qnode_};
}

/*############################################################################*
 * Exclusive lock guards
 *############################################################################*/

auto
MCSLock::XGuard::operator=(  //
    XGuard &&rhs) noexcept   //
    -> XGuard &
{
  if (dest_) {
    dest_->UnlockX(qnode_);
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  qnode_ = rhs.qnode_;
  return *this;
}

MCSLock::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX(qnode_);
  }
}

}  // namespace dbgroup::lock
