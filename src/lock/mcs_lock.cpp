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

/// @brief A lock state representing a shared-with-intent-exclusive lock.
constexpr uint64_t kSIXLock = 1UL << 62UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 63UL;

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kPtrMask = kSLock - 1UL;

/// @brief A bit mask for extracting a lock state.
constexpr uint64_t kLockMask = ~kPtrMask;

/// @brief A bit mask for extracting X and SIX states.
constexpr uint64_t kXMask = kXLock | kSIXLock;

/// @brief A bit mask for extracting a sharedlock state.
constexpr uint64_t kSMask = kLockMask ^ kXMask;

}  // namespace

namespace dbgroup::lock
{
/*##############################################################################
 * Public APIs
 *############################################################################*/

auto
MCSLock::LockS()  //
    -> SGuard
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
  if (cur & kXMask) {
    SpinWithBackoff(
        [](std::atomic_uint64_t *lock, uint64_t &cur, uint64_t tail_ptr) -> bool {
          cur = lock->load(kAcquire);
          return (cur & kPtrMask) != tail_ptr || (cur & kXMask) == kNoLocks;
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
            return (lock->load(kAcquire) & kXMask) == kNoLocks;
          },
          &(reinterpret_cast<MCSLock *>(tail_ptr)->lock_));
    }
  }
end:
  return SGuard{this, qnode};
}

auto
MCSLock::LockSIX()  //
    -> SIXGuard
{
  auto *qnode = tls_node_ ? tls_node_.release() : new MCSLock{};
  const auto new_tail = reinterpret_cast<uint64_t>(qnode) | kSIXLock;

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
          return (lock->load(kAcquire) & kXMask) == kNoLocks;
        },
        &(qnode->lock_));
  }

  return SIXGuard{this, qnode};
}

auto
MCSLock::LockX()  //
    -> XGuard
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

  return XGuard{this, qnode};
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
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      const auto unlock = cur - kSLock;
      if (unlock & (kSMask | kSIXLock)) {
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
MCSLock::UnlockSIX(  //
    MCSLock *qnode)
{
  // wait for sharel lock holders to release their locks
  uint64_t next_ptr{};
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t &next_ptr) -> bool {
        next_ptr = lock->load(kRelaxed);
        return (next_ptr & kSMask) == kNoLocks;
      },
      &(qnode->lock_), next_ptr);

  const auto this_ptr = reinterpret_cast<uint64_t>(qnode);
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      if (cur & kSMask) {
        if (lock_.compare_exchange_weak(cur, cur ^ kSIXLock, kRelease, kRelaxed)) return;
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
  if ((next->lock_.fetch_sub(kSIXLock, kRelease) & kSMask) == kNoLocks) {
    tls_node_.reset(qnode);
  }
}

void
MCSLock::UnlockX(  //
    MCSLock *qnode)
{
  const auto this_ptr = reinterpret_cast<uint64_t>(qnode);
  auto next_ptr = qnode->lock_.load(kRelaxed);
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      if (cur & kSMask) {
        if (lock_.compare_exchange_weak(cur, cur ^ kXLock, kRelease, kRelaxed)) return;
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
 * Shared lock guards
 *############################################################################*/

MCSLock::SGuard::SGuard(  //
    SGuard &&obj) noexcept
{
  dest_ = obj.dest_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
}

auto
MCSLock::SGuard::operator=(  //
    SGuard &&obj) noexcept   //
    -> SGuard &
{
  if (qnode_) {
    dest_->UnlockS(qnode_);
  }
  dest_ = obj.dest_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
  return *this;
}

MCSLock::SGuard::~SGuard()
{
  if (qnode_) {
    dest_->UnlockS(qnode_);
  }
}

/*##############################################################################
 * Shared-with-intent-exclusive lock guards
 *############################################################################*/

MCSLock::SIXGuard::SIXGuard(  //
    SIXGuard &&obj) noexcept
{
  dest_ = obj.dest_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
}

auto
MCSLock::SIXGuard::operator=(  //
    SIXGuard &&obj) noexcept   //
    -> SIXGuard &
{
  if (qnode_) {
    dest_->UnlockSIX(qnode_);
  }
  dest_ = obj.dest_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
  return *this;
}

MCSLock::SIXGuard::~SIXGuard()
{
  if (qnode_) {
    dest_->UnlockSIX(qnode_);
  }
}

auto
MCSLock::SIXGuard::UpgradeToX()  //
    -> XGuard
{
  if (qnode_ == nullptr) return XGuard{};
  auto *qnode = qnode_;
  qnode_ = nullptr;  // release the ownership

  // wait for sharel lock holders to release their locks
  uint64_t next_ptr{};
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t &next_ptr) -> bool {
        next_ptr = lock->load(kRelaxed);
        return (next_ptr & kSMask) == kNoLocks;
      },
      &(qnode->lock_), next_ptr);

  const auto this_ptr = reinterpret_cast<uint64_t>(qnode);
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = dest_->lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      if (dest_->lock_.compare_exchange_weak(cur, cur ^ kXMask, kRelaxed, kRelaxed)) {
        return XGuard{dest_, qnode};
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
  next->lock_.fetch_xor(kXMask, kRelaxed);
  return XGuard{dest_, qnode};
}

/*##############################################################################
 * Exclusive lock guards
 *############################################################################*/

MCSLock::XGuard::XGuard(  //
    XGuard &&obj) noexcept
{
  dest_ = obj.dest_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
}

auto
MCSLock::XGuard::operator=(  //
    XGuard &&obj) noexcept   //
    -> XGuard &
{
  if (qnode_) {
    dest_->UnlockX(qnode_);
  }
  dest_ = obj.dest_;
  qnode_ = obj.qnode_;
  obj.qnode_ = nullptr;
  return *this;
}

MCSLock::XGuard::~XGuard()
{
  if (qnode_) {
    dest_->UnlockX(qnode_);
  }
}

auto
MCSLock::XGuard::DowngradeToSIX()  //
    -> SIXGuard
{
  if (qnode_ == nullptr) return SIXGuard{};
  auto *qnode = qnode_;
  qnode_ = nullptr;  // release the ownership

  const auto this_ptr = reinterpret_cast<uint64_t>(qnode);
  auto next_ptr = qnode->lock_.load(kRelaxed) & kPtrMask;
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = dest_->lock_.load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      if (dest_->lock_.compare_exchange_weak(cur, cur ^ kXMask, kRelease, kRelaxed)) {
        return SIXGuard{dest_, qnode};
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
  next->lock_.fetch_xor(kXMask, kRelease);
  return SIXGuard{dest_, qnode};
}

}  // namespace dbgroup::lock
