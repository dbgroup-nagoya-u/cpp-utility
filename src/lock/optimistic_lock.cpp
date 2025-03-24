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
#include "dbgroup/lock/optimistic_lock.hpp"

// C++ standard libraries
#include <atomic>
#include <cstdint>
#include <thread>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/common.hpp"

namespace
{
/*############################################################################*
 * Local constants
 *############################################################################*/

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000UL;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << 32UL;

/// @brief A lock state representing a shared-with-intent-exclusive lock.
constexpr uint64_t kSIXLock = 1UL << 62UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 63UL;

/// @brief A bit mask for extracting version values.
constexpr uint64_t kVersionMask = kSLock - 1UL;

/// @brief A bit mask for extracting an SIX/X-lock state and version values.
constexpr uint64_t kAllLockMask = ~0UL ^ kVersionMask;

/// @brief A bit mask for extracting X and SIX states.
constexpr uint64_t kXMask = kXLock | kSIXLock;

/// @brief A bit mask for extracting an S-lock state.
constexpr uint64_t kSMask = kAllLockMask ^ kXMask;

/// @brief A bit mask for extracting an S/SIX-lock state.
constexpr uint64_t kSAndSIXMask = kSMask | kSIXLock;

/// @brief A bit mask for extracting an X-lock state and version values.
constexpr uint64_t kXAndVersionMask = kXLock | kVersionMask;

}  // namespace

namespace dbgroup::lock
{
/*############################################################################*
 * Optimistic read APIs
 *############################################################################*/

auto
OptimisticLock::GetVersion()  //
    -> OptGuard
{
  uint64_t cur{};
  while (true) {
    cur = lock_.load(kAcquire);
    if ((cur & kXLock) == kNoLocks) break;
    std::this_thread::yield();
  }

  return OptGuard{this, static_cast<uint32_t>(cur)};
}

auto
OptimisticLock::PrepareRead()  //
    -> CompositeGuard
{
  uint64_t cur{};
  for (size_t i = 0; true; ++i) {
    cur = lock_.load(kAcquire);
    if ((cur & kXLock) == kNoLocks) return CompositeGuard{this, static_cast<uint32_t>(cur)};
    if (i >= kRetryNum) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        *cur = lock->load(kAcquire);
        return (*cur & kXLock) == kNoLocks
               && ((*cur & kAllLockMask)
                   || lock->compare_exchange_weak(*cur, *cur + kSLock, kRelaxed, kRelaxed));
      },
      &lock_, &cur);

  return (cur & kAllLockMask) ? CompositeGuard{this, static_cast<uint32_t>(cur)}
                              : CompositeGuard{this};
}

/*############################################################################*
 * Pessimistic lock APIs
 *############################################################################*/

auto
OptimisticLock::LockS()  //
    -> SGuard
{
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock) -> bool {
        auto cur = lock->load(kRelaxed);
        return (cur & kXLock) == kNoLocks
               && lock->compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed);
      },
      &lock_);
  return SGuard{this};
}

auto
OptimisticLock::LockSIX()  //
    -> SIXGuard
{
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock) -> bool {
        auto cur = lock->load(kRelaxed);
        return (cur & kXMask) == kNoLocks
               && lock->compare_exchange_weak(cur, cur | kSIXLock, kAcquire, kRelaxed);
      },
      &lock_);
  return SIXGuard{this};
}

auto
OptimisticLock::LockX()  //
    -> XGuard
{
  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        *cur = lock->load(kRelaxed);
        return (*cur & kAllLockMask) == kNoLocks
               && lock->compare_exchange_weak(*cur, *cur | kXLock, kAcquire, kRelaxed);
      },
      &lock_, &cur);

  return XGuard{this, static_cast<uint32_t>(cur)};
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

void
OptimisticLock::UnlockS()
{
  lock_.fetch_sub(kSLock, kRelaxed);
}

void
OptimisticLock::UnlockSIX()
{
  lock_.fetch_xor(kSIXLock, kRelaxed);
}

void
OptimisticLock::UnlockX(  //
    const uint64_t ver)
{
  lock_.store(ver, kRelease);
}

/*############################################################################*
 * Shared lock guards
 *############################################################################*/

auto
OptimisticLock::SGuard::operator=(  //
    SGuard &&rhs) noexcept          //
    -> SGuard &
{
  if (dest_) {
    dest_->UnlockS();
  }
  dest_ = rhs.dest_;
  rhs.dest_ = nullptr;
  return *this;
}

OptimisticLock::SGuard::~SGuard()
{
  if (dest_) {
    dest_->UnlockS();
  }
}

/*############################################################################*
 * Shared-with-intent-exclusive lock guards
 *############################################################################*/

auto
OptimisticLock::SIXGuard::operator=(  //
    SIXGuard &&rhs) noexcept          //
    -> SIXGuard &
{
  if (dest_) {
    dest_->UnlockSIX();
  }
  dest_ = rhs.dest_;
  rhs.dest_ = nullptr;
  return *this;
}

OptimisticLock::SIXGuard::~SIXGuard()
{
  if (dest_) {
    dest_->UnlockSIX();
  }
}

auto
OptimisticLock::SIXGuard::UpgradeToX()  //
    -> XGuard
{
  if (dest_ == nullptr) return XGuard{};
  auto *dest = dest_;
  dest_ = nullptr;  // release the ownership

  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        *cur = lock->load(kRelaxed);
        return (*cur & kSMask) == kNoLocks
               && lock->compare_exchange_weak(*cur, *cur ^ kXMask, kAcquire, kRelaxed);
      },
      &(dest->lock_), &cur);

  return XGuard{dest, static_cast<uint32_t>(cur)};
}

/*############################################################################*
 * Exclusive lock guards
 *############################################################################*/

auto
OptimisticLock::XGuard::operator=(  //
    XGuard &&rhs) noexcept          //
    -> XGuard &
{
  if (dest_) {
    dest_->UnlockX(new_ver_);
  }
  dest_ = rhs.dest_;
  old_ver_ = rhs.old_ver_;
  new_ver_ = rhs.new_ver_;
  rhs.dest_ = nullptr;
  return *this;
}

OptimisticLock::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX(new_ver_);
  }
}

auto
OptimisticLock::XGuard::DowngradeToSIX()  //
    -> SIXGuard
{
  if (dest_ == nullptr) return SIXGuard{};
  auto *dest = dest_;
  dest_ = nullptr;  // release the ownership

  dest->lock_.store(new_ver_ | kSIXLock, kRelease);
  return SIXGuard{dest};
}

/*############################################################################*
 * Optimistic lock guards
 *############################################################################*/

auto
OptimisticLock::OptGuard::VerifyVersion()  //
    -> bool
{
  auto expected = ver_;
  uint64_t cur{};
  while (true) {
    std::atomic_thread_fence(kRelease);
    cur = dest_->lock_.load(kRelaxed);
    if ((cur & kXLock) == kNoLocks) break;
    std::this_thread::yield();
  }

  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  return ver_ == expected;
}

auto
OptimisticLock::OptGuard::TryLockS()  //
    -> SGuard
{
  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur, uint64_t ver) -> bool {
        *cur = lock->load(kAcquire);
        return (*cur & kXLock) == kNoLocks
               && ((*cur & kVersionMask) != ver
                   || lock->compare_exchange_weak(*cur, *cur + kSLock, kRelaxed, kRelaxed));
      },
      &(dest_->lock_), &cur, ver_);

  auto expected = ver_;
  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  return (ver_ == expected) ? SGuard{dest_} : SGuard{};
}

auto
OptimisticLock::OptGuard::TryLockSIX()  //
    -> SIXGuard
{
  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur, uint64_t ver) -> bool {
        *cur = lock->load(kAcquire);
        return (*cur & kXMask) == kNoLocks
               && ((*cur & kVersionMask) != ver
                   || lock->compare_exchange_weak(*cur, *cur | kSIXLock, kRelaxed, kRelaxed));
      },
      &(dest_->lock_), &cur, ver_);

  auto expected = ver_;
  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  return (ver_ == expected) ? SIXGuard{dest_} : SIXGuard{};
}

auto
OptimisticLock::OptGuard::TryLockX()  //
    -> XGuard
{
  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur, uint64_t ver) -> bool {
        *cur = lock->load(kAcquire);
        return (*cur & kAllLockMask) == kNoLocks
               && ((*cur & kXAndVersionMask) != ver
                   || lock->compare_exchange_weak(*cur, *cur | kXLock, kRelaxed, kRelaxed));
      },
      &(dest_->lock_), &cur, ver_);

  auto expected = ver_;
  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  return (ver_ == expected) ? XGuard{dest_, ver_} : XGuard{};
}

/*############################################################################*
 * Composite (optimistic or shared) lock guards
 *############################################################################*/

auto
OptimisticLock::CompositeGuard::operator=(  //
    CompositeGuard &&rhs) noexcept          //
    -> CompositeGuard &
{
  if (has_lock_) {
    dest_->UnlockS();
  }
  dest_ = rhs.dest_;
  ver_ = rhs.ver_;
  has_lock_ = rhs.has_lock_;
  rhs.has_lock_ = false;
  return *this;
}

OptimisticLock::CompositeGuard::~CompositeGuard()
{
  if (has_lock_) {
    dest_->UnlockS();
  }
}

auto
OptimisticLock::CompositeGuard::VerifyVersion()  //
    -> bool
{
  if (has_lock_) return true;

  auto expected = ver_;
  uint64_t cur{};
  while (true) {
    std::atomic_thread_fence(kRelease);
    cur = dest_->lock_.load(kRelaxed);
    if ((cur & kXLock) == kNoLocks) break;
    std::this_thread::yield();
  }

  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  return ver_ == expected;
}

}  // namespace dbgroup::lock
