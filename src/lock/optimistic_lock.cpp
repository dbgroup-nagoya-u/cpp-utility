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
#include <cassert>
#include <cstdint>
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

/// @brief A bit mask for extracting an SIX/X-lock state and the S-lock counter.
constexpr uint64_t kAllLockMask = ~0UL ^ kVersionMask;

/// @brief A bit mask for extracting X and SIX states.
constexpr uint64_t kXMask = kXLock | kSIXLock;

/// @brief A bit mask for extracting an S-lock state.
constexpr uint64_t kSMask = kAllLockMask ^ kXMask;

}  // namespace

/*############################################################################*
 * Optimistic read APIs
 *############################################################################*/

auto
OptimisticLock::GetVersion() noexcept  //
    -> OptGuard
{
  uint64_t cur{};
  SpinWithYield(
      [](std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        *cur = lock->load(kAcquire);
        return (*cur & kXMask) == kNoLocks;
      },
      &lock_, &cur);
  return OptGuard{this, static_cast<uint32_t>(cur)};
}

/*############################################################################*
 * Pessimistic lock APIs
 *############################################################################*/

auto
OptimisticLock::LockS()  //
    -> SGuard
{
  SpinWithYield(
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
        return (*cur & kXMask) == kNoLocks
               && lock->compare_exchange_weak(*cur, *cur | kXLock, kAcquire, kRelaxed);
      },
      &lock_, &cur);

  while (cur & kSMask) {
    CPP_UTILITY_SPINLOCK_HINT
    cur = lock_.load(kRelaxed);
  }
  return XGuard{this, static_cast<uint32_t>(cur)};
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

void
OptimisticLock::UnlockS() noexcept
{
  lock_.fetch_sub(kSLock, kRelaxed);
}

void
OptimisticLock::UnlockSIX() noexcept
{
  lock_.fetch_xor(kSIXLock, kRelaxed);
}

void
OptimisticLock::UnlockX(  //
    const uint64_t ver) noexcept
{
  assert((lock_.load(kRelaxed) & (kSIXLock | kSMask)) == kNoLocks);
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
  dest_ = std::exchange(rhs.dest_, nullptr);
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
  dest_ = std::exchange(rhs.dest_, nullptr);
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

  auto cur = dest_->lock_.fetch_xor(kXMask, kRelaxed);
  assert((cur & kXMask) == kSIXLock);
  while (cur & kSMask) {
    CPP_UTILITY_SPINLOCK_HINT
    cur = dest_->lock_.load(kRelaxed);
  }
  return XGuard{std::exchange(dest_, nullptr), static_cast<uint32_t>(cur)};
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
  dest_ = std::exchange(rhs.dest_, nullptr);
  old_ver_ = rhs.old_ver_;
  new_ver_ = rhs.new_ver_;
  return *this;
}

OptimisticLock::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX(new_ver_);
  }
}

auto
OptimisticLock::XGuard::DowngradeToSIX() noexcept  //
    -> SIXGuard
{
  if (dest_ == nullptr) return SIXGuard{};

  assert((dest_->lock_.load(kRelaxed) & kAllLockMask) == kXLock);
  dest_->lock_.store(kSIXLock | new_ver_, kRelease);
  return SIXGuard{std::exchange(dest_, nullptr)};
}

/*############################################################################*
 * Optimistic lock guards
 *############################################################################*/

auto
OptimisticLock::OptGuard::operator=(  //
    OptGuard &&rhs) noexcept          //
    -> OptGuard &
{
  if (dest_ && has_lock_) {
    dest_->UnlockS();
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  ver_ = rhs.ver_;
  retry_num_ = rhs.retry_num_;
  has_lock_ = std::exchange(rhs.has_lock_, false);
  return *this;
}

OptimisticLock::OptGuard::~OptGuard()
{
  if (dest_ && has_lock_) {
    dest_->UnlockS();
  }
}

auto
OptimisticLock::OptGuard::VerifyVersion(  //
    const uint32_t mask,
    const size_t max_retry) noexcept  //
    -> bool
{
  if (has_lock_) {
    dest_->UnlockS();
    has_lock_ = false;
    return true;
  }

  // verify using the optimistic read procedure
  uint64_t cur;
  const auto expected = ver_;
  SpinWithYield(
      [](std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        std::atomic_thread_fence(kRelease);
        *cur = lock->load(kAcquire);
        return (*cur & kXLock) == kNoLocks;
      },
      &dest_->lock_, &cur);
  ver_ = static_cast<uint32_t>(cur);
  if (((ver_ ^ expected) & mask) == 0) return true;
  if (++retry_num_ < max_retry) return false;  // continue with OCC

  // try to acquire a shared lock
  while ((cur & kSMask) == kNoLocks) {
    has_lock_ = dest_->lock_.compare_exchange_weak(cur, cur | kSLock, kAcquire, kRelaxed);
    if (has_lock_) break;
    CPP_UTILITY_SPINLOCK_HINT
  }
  return false;
}

auto
OptimisticLock::OptGuard::ImmediateVerify(  //
    const uint32_t mask) noexcept           //
    -> bool
{
  if (has_lock_) {
    dest_->UnlockS();
    has_lock_ = false;
    return true;
  }

  // verify using the optimistic read procedure
  std::atomic_thread_fence(kRelease);
  const auto expected = ver_;
  const auto cur = dest_->lock_.load(kAcquire);
  ver_ = static_cast<uint32_t>(cur);
  return ((ver_ ^ expected) & mask) == 0 && (cur & kXLock) == kNoLocks;
}

auto
OptimisticLock::OptGuard::TryLockS(  //
    const uint32_t mask)             //
    -> SGuard
{
  SGuard grd;
  if (has_lock_) {
    grd = SGuard{std::exchange(dest_, nullptr)};
    return grd;
  }

  const auto expected = ver_;
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint32_t *ver, uint32_t expected, uint32_t mask) -> bool {
        std::atomic_thread_fence(kRelease);
        auto cur = lock->load(kAcquire);
        *ver = static_cast<uint32_t>(cur);
        return ((*ver ^ expected) & mask)
               || ((cur & kXLock) == kNoLocks
                   && lock->compare_exchange_weak(cur, cur + kSLock, kRelaxed, kRelaxed));
      },
      &(dest_->lock_), &ver_, ver_, mask);

  grd = ((ver_ ^ expected) & mask) ? SGuard{} : SGuard{std::exchange(dest_, nullptr)};
  return grd;
}

auto
OptimisticLock::OptGuard::TryLockSIX(  //
    const uint32_t mask)               //
    -> SIXGuard
{
  if (has_lock_) {
    dest_->lock_.fetch_sub(kSLock, kRelaxed);
    has_lock_ = false;
  }

  const auto expected = ver_;
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint32_t *ver, uint32_t expected, uint32_t mask) -> bool {
        auto cur = lock->load(kAcquire);
        *ver = static_cast<uint32_t>(cur);
        return ((*ver ^ expected) & mask)
               || ((cur & kXMask) == kNoLocks
                   && lock->compare_exchange_weak(cur, cur | kSIXLock, kRelaxed, kRelaxed));
      },
      &(dest_->lock_), &ver_, ver_, mask);

  return ((ver_ ^ expected) & mask) ? SIXGuard{} : SIXGuard{std::exchange(dest_, nullptr)};
}

auto
OptimisticLock::OptGuard::TryLockX(  //
    const uint32_t mask)             //
    -> XGuard
{
  if (has_lock_) {
    dest_->lock_.fetch_sub(kSLock, kRelaxed);
    has_lock_ = false;
  }

  uint64_t cur{};
  const auto expected = ver_;
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur,  //
         const uint32_t expected, const uint32_t mask) -> bool {
        std::atomic_thread_fence(kRelease);
        *cur = lock->load(kAcquire);
        return ((static_cast<uint32_t>(*cur) ^ expected) & mask)
               || ((*cur & kXMask) == kNoLocks
                   && lock->compare_exchange_weak(*cur, *cur | kXLock, kRelaxed, kRelaxed));
      },
      &(dest_->lock_), &cur, expected, mask);

  XGuard grd;
  ver_ = static_cast<uint32_t>(cur);
  if ((ver_ ^ expected) & mask) {
    grd = {};
  } else {
    while (cur & kSMask) {
      CPP_UTILITY_SPINLOCK_HINT
      cur = dest_->lock_.load(kRelaxed);
    }
    grd = XGuard{std::exchange(dest_, nullptr), ver_};
  }
  return grd;
}

}  // namespace dbgroup::lock
