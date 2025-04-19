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

/// @brief A bit mask for extracting an S/SIX-lock state.
constexpr uint64_t kSAndSIXMask = kSMask | kSIXLock;

/// @brief A bit mask for extracting an X-lock state and version values.
constexpr uint64_t kXAndVersionMask = kXLock | kVersionMask;

}  // namespace

/*############################################################################*
 * Optimistic read APIs
 *############################################################################*/

auto
OptimisticLock::GetVersion() noexcept  //
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
  while (true) {
    cur = lock_.load(kAcquire);
    if ((cur & kXLock) == kNoLocks) break;
    std::this_thread::yield();
  }

  return CompositeGuard{this, static_cast<uint32_t>(cur)};
}

/*############################################################################*
 * Pessimistic lock APIs
 *############################################################################*/

auto
OptimisticLock::LockS()  //
    -> SGuard
{
  if (lock_.fetch_add(kSLock, kAcquire) & kXLock) {
    do {
      std::this_thread::yield();
    } while (lock_.load(kAcquire) & kXLock);
  }
  return SGuard{this};
}

auto
OptimisticLock::LockSIX()  //
    -> SIXGuard
{
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock) -> bool {
        while (true) {
          auto cur = lock->load(kRelaxed);
          if (cur & kXMask) return false;
          if (lock->compare_exchange_weak(cur, cur | kSIXLock, kAcquire, kRelaxed)) return true;
          CPP_UTILITY_SPINLOCK_HINT
        }
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
  auto cur = lock_.load(kRelaxed);
  while (true) {
    if (lock_.compare_exchange_weak(cur, (cur & kSMask) | ver, kRelease, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }
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

  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        *cur = lock->load(kRelaxed);
        return (*cur & kSMask) == kNoLocks
               && lock->compare_exchange_weak(*cur, *cur ^ kXMask, kAcquire, kRelaxed);
      },
      &(dest_->lock_), &cur);

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

  auto cur = dest_->lock_.load(kRelaxed);
  while (true) {
    const auto state = (cur & kSMask) | kSIXLock | new_ver_;
    if (dest_->lock_.compare_exchange_weak(cur, state, kRelease, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }
  return SIXGuard{std::exchange(dest_, nullptr)};
}

/*############################################################################*
 * Optimistic lock guards
 *############################################################################*/

auto
OptimisticLock::OptGuard::VerifyVersion(  //
    const uint32_t mask) noexcept         //
    -> bool
{
  const auto expected = ver_;
  while (true) {
    std::atomic_thread_fence(kRelease);
    const auto cur = dest_->lock_.load(kAcquire);
    if ((cur & kXLock) == kNoLocks) {
      ver_ = static_cast<uint32_t>(cur);
      break;
    }
    std::this_thread::yield();
  }

  return ((ver_ ^ expected) & mask) == 0;
}

auto
OptimisticLock::OptGuard::TryLockS(  //
    const uint32_t mask)             //
    -> SGuard
{
  const auto expected = ver_;
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint32_t *ver, uint32_t expected, uint32_t mask) -> bool {
        auto cur = lock->load(kAcquire);
        if (cur & kXLock) return false;
        *ver = static_cast<uint32_t>(cur);
        return ((*ver ^ expected) & mask)
               || lock->compare_exchange_weak(cur, cur + kSLock, kRelaxed, kRelaxed);
      },
      &(dest_->lock_), &ver_, ver_, mask);

  return ((ver_ ^ expected) & mask) ? SGuard{} : SGuard{dest_};
}

auto
OptimisticLock::OptGuard::TryLockSIX(  //
    const uint32_t mask)               //
    -> SIXGuard
{
  const auto expected = ver_;
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint32_t *ver, uint32_t expected, uint32_t mask) -> bool {
        auto cur = lock->load(kAcquire);
        if (cur & kXLock) return false;
        *ver = static_cast<uint32_t>(cur);
        return ((*ver ^ expected) & mask)
               || ((cur & kXMask) == kNoLocks
                   && lock->compare_exchange_weak(cur, cur | kSIXLock, kRelaxed, kRelaxed));
      },
      &(dest_->lock_), &ver_, ver_, mask);

  return ((ver_ ^ expected) & mask) ? SIXGuard{} : SIXGuard{dest_};
}

auto
OptimisticLock::OptGuard::TryLockX(  //
    const uint32_t mask)             //
    -> XGuard
{
  const auto expected = ver_;
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint32_t *ver, uint32_t expected, uint32_t mask) -> bool {
        auto cur = lock->load(kAcquire);
        if (cur & kXLock) return false;
        *ver = static_cast<uint32_t>(cur);
        return ((*ver ^ expected) & mask)
               || ((cur & kAllLockMask) == kNoLocks
                   && lock->compare_exchange_weak(cur, cur | kXLock, kRelaxed, kRelaxed));
      },
      &(dest_->lock_), &ver_, ver_, mask);

  return ((ver_ ^ expected) & mask) ? XGuard{} : XGuard{dest_, ver_};
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
OptimisticLock::CompositeGuard::VerifyVersion(  //
    const uint32_t mask) noexcept               //
    -> bool
{
  if (has_lock_) return true;  // the lock has been acquired

  // verify using the optimistic read procedure
  uint64_t cur;
  const auto expected = ver_;
  while (true) {
    std::atomic_thread_fence(kRelease);
    cur = dest_->lock_.load(kAcquire);
    if ((cur & kXLock) == kNoLocks) {
      ver_ = static_cast<uint32_t>(cur);
      break;
    }
    std::this_thread::yield();
  }
  if (((ver_ ^ expected) & mask) == 0) return true;

  // try to acquire a shared lock
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        *cur = lock->load(kAcquire);
        return (*cur & kXLock) == kNoLocks
               && ((*cur & kSMask)
                   || lock->compare_exchange_weak(*cur, *cur + kSLock, kRelaxed, kRelaxed));
      },
      &dest_->lock_, &cur);

  has_lock_ = (cur & kSMask) == kNoLocks;
  return false;
}

}  // namespace dbgroup::lock
