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
#include "dbgroup/lock/pessimistic_lock.hpp"

// C++ standard libraries
#include <atomic>
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

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0UL;

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
 * Public APIs
 *############################################################################*/

auto
PessimisticLock::LockS()  //
    -> SGuard
{
  auto cur = lock_.fetch_add(kSLock, kAcquire);
  if (cur & kXLock) {
    do {
      std::this_thread::yield();
      cur = lock_.load(kAcquire);
    } while (cur & kXLock);
  }
  return SGuard{this, static_cast<uint32_t>(cur)};
}

auto
PessimisticLock::LockSIX()  //
    -> SIXGuard
{
  uint64_t cur;
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        *cur = lock->load(kRelaxed);
        while (true) {
          if (*cur & kXMask) return false;
          if (lock->compare_exchange_weak(*cur, *cur | kSIXLock, kAcquire, kRelaxed)) return true;
          CPP_UTILITY_SPINLOCK_HINT
        }
      },
      &lock_, &cur);
  return SIXGuard{this, static_cast<uint32_t>(cur)};
}

auto
PessimisticLock::LockX()  //
    -> XGuard
{
  uint64_t cur;
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
PessimisticLock::UnlockS() noexcept
{
  lock_.fetch_sub(kSLock, kRelaxed);
}

void
PessimisticLock::UnlockSIX() noexcept
{
  lock_.fetch_xor(kSIXLock, kRelaxed);
}

void
PessimisticLock::UnlockX(  //
    const uint32_t old_ver,
    const uint32_t new_ver) noexcept
{
  lock_.fetch_sub(kXLock + old_ver - new_ver, kRelease);
}

/*############################################################################*
 * Shared lock guards
 *############################################################################*/

auto
PessimisticLock::SGuard::operator=(  //
    SGuard &&rhs) noexcept           //
    -> SGuard &
{
  if (dest_) {
    dest_->UnlockS();
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  return *this;
}

PessimisticLock::SGuard::~SGuard()
{
  if (dest_) {
    dest_->UnlockS();
  }
}

/*############################################################################*
 * Shared-with-intent-exclusive lock guards
 *############################################################################*/

auto
PessimisticLock::SIXGuard::operator=(  //
    SIXGuard &&rhs) noexcept           //
    -> SIXGuard &
{
  if (dest_) {
    dest_->UnlockSIX();
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  return *this;
}

PessimisticLock::SIXGuard::~SIXGuard()
{
  if (dest_) {
    dest_->UnlockSIX();
  }
}

auto
PessimisticLock::SIXGuard::UpgradeToX()  //
    -> XGuard
{
  if (dest_ == nullptr) return XGuard{};

  uint64_t cur;
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
PessimisticLock::XGuard::operator=(  //
    XGuard &&rhs) noexcept           //
    -> XGuard &
{
  if (dest_) {
    dest_->UnlockX(old_ver_, new_ver_);
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  old_ver_ = rhs.old_ver_;
  new_ver_ = rhs.new_ver_;
  return *this;
}

PessimisticLock::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX(old_ver_, new_ver_);
  }
}

auto
PessimisticLock::XGuard::DowngradeToSIX() noexcept  //
    -> SIXGuard
{
  if (dest_ == nullptr) return SIXGuard{};

  dest_->lock_.fetch_sub(kXLock - kSIXLock + old_ver_ - new_ver_, kRelease);
  return SIXGuard{std::exchange(dest_, nullptr), new_ver_};
}

}  // namespace dbgroup::lock
