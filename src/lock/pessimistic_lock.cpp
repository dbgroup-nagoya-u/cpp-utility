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

// local sources
#include "dbgroup/lock/common.hpp"

namespace
{
/*##############################################################################
 * Local constants
 *############################################################################*/

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000UL;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 0b001UL;

/// @brief A lock state representing a shared-with-intent-exclusive lock.
constexpr uint64_t kSIXLock = 0b001UL << 62UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 0b010UL << 62UL;

/// @brief A bit mask for extracting an SIX/X-lock state.
constexpr uint64_t kXMask = kSIXLock | kXLock;

}  // namespace

namespace dbgroup::lock
{
/*##############################################################################
 * Public APIs
 *############################################################################*/

auto
PessimisticLock::LockS()  //
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
PessimisticLock::LockX()  //
    -> XGuard
{
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock) -> bool {
        auto cur = lock->load(kRelaxed);
        return cur == kNoLocks
               && lock->compare_exchange_weak(cur, cur | kXLock, kAcquire, kRelaxed);
      },
      &lock_);
  return XGuard{this};
}

auto
PessimisticLock::LockSIX()  //
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

/*##############################################################################
 * Internal APIs
 *############################################################################*/

void
PessimisticLock::UnlockS()
{
  lock_.fetch_sub(kSLock, kRelaxed);
}

void
PessimisticLock::UnlockSIX()
{
  lock_.fetch_xor(kSIXLock, kRelaxed);
}

void
PessimisticLock::UnlockX()
{
  lock_.store(kNoLocks, kRelease);
}

/*##############################################################################
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
  dest_ = rhs.dest_;
  rhs.dest_ = nullptr;
  return *this;
}

PessimisticLock::SGuard::~SGuard()
{
  if (dest_) {
    dest_->UnlockS();
  }
}

/*##############################################################################
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
  dest_ = rhs.dest_;
  rhs.dest_ = nullptr;
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
  auto *dest = dest_;
  dest_ = nullptr;  // release the ownership

  SpinWithBackoff(
      [](std::atomic_uint64_t *lock) -> bool {
        auto cur = lock->load(kRelaxed);
        return cur == kSIXLock && lock->compare_exchange_weak(cur, kXLock, kRelaxed, kRelaxed);
      },
      &(dest->lock_));

  return XGuard{dest_};
}

/*##############################################################################
 * Exclusive lock guards
 *############################################################################*/

auto
PessimisticLock::XGuard::operator=(  //
    XGuard &&rhs) noexcept           //
    -> XGuard &
{
  if (dest_) {
    dest_->UnlockX();
  }
  dest_ = rhs.dest_;
  rhs.dest_ = nullptr;
  return *this;
}

PessimisticLock::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX();
  }
}

auto
PessimisticLock::XGuard::DowngradeToSIX()  //
    -> SIXGuard
{
  if (dest_ == nullptr) return SIXGuard{};
  auto *dest = dest_;
  dest_ = nullptr;  // release the ownership

  dest->lock_.store(kSIXLock, kRelease);
  return SIXGuard{dest};
}

}  // namespace dbgroup::lock
