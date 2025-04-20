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
constexpr uint64_t kSLock = 1UL;

/// @brief A lock state representing a shared-with-intent-exclusive lock.
constexpr uint64_t kSIXLock = 1UL << 62UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 63UL;

/// @brief A bit mask for extracting an SIX/X-lock state.
constexpr uint64_t kXMask = kSIXLock | kXLock;

}  // namespace

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
PessimisticLock::LockS()  //
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
PessimisticLock::LockSIX()  //
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
PessimisticLock::LockX()  //
    -> XGuard
{
  SpinWithBackoff(
      [](std::atomic_uint64_t *lock) -> bool {
        auto cur = lock->load(kRelaxed);
        return cur == kNoLocks  //
               && lock->compare_exchange_weak(cur, kXLock, kAcquire, kRelaxed);
      },
      &lock_);
  return XGuard{this};
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
PessimisticLock::UnlockX() noexcept
{
  lock_.fetch_xor(kXLock, kRelease);
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

  SpinWithBackoff(
      [](std::atomic_uint64_t *lock) -> bool {
        auto cur = lock->load(kRelaxed);
        return cur == kSIXLock && lock->compare_exchange_weak(cur, kXLock, kRelaxed, kRelaxed);
      },
      &(dest_->lock_));

  return XGuard{std::exchange(dest_, nullptr)};
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
    dest_->UnlockX();
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  return *this;
}

PessimisticLock::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX();
  }
}

auto
PessimisticLock::XGuard::DowngradeToSIX() noexcept  //
    -> SIXGuard
{
  if (dest_ == nullptr) return SIXGuard{};

  dest_->lock_.fetch_xor(kXMask, kRelease);
  return SIXGuard{std::exchange(dest_, nullptr)};
}

}  // namespace dbgroup::lock
