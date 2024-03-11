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
#include "lock/optimistic_lock.hpp"

// C++ standard libraries
#include <atomic>
#include <cstdint>
#include <thread>

// local sources
#include "lock/common.hpp"

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
constexpr uint64_t kSIXLock = 0b001UL << 16UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 0b010UL << 16UL;

/// @brief A bit mask for extracting an S-lock state.
constexpr uint64_t kSMask = ~((~0UL) << 16UL);

/// @brief A bit mask for extracting an S/SIX-lock state.
constexpr uint64_t kSAndSIXMask = kSMask | kSIXLock;

/// @brief A bit mask for extracting an SIX/X-lock state.
constexpr uint64_t kSIXAndXMask = kSIXLock | kXLock;

/// @brief A bit mask for extracting an SIX/X-lock state and version values.
constexpr uint64_t kAllLockMask = kSMask | kSIXLock | kXLock;

/// @brief A bit mask for extracting an X-lock state and version values.
constexpr uint64_t kXAndVersionMask = (~0UL) << 17UL;

/// @brief A bit mask for extracting version values.
constexpr uint64_t kVersionMask = (~0UL) << 18UL;

}  // namespace

namespace dbgroup::lock
{
/*##############################################################################
 * Optimistic read APIs
 *############################################################################*/

auto
OptimisticLock::GetVersion() const  //
    -> uint64_t
{
  while (true) {
    for (size_t i = 1; true; ++i) {
      const auto expected = lock_.load(kAcquire);
      if ((expected & kXLock) == 0) return expected & kVersionMask;
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

auto
OptimisticLock::HasSameVersion(     //
    const uint64_t expected) const  //
    -> bool
{
  std::atomic_thread_fence(kRelease);
  const auto desired = lock_.load(kRelaxed) & kXAndVersionMask;
  return expected == desired;
}

/*##############################################################################
 * Pessimistic lock APIs
 *############################################################################*/

void
OptimisticLock::LockS()
{
  while (true) {
    for (size_t i = 1; true; ++i) {
      auto expected = lock_.load(kRelaxed);
      if ((expected & kXLock) == kNoLocks
          && lock_.compare_exchange_weak(expected, expected + kSLock, kAcquire, kRelaxed)) {
        return;
      }
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

auto
OptimisticLock::TryLockS(  //
    const uint64_t ver)    //
    -> bool
{
  while (true) {
    auto expected = lock_.load(kRelaxed);
    for (size_t i = 1; true; ++i) {
      if ((expected & kXAndVersionMask) != ver) return false;
      if (lock_.compare_exchange_weak(expected, expected + kSLock, kRelaxed, kRelaxed)) return true;
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

void
OptimisticLock::UnlockS()
{
  lock_.fetch_sub(kSLock, kRelaxed);
}

void
OptimisticLock::LockX()
{
  while (true) {
    for (size_t i = 1; true; ++i) {
      auto expected = lock_.load(kRelaxed);
      if ((expected & kAllLockMask) == kNoLocks
          && lock_.compare_exchange_weak(expected, expected | kXLock, kAcquire, kRelaxed)) {
        return;
      }
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

auto
OptimisticLock::TryLockX(  //
    const uint64_t ver)    //
    -> bool
{
  const auto desired = ver | kXLock;
  while (true) {
    for (size_t i = 1; true; ++i) {
      auto expected = lock_.load(kRelaxed);
      if ((expected & kXAndVersionMask) != ver) return false;
      if ((expected & kSAndSIXMask) == kNoLocks
          && lock_.compare_exchange_weak(expected, desired, kAcquire, kRelaxed)) {
        return true;
      }
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

auto
OptimisticLock::DowngradeToSIX()  //
    -> uint64_t
{
  const auto old_ver = lock_.fetch_add(kXLock | kSIXLock, kRelease);
  return (old_ver + kXLock) & kVersionMask;
}

auto
OptimisticLock::UnlockX()  //
    -> uint64_t
{
  const auto old_ver = lock_.fetch_add(kXLock, kRelease);
  return (old_ver + kXLock) & kVersionMask;
}

void
OptimisticLock::LockSIX()
{
  while (true) {
    for (size_t i = 1; true; ++i) {
      auto expected = lock_.load(kRelaxed);
      if ((expected & kSIXAndXMask) == kNoLocks
          && lock_.compare_exchange_weak(expected, expected | kSIXLock, kAcquire, kRelaxed)) {
        return;
      }
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

auto
OptimisticLock::TryLockSIX(  //
    const uint64_t ver)      //
    -> bool
{
  while (true) {
    for (size_t i = 1; true; ++i) {
      auto expected = lock_.load(kRelaxed);
      if ((expected & kXAndVersionMask) != ver) return false;
      if ((expected & kSIXLock) == kNoLocks
          && lock_.compare_exchange_weak(expected, expected | kSIXLock, kRelaxed, kRelaxed)) {
        return true;
      }
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

void
OptimisticLock::UpgradeToX()
{
  auto expected = lock_.load(kRelaxed);
  const auto desired = expected & ~kSMask;
  while (true) {
    for (size_t i = 1; true; ++i) {
      if ((expected & kSMask) == kNoLocks
          && lock_.compare_exchange_weak(expected, desired, kAcquire, kRelaxed)) {
        return;
      }
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
      expected = lock_.load(kRelaxed);
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

void
OptimisticLock::UnlockSIX()
{
  lock_.fetch_sub(kSIXLock, kRelaxed);
}

}  // namespace dbgroup::lock
