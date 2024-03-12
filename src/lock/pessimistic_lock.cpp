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
#include "lock/pessimistic_lock.hpp"

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
constexpr uint64_t kSIXLock = 0b001UL << 62UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 0b010UL << 62UL;

/// @brief A bit mask for extracting an SIX/X-lock state.
constexpr uint64_t kSIXAndXMask = kSIXLock | kXLock;

}  // namespace

namespace dbgroup::lock
{

void
PessimisticLock::LockS()
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

void
PessimisticLock::UnlockS()
{
  lock_.fetch_sub(kSLock, kRelaxed);
}

void
PessimisticLock::LockX()
{
  while (true) {
    for (size_t i = 1; true; ++i) {
      auto expected = lock_.load(kAcquire);
      if (expected == kNoLocks
          && lock_.compare_exchange_weak(expected, kXLock, kAcquire, kRelaxed)) {
        return;
      }
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

void
PessimisticLock::DowngradeToSIX()
{
  lock_.store(kSIXLock, kRelease);
}

void
PessimisticLock::UnlockX()
{
  lock_.store(kNoLocks, kRelease);
}

void
PessimisticLock::LockSIX()
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

void
PessimisticLock::UpgradeToX()
{
  while (true) {
    for (size_t i = 1; true; ++i) {
      auto expected = lock_.load(kAcquire);
      if (expected == kSIXLock
          && lock_.compare_exchange_weak(expected, kXLock, kAcquire, kRelaxed)) {
        return;
      }
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

void
PessimisticLock::UnlockSIX()
{
  lock_.fetch_sub(kSIXLock, kRelaxed);
}

}  // namespace dbgroup::lock
