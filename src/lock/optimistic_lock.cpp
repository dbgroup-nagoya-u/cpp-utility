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
  uint64_t cur;
  auto f = [](const std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
    *cur = lock->load(kAcquire);
    return (*cur & kXLock) == 0;
  };
  SpinWithBackoff(&f, &lock_, &cur);
  return cur & kVersionMask;
}

auto
OptimisticLock::HasSameVersion(  //
    const uint64_t cur) const    //
    -> bool
{
  std::atomic_thread_fence(kRelease);
  const auto desired = lock_.load(kRelaxed) & kXAndVersionMask;
  return cur == desired;
}

/*##############################################################################
 * Pessimistic lock APIs
 *############################################################################*/

void
OptimisticLock::LockS()
{
  auto f = [](std::atomic_uint64_t *lock) -> bool {
    auto cur = lock->load(kRelaxed);
    return (cur & kXLock) == kNoLocks
           && lock->compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed);
  };
  SpinWithBackoff(&f, &lock_);
}

auto
OptimisticLock::TryLockS(  //
    const uint64_t ver)    //
    -> bool
{
  uint64_t cur;
  auto f = [](std::atomic_uint64_t *lock, uint64_t *cur, uint64_t ver) -> bool {
    *cur = lock->load(kRelaxed);
    return (*cur & kXAndVersionMask) != ver
           || lock->compare_exchange_weak(*cur, *cur + kSLock, kAcquire, kRelaxed);
  };
  SpinWithBackoff(&f, &lock_, &cur, ver);
  return (cur & kXAndVersionMask) == ver;
}

void
OptimisticLock::UnlockS()
{
  lock_.fetch_sub(kSLock, kRelaxed);
}

void
OptimisticLock::LockX()
{
  auto f = [](std::atomic_uint64_t *lock) -> bool {
    auto cur = lock->load(kRelaxed);
    return (cur & kAllLockMask) == kNoLocks
           && lock->compare_exchange_weak(cur, cur | kXLock, kAcquire, kRelaxed);
  };
  SpinWithBackoff(&f, &lock_);
}

auto
OptimisticLock::TryLockX(  //
    const uint64_t ver)    //
    -> bool
{
  uint64_t cur;
  auto f = [](std::atomic_uint64_t *lock, uint64_t *cur, uint64_t ver) -> bool {
    *cur = lock->load(kRelaxed);
    return (*cur & kXAndVersionMask) != ver
           || ((*cur & kSAndSIXMask) == kNoLocks
               && lock->compare_exchange_weak(*cur, *cur | kXLock, kAcquire, kRelaxed));
  };
  SpinWithBackoff(&f, &lock_, &cur, ver);
  return (cur & kXAndVersionMask) == ver;
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
  auto f = [](std::atomic_uint64_t *lock) -> bool {
    auto cur = lock->load(kRelaxed);
    return (cur & kSIXAndXMask) == kNoLocks
           && lock->compare_exchange_weak(cur, cur | kSIXLock, kAcquire, kRelaxed);
  };
  SpinWithBackoff(&f, &lock_);
}

auto
OptimisticLock::TryLockSIX(  //
    const uint64_t ver)      //
    -> bool
{
  uint64_t cur;
  auto f = [](std::atomic_uint64_t *lock, uint64_t *cur, uint64_t ver) -> bool {
    *cur = lock->load(kRelaxed);
    return (*cur & kXAndVersionMask) != ver
           || ((*cur & kSIXLock) == kNoLocks
               && lock->compare_exchange_weak(*cur, *cur | kSIXLock, kAcquire, kRelaxed));
  };
  SpinWithBackoff(&f, &lock_, &cur, ver);
  return (cur & kXAndVersionMask) == ver;
}

void
OptimisticLock::UpgradeToX()
{
  auto f = [](std::atomic_uint64_t *lock) -> bool {
    auto cur = lock->load(kRelaxed);
    return (cur & kSMask) == kNoLocks
           && lock->compare_exchange_weak(cur, (cur & kVersionMask) | kXLock, kAcquire, kRelaxed);
  };
  SpinWithBackoff(&f, &lock_);
}

void
OptimisticLock::UnlockSIX()
{
  lock_.fetch_sub(kSIXLock, kRelaxed);
}

}  // namespace dbgroup::lock
