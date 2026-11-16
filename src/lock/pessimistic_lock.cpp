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

// the corresponding header
#include "dbgroup/lock/pessimistic_lock.hpp"

// C++ standard libraries
#include <atomic>
#include <cassert>
#include <cstdint>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
PessimisticLock::LockS()  //
    -> SGuard
{
  SpinWithYield(
      [](std::atomic_uint64_t* lock) -> bool {
        auto cur = lock->load(kRelaxed);
        return (cur & kXLock) == kNoLocks
               && lock->compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed);
      },
      &lock_);
  return SGuard{this};
}

auto
PessimisticLock::LockSIX()  //
    -> SIXGuard
{
  SpinWithBackoff(
      [](std::atomic_uint64_t* lock) -> bool {
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
  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_uint64_t* lock, uint64_t* cur) -> bool {
        *cur = lock->load(kRelaxed);
        return (*cur & kXMask) == kNoLocks
               && lock->compare_exchange_weak(*cur, *cur | kXLock, kAcquire, kRelaxed);
      },
      &lock_, &cur);

  while (cur & kSMask) {
    CPP_UTILITY_SPINLOCK_HINT
    cur = lock_.load(kAcquire);
  }
  return XGuard{this};
}

}  // namespace dbgroup::lock
