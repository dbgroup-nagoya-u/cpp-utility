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
/*############################################################################*
 * Pessimistic lock APIs
 *############################################################################*/

auto
OptimisticLock::LockS()  //
    -> SGuard
{
  uint64_t cur{};
  SpinWithYield(
      [](std::atomic_ref<uint64_t> lock, uint64_t* cur) -> bool {
        *cur = lock.load(kRelaxed);
        return (*cur & kXLock) == kNoLocks
               && lock.compare_exchange_weak(*cur, *cur + kSLock, kAcquire, kRelaxed);
      },
      LockWord(), &cur);
  return SGuard{this, static_cast<uint32_t>(cur)};
}

auto
OptimisticLock::LockSIX()  //
    -> SIXGuard
{
  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_ref<uint64_t> lock, uint64_t* cur) -> bool {
        *cur = lock.load(kRelaxed);
        return (*cur & kXMask) == kNoLocks
               && lock.compare_exchange_weak(*cur, *cur | kSIXLock, kAcquire, kRelaxed);
      },
      LockWord(), &cur);
  return SIXGuard{this, static_cast<uint32_t>(cur)};
}

auto
OptimisticLock::LockX()  //
    -> XGuard
{
  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_ref<uint64_t> lock, uint64_t* cur) -> bool {
        *cur = lock.load(kRelaxed);
        return (*cur & kXMask) == kNoLocks
               && lock.compare_exchange_weak(*cur, *cur | kXLock, kAcquire, kRelaxed);
      },
      LockWord(), &cur);

  while (cur & kSMask) {
    CPP_UTILITY_SPINLOCK_HINT
    cur = LockWord().load(kAcquire);
  }
  std::atomic_thread_fence(kRelease);  // make the X bit visible before writes
  return XGuard{this, static_cast<uint32_t>(cur)};
}

/*############################################################################*
 * Optimistic lock guards
 *############################################################################*/

void
OptimisticLock::OptGuard::FallBackToSLock() noexcept
{
  uint64_t cur;
  SpinWithBackoff(
      [](std::atomic_ref<uint64_t> lock, uint64_t* cur, bool* has_lock) -> bool {
        *cur = lock.load(kAcquire);
        while ((*cur & kXLock) == kNoLocks) {
          if (*cur & kSMask) return true;  // another thread has fallen back
          *has_lock = lock.compare_exchange_weak(*cur, *cur + kSLock, kAcquire, kAcquire);
          if (*has_lock) return true;
        }
        return false;  // wait for the X lock to be released
      },
      dest_->LockWord(), &cur, &has_lock_);
  ver_ = static_cast<uint32_t>(cur);
  retry_num_ = 0;
}

auto
OptimisticLock::OptGuard::TryLockS(  //
    const uint32_t mask)             //
    -> SGuard
{
  if (has_lock_) {
    has_lock_ = false;
    return SGuard{std::exchange(dest_, nullptr), ver_};
  }

  uint32_t cur_ver;
  SpinWithBackoff(
      [](std::atomic_ref<uint64_t> lock, uint32_t* ver, uint32_t expected, uint32_t mask) -> bool {
        std::atomic_thread_fence(kAcquire);
        auto cur = lock.load(kAcquire);
        *ver = static_cast<uint32_t>(cur);
        return ((*ver ^ expected) & mask)
               || ((cur & kXLock) == kNoLocks
                   && lock.compare_exchange_weak(cur, cur + kSLock, kRelaxed, kRelaxed));
      },
      dest_->LockWord(), &cur_ver, ver_, mask);

  if ((ver_ ^ cur_ver) & mask) {
    ver_ = cur_ver;
    return SGuard{};
  }
  return SGuard{std::exchange(dest_, nullptr), cur_ver};
}

auto
OptimisticLock::OptGuard::TryLockSIX(  //
    const uint32_t mask)               //
    -> SIXGuard
{
  if (has_lock_) {
    dest_->LockWord().fetch_sub(kSLock, kRelease);
    has_lock_ = false;
  }

  uint32_t cur_ver;
  SpinWithBackoff(
      [](std::atomic_ref<uint64_t> lock, uint32_t* ver, uint32_t expected, uint32_t mask) -> bool {
        auto cur = lock.load(kAcquire);
        *ver = static_cast<uint32_t>(cur);
        return ((*ver ^ expected) & mask)
               || ((cur & kXMask) == kNoLocks
                   && lock.compare_exchange_weak(cur, cur | kSIXLock, kRelaxed, kRelaxed));
      },
      dest_->LockWord(), &cur_ver, ver_, mask);

  if ((ver_ ^ cur_ver) & mask) {
    ver_ = cur_ver;
    return SIXGuard{};
  }
  return SIXGuard{std::exchange(dest_, nullptr), cur_ver};
}

auto
OptimisticLock::OptGuard::TryLockX(  //
    const uint32_t mask)             //
    -> XGuard
{
  if (has_lock_) {
    dest_->LockWord().fetch_sub(kSLock, kRelease);
    has_lock_ = false;
  }

  uint64_t cur{};
  SpinWithBackoff(
      [](std::atomic_ref<uint64_t> lock, uint64_t* cur,  //
         const uint32_t expected, const uint32_t mask) -> bool {
        std::atomic_thread_fence(kAcquire);
        *cur = lock.load(kAcquire);
        return ((static_cast<uint32_t>(*cur) ^ expected) & mask)
               || ((*cur & kXMask) == kNoLocks
                   && lock.compare_exchange_weak(*cur, *cur | kXLock, kAcquire, kRelaxed));
      },
      dest_->LockWord(), &cur, ver_, mask);

  const auto cur_ver = static_cast<uint32_t>(cur);
  if ((ver_ ^ cur_ver) & mask) {
    ver_ = cur_ver;
    return XGuard{};
  }
  while (cur & kSMask) {
    CPP_UTILITY_SPINLOCK_HINT
    cur = dest_->LockWord().load(kAcquire);
  }
  std::atomic_thread_fence(kRelease);  // make the X bit visible before writes
  return XGuard{std::exchange(dest_, nullptr), cur_ver};
}

}  // namespace dbgroup::lock
