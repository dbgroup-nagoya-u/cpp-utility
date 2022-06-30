/*
 * Copyright 2021 Database Group, Nagoya University
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

#ifndef CPP_UTILITY_LOCK_HPP
#define CPP_UTILITY_LOCK_HPP

#ifndef SPINLOCK_HINT
#ifdef CPP_UTILITY_HAS_SPINLOCK_HINT
#include <xmmintrin.h>
#define SPINLOCK_HINT _mm_pause();  // NOLINT
#else
#define SPINLOCK_HINT /* do nothing */
#endif
#endif

#include <atomic>

namespace dbgroup::lock
{
class PessimisticLock
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr PessimisticLock() = default;

  PessimisticLock(const PessimisticLock&) = delete;
  PessimisticLock(PessimisticLock&&) = delete;

  auto operator=(const PessimisticLock&) -> PessimisticLock& = delete;
  auto operator=(PessimisticLock&&) -> PessimisticLock& = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~PessimisticLock() = default;

  /*####################################################################################
   * Public getters
   *##################################################################################*/

  void
  LockShared()
  {
    auto expected = lock_.load(std::memory_order_acquire) & (~1UL);  // turn-off LSB
    auto desired = expected + 2;   // increment read-counter
    while (!lock_.compare_exchange_strong(expected, desired, std::memory_order_relaxed, std::memory_order_acquire)) {
      expected = expected & (~1UL);
      desired = expected + 2;
      SPINLOCK_HINT
    }
  }

  void
  UnlockShared()
  {
    auto expected = lock_ & (~1);  // turn-off LSB
    auto desired = expected - 2;   // decrement read-counter
    while (!lock_.compare_exchange_strong(expected, desired, std::memory_order_relaxed)) {
      expected = expected & (~1);
      desired = expected - 2;
      SPINLOCK_HINT
    }
  }

  void
  Lock()
  {
    uint64_t expected = 0;
    const auto desired = 1;
    while (!lock_.compare_exchange_strong(expected, desired, std::memory_order_relaxed)) {
      expected = 0;
      SPINLOCK_HINT
    }
  }

  void
  Unlock()
  {
    lock_ = 0;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::atomic<uint64_t> lock_{0};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_LOCK_HPP
