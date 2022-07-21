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

#ifndef CPP_UTILITY_PESSIMISTIC_LOCK_HPP
#define CPP_UTILITY_PESSIMISTIC_LOCK_HPP

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
   * Public utility functions
   *##################################################################################*/

  void
  LockS()
  {
    auto expected = lock_.load(std::memory_order_relaxed) & kSLockMask;
    auto desired = expected + kSLock;                       // increment read-counter
    while (!lock_.compare_exchange_weak(expected, desired,  //
                                        std::memory_order_acquire, std::memory_order_relaxed)) {
      expected &= kSLockMask;
      desired = expected + kSLock;
      SPINLOCK_HINT
    }
  }

  void
  UnlockS()
  {
    auto expected = lock_.load(std::memory_order_relaxed);
    auto desired = expected - kSLock;  // decrement read-counter
    while (!lock_.compare_exchange_weak(expected, desired, std::memory_order_relaxed)) {
      desired = expected - kSLock;
      SPINLOCK_HINT
    }
  }

  void
  LockX()
  {
    auto expected = kNoLocks;
    while (!lock_.compare_exchange_weak(expected, kXLock,  //
                                        std::memory_order_acquire, std::memory_order_relaxed)) {
      expected = kNoLocks;
      SPINLOCK_HINT
    }
  }

  void
  UnlockX()
  {
    lock_.store(kNoLocks, std::memory_order_release);
  }

  void
  LockSIX()
  {
    auto expected = lock_.load(std::memory_order_relaxed) & kSIXLockMask;
    auto desired = expected | kSIXLock;
    while (!lock_.compare_exchange_weak(expected, desired,  //
                                        std::memory_order_acquire, std::memory_order_relaxed)) {
      expected &= kSIXLockMask;
      desired = expected | kSIXLock;
      SPINLOCK_HINT
    }
  }

  void
  UnlockSIX()
  {
    auto expected = lock_.load(std::memory_order_relaxed);
    auto desired = expected - kSIXLock;
    while (!lock_.compare_exchange_weak(expected, desired, std::memory_order_relaxed)) {
      desired = expected - kSIXLock;
      SPINLOCK_HINT
    }
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr uint64_t kNoLocks = 0b000;

  static constexpr uint64_t kXLock = 0b001;

  static constexpr uint64_t kSIXLock = 0b010;

  static constexpr uint64_t kSLock = 0b100;

  static constexpr uint64_t kSIXLockMask = ~0b011;

  static constexpr uint64_t kSLockMask = ~0b001;

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::atomic<uint64_t> lock_{0};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_PESSIMISTIC_LOCK_HPP
