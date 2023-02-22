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

#include <atomic>
#include <chrono>
#include <thread>

#include "common.hpp"

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

  /**
   * @brief Get a shared lock.
   *
   * NOTE: this function does not give up acquiring locks and continues spinlock.
   */
  void
  LockS()
  {
    while (true) {
      for (size_t i = 1; true; ++i) {
        auto expected = lock_.load(std::memory_order_relaxed);
        if (!(expected & (~kSLockMask))) {
          const auto desired = expected + kSLock;  // increment read-counter
          const auto cas_success = lock_.compare_exchange_weak(
              expected, desired, std::memory_order_acquire, std::memory_order_relaxed);
          if (cas_success) return;
        }
        if (i >= kRetryNum) break;

        CPP_UTILITY_SPINLOCK_HINT
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Release a shared lock.
   *
   */
  void
  UnlockS()
  {
    auto expected = lock_.load(std::memory_order_relaxed);
    auto desired = expected - kSLock;     // decrement read-counter
    while (!lock_.compare_exchange_weak(  //
        expected, desired, std::memory_order_release, std::memory_order_relaxed)) {
      desired = expected - kSLock;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  /**
   * @brief Get an exclusive lock.
   *
   * NOTE: this function does not give up acquiring locks and continues spinlock.
   */
  void
  LockX()
  {
    while (true) {
      for (size_t i = 1; true; ++i) {
        auto expected = kNoLocks;
        if (!lock_.load(std::memory_order_acquire)) {
          const auto cas_success = lock_.compare_exchange_weak(
              expected, kXLock, std::memory_order_acquire, std::memory_order_relaxed);
          if (cas_success) return;
        }
        if (i >= kRetryNum) break;

        CPP_UTILITY_SPINLOCK_HINT
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Downgrade an X lock to an SIX lock.
   *
   * NOTE: if a thread that does not have an exclusive lock calls this function, it will
   * corrupt an internal lock status.
   */
  void
  DowngradeToSIX()
  {
    lock_.store(kSIXLock, std::memory_order_release);
  }

  /**
   * @brief Release an exclusive lock.
   *
   */
  void
  UnlockX()
  {
    lock_.store(kNoLocks, std::memory_order_release);
  }

  /**
   * @brief Get a shared lock with an intent-exclusive lock.
   *
   * NOTE: this function does not give up acquiring locks and continues spinlock.
   */
  void
  LockSIX()
  {
    while (true) {
      for (size_t i = 1; true; ++i) {
        auto expected = lock_.load(std::memory_order_relaxed);
        if (!(expected & (~kSIXLockMask))) {
          const auto desired = expected | kSIXLock;
          const auto cas_success = lock_.compare_exchange_weak(
              expected, desired, std::memory_order_acquire, std::memory_order_relaxed);
          if (cas_success) return;
        }
        if (i >= kRetryNum) break;

        CPP_UTILITY_SPINLOCK_HINT
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Upgrade an SIX lock to an X lock.
   *
   * NOTE: if a thread that does not have a shared lock with an intent-exclusive lock
   * calls this function, it will corrupt an internal lock status.
   */
  void
  UpgradeToX()
  {
    while (true) {
      for (size_t i = 1; true; ++i) {
        auto expected = kSIXLock;
        if (!(lock_.load(std::memory_order_acquire) & (~kSIXLock))) {
          const auto cas_success = lock_.compare_exchange_weak(
              expected, kXLock, std::memory_order_acquire, std::memory_order_relaxed);
          if (cas_success) return;
        }
        if (i >= kRetryNum) break;

        CPP_UTILITY_SPINLOCK_HINT
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Release a shared lock with an intent-exclusive lock.
   *
   */
  void
  UnlockSIX()
  {
    auto expected = lock_.load(std::memory_order_relaxed);
    auto desired = expected - kSIXLock;
    while (!lock_.compare_exchange_weak(  //
        expected, desired, std::memory_order_release, std::memory_order_relaxed)) {
      desired = expected - kSIXLock;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// a lock status for no locks.
  static constexpr uint64_t kNoLocks = 0b000UL;

  /// a lock status for exclusive locks.
  static constexpr uint64_t kXLock = 0b001UL;

  /// a lock status for shared locks with intent-exclusive locks.
  static constexpr uint64_t kSIXLock = 0b010UL;

  /// a lock status for shared locks.
  static constexpr uint64_t kSLock = 0b100UL;

  /// a bit mask for removing SIX/X-lock flags.
  static constexpr uint64_t kSIXLockMask = ~0b011UL;

  /// a bit mask for removing an X-lock flag.
  static constexpr uint64_t kSLockMask = ~0b001UL;

  /// the maximum number of retries for preventing busy loops.
  static constexpr size_t kRetryNum = 10UL;

  /// a sleep time for preventing busy loops.
  static constexpr auto kShortSleep = std::chrono::microseconds{10};

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an internal lock status.
  std::atomic<uint64_t> lock_{0};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_PESSIMISTIC_LOCK_HPP
