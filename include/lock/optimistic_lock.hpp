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

#ifndef CPP_UTILITY_OPTIMISTIC_LOCK_HPP
#define CPP_UTILITY_OPTIMISTIC_LOCK_HPP

#include <atomic>
#include <chrono>
#include <thread>

#include "common.hpp"

namespace dbgroup::lock
{
class OptimisticLock
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr OptimisticLock() = default;

  OptimisticLock(const OptimisticLock&) = delete;
  OptimisticLock(OptimisticLock&&) = delete;

  auto operator=(const OptimisticLock&) -> OptimisticLock& = delete;
  auto operator=(OptimisticLock&&) -> OptimisticLock& = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~OptimisticLock() = default;

  /*####################################################################################
   * Public getters
   *##################################################################################*/
  /**
   * @return a current version value.
   */
  [[nodiscard]] auto
  GetVersion() const  //
      -> uint64_t
  {
    while (true) {
      for (size_t i = 1; true; ++i) {
        const auto expected = lock_.load(std::memory_order_acquire);
        if ((expected & kXLock) == 0) return expected & kAllBitsMask;
        if (i >= kRetryNum) break;

        CPP_UTILITY_SPINLOCK_HINT
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @param expected an expected version value.
   * @retval true if the given version value is the same as a current one.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  HasSameVersion(const uint64_t expected) const  //
      -> bool
  {
    std::atomic_thread_fence(std::memory_order_release);
    const auto desired = lock_.load(std::memory_order_relaxed) & kSIXAndSBitsMask;
    return expected == desired;
  }

  /**
   * @brief Get a shared lock.
   *
   * NOTE: this function does not give up acquiring locks and continues spinlock.
   */
  void
  LockS()
  {
    while (true) {
      auto expected = lock_.load(std::memory_order_relaxed) & kXBitMask;
      auto desired = expected + kSLock;  // increment read-counter
      for (size_t i = 1; true; ++i) {
        const auto cas_success = lock_.compare_exchange_weak(
            expected, desired, std::memory_order_acquire, std::memory_order_relaxed);
        if (cas_success) return;
        if (i >= kRetryNum) break;

        expected &= kXBitMask;
        desired = expected + kSLock;
        CPP_UTILITY_SPINLOCK_HINT
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Get a shared lock if version is same.
   *
   * @param ver an expected version value (turned off by kSIXAndSBitsMask).
   * @retval true if the given version value is the same as a current one.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  TryLockS(const uint64_t ver)  //
      -> bool
  {
    while (true) {
      auto expected = ver | (lock_.load(std::memory_order_relaxed) & ~kSIXAndSBitsMask);
      auto desired = expected + kSLock;
      for (size_t i = 1; true; ++i) {
        if (lock_.compare_exchange_weak(expected, desired, std::memory_order_relaxed)) return true;
        if ((expected & kSIXAndSBitsMask) != ver) return false;
        if (i >= kRetryNum) break;

        desired = expected + kSLock;
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
      auto expected = lock_.load(std::memory_order_relaxed) & kAllBitsMask;
      auto desired = expected | kXLock;
      for (size_t i = 1; true; ++i) {
        const auto cas_success = lock_.compare_exchange_weak(
            expected, desired, std::memory_order_acquire, std::memory_order_relaxed);
        if (cas_success) return;
        if (i >= kRetryNum) break;

        expected &= kAllBitsMask;
        desired = expected | kXLock;
        CPP_UTILITY_SPINLOCK_HINT
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Get an exclusive lock if version is same.
   *
   * @param ver an expected version value (turned off by kSIXAndSBitsMask).
   * @retval true if the given version value is the same as a current one.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  TryLockX(const uint64_t ver)  //
      -> bool
  {
    const auto desired = ver | kXLock;
    while (true) {
      for (size_t i = 1; true; ++i) {
        auto expected = ver;
        const auto cas_success = lock_.compare_exchange_weak(
            expected, desired, std::memory_order_acquire, std::memory_order_relaxed);
        if (cas_success) return true;
        if ((expected & kSIXAndSBitsMask) != ver) return false;
        if (i >= kRetryNum) break;

        CPP_UTILITY_SPINLOCK_HINT
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Downgrade an X lock to an SIX lock.
   *
   * @retval version value after downgrade from an X lock to an SIX lock.
   *
   * NOTE: if a thread that does not have an exclusive lock calls this function, it will
   * corrupt an internal lock status.
   */
  auto
  DowngradeToSIX()  //
      -> uint64_t
  {
    const auto old_ver = lock_.fetch_add(kXLock + kSIXLock, std::memory_order_release);
    return (old_ver + kXLock + kSIXLock) & kAllBitsMask;
  }

  /**
   * @brief Release an exclusive lock.
   *
   * @retval version value after an exclusive lock release.
   */
  auto
  UnlockX()  //
      -> uint64_t
  {
    const auto old_ver = lock_.fetch_add(kXLock, std::memory_order_release);
    return (old_ver + kXLock) & kAllBitsMask;
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
      auto expected = lock_.load(std::memory_order_relaxed) & kXAndSIXBitsMask;
      auto desired = expected | kSIXLock;
      for (size_t i = 1; true; ++i) {
        const auto cas_success = lock_.compare_exchange_weak(
            expected, desired, std::memory_order_acquire, std::memory_order_relaxed);
        if (cas_success) return;
        if (i >= kRetryNum) break;

        expected &= kXAndSIXBitsMask;
        desired = expected | kSIXLock;
        CPP_UTILITY_SPINLOCK_HINT
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Get a shared lock with an intent-exclusive lock if version is same.
   *
   * @param ver an expected version value (turned off by kSIXAndSBitsMask).
   * @retval true if the given version value is the same as a current one.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  TryLockSIX(const uint64_t ver)  //
      -> bool
  {
    while (true) {
      auto expected = ver | (lock_.load(std::memory_order_relaxed) & ~kSBitsMask);
      auto desired = expected | kSIXLock;
      for (size_t i = 1; true; ++i) {
        if (lock_.compare_exchange_weak(expected, desired, std::memory_order_relaxed)) return true;
        if ((expected & kSIXAndSBitsMask) != ver) return false;
        if (i >= kRetryNum) break;

        expected &= ~kSIXLock;
        desired = expected | kSIXLock;
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
    const auto expected = lock_.load(std::memory_order_relaxed) & kSBitsMask;
    const auto desired = expected + kSIXLock;
    while (true) {
      for (size_t i = 1; true; ++i) {
        auto tmp_exp = expected;
        const auto cas_success = lock_.compare_exchange_weak(
            tmp_exp, desired, std::memory_order_acquire, std::memory_order_relaxed);
        if (cas_success) return;
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
    lock_.fetch_sub(kSIXLock, std::memory_order_release);
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// a lock status for no locks.
  static constexpr uint64_t kNoLocks = 0b000UL;

  /// a lock status for shared locks.
  static constexpr uint64_t kSLock = 0b001UL;

  /// a lock status for exclusive lock.
  static constexpr uint64_t kXLock = 0b010UL << 16UL;

  /// a lock status for shared locks with intent-exclusive lock.
  static constexpr uint64_t kSIXLock = 0b001UL << 16UL;

  /// a bit mask for removing an S-lock flags.
  static constexpr uint64_t kSBitsMask = (~0UL) << 16UL;

  /// a bit mask for removing an X-lock flag.
  static constexpr uint64_t kXBitMask = ~(0b010UL << 16UL);

  /// a bit mask for removing SIX/X-lock flags.
  static constexpr uint64_t kXAndSIXBitsMask = ~(0b011UL << 16UL);

  /// a bit mask for removing S/SIX-lock flags.
  static constexpr uint64_t kSIXAndSBitsMask = (~0UL) << 17UL;

  /// a bit mask for removing S/SIX/X-lock flags.
  static constexpr uint64_t kAllBitsMask = (~0UL) << 18UL;

  /// the maximum number of retries for preventing busy loops.
  static constexpr size_t kRetryNum = 10UL;

  /// a sleep time for preventing busy loops.
  static constexpr auto kShortSleep = std::chrono::microseconds{10};

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an actual lock status.
  std::atomic<uint64_t> lock_{0};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_OPTIMISTIC_LOCK_HPP
