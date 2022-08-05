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

#ifndef SPINLOCK_HINT
#ifdef CPP_UTILITY_HAS_SPINLOCK_HINT
#include <xmmintrin.h>
#define SPINLOCK_HINT _mm_pause();  // NOLINT
#else
#define SPINLOCK_HINT /* do nothing */
#endif
#endif

#include <xmmintrin.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <utility>

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
   * @brief Get the Version object
   *
   * @return std::pair<uint64_t, bool>
   */
  [[nodiscard]] auto
  GetVersion() const  //
      -> uint64_t
  {
    while (true) {
      auto expected = lock_.load(std::memory_order_acquire);
      for (size_t i = 0; i < kRetryNum; ++i) {
        if (expected & kXLock == 0) return expected & kXLockMask;

        SPINLOCK_HINT
        expected = lock_.load(std::memory_order_acquire);
      }

      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Check whether the version after reading is equal to the version before reading
   *
   * @return true
   * @return false
   */
  auto
  CheckVersion()  //
      -> bool
  {
    std::pair<uint64_t, bool> current_version_ = GetVersion();
    if (!GetVersion().second) return false;
    if (current_version_.first == version_) return true;
    return false;
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
      auto expected = lock_.load(std::memory_order_relaxed) & kXLockMask;
      auto desired = expected + kXLock;
      for (size_t i = 0; i < kRetryNum; ++i) {
        const auto cas_success = lock_.compare_exchange_weak(
            expected, desired, std::memory_order_acquire, std::memory_order_relaxed);
        if (cas_success) return;

        expected &= kXLockMask;
        desired = expected + kXLock;
        SPINLOCK_HINT
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
    lock_.fetch_add(kXLock + kSIXLock, std::memory_order_release);
  }

  /**
   * @brief Release an exclusive lock.
   *
   */
  void
  UnlockX()
  {
    lock_.fetch_add(kXLock, std::memory_order_release);
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
      auto expected = lock_.load(std::memory_order_relaxed) & kXLockMask;
      auto desired = expected + kSIXLock;
      for (size_t i = 0; i < kRetryNum; ++i) {
        const auto cas_success = lock_.compare_exchange_weak(
            expected, desired, std::memory_order_acquire, std::memory_order_relaxed);
        if (cas_success) return;

        expected &= kXLockMask;
        desired = expected + kSIXLock;
        SPINLOCK_HINT
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
    lock_.fetch_add(kSIXLock, std::memory_order_relaxed);
  }

  /**
   * @brief Release a shared lock with an intent-exclusive lock.
   *
   */
  void
  UnlockSIX()
  {
    lock_.fetch_sub(kSIXLock, std::memory_order_relaxed);
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// a lock status for no locks.
  static constexpr uint64_t kNoLocks = 0b000UL;

  /// a lock status for exclusive locks.
  static constexpr uint64_t kXLock = 0b010UL;

  /// a lock status for shared locks with intent-exclusive locks.
  static constexpr uint64_t kSIXLock = 0b001UL;

  /// a bit mask for removing SIX/X-lock flags.
  static constexpr uint64_t kXLockMask = ~0b011UL;

  /// the maximum number of retries for preventing busy loops/
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
