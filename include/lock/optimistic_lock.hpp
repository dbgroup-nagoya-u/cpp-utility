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

#ifndef CPP_UTILITY_LOCK_OPTIMISTIC_LOCK_HPP
#define CPP_UTILITY_LOCK_OPTIMISTIC_LOCK_HPP

// C++ standard libraries
#include <atomic>
#include <cstdint>

namespace dbgroup::lock
{
class OptimisticLock
{
 public:
  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr OptimisticLock() = default;

  OptimisticLock(const OptimisticLock&) = delete;
  OptimisticLock(OptimisticLock&&) = delete;

  auto operator=(const OptimisticLock&) -> OptimisticLock& = delete;
  auto operator=(OptimisticLock&&) -> OptimisticLock& = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~OptimisticLock() = default;

  /*############################################################################
   * Optimistic read APIs
   *##########################################################################*/

  /**
   * @return The current version value.
   *
   * @note This function does not give up reading a version value and continues
   * with spinlock and back-off.
   */
  [[nodiscard]] auto GetVersion() const  //
      -> uint64_t;

  /**
   * @param expected An expected version value.
   * @retval true if the given version value is the same as the current one.
   * @retval false otherwise.
   */
  [[nodiscard]] auto HasSameVersion(  //
      const uint64_t expected) const  //
      -> bool;

  /*############################################################################
   * Pessimistic lock APIs
   *##########################################################################*/

  /**
   * @brief Get a shared lock.
   *
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  void LockS();

  /**
   * @brief Get a shared lock if a given version is the same as the current one.
   *
   * @param ver An expected version value.
   * @retval true if a shared lock is acquired.
   * @retval false if the current version value is different from the given one.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto TryLockS(  //
      const uint64_t ver)       //
      -> bool;

  /**
   * @brief Release a shared lock.
   *
   * @note If a thread calls this function without acquiring an S lock, it will
   * corrupt an internal lock state.
   */
  void UnlockS();

  /**
   * @brief Get an exclusive lock.
   *
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  void LockX();

  /**
   * @brief Get an X lock if a given version is the same as the current one.
   *
   * @param ver An expected version value.
   * @retval true if the given version value is the same as the current one.
   * @retval false otherwise.
   */
  [[nodiscard]] auto TryLockX(  //
      const uint64_t ver)       //
      -> bool;

  /**
   * @brief Downgrade an X lock to an SIX lock.
   *
   * @retval A version value after downgrades.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  auto DowngradeToSIX()  //
      -> uint64_t;

  /**
   * @brief Release an exclusive lock.
   *
   * @return A version value after an exclusive lock release.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  auto UnlockX()  //
      -> uint64_t;

  /**
   * @brief Get a shared-with-intent-exclusive lock.
   *
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  void LockSIX();

  /**
   * @brief Get an SIX lock if a given version is the same as the current one.
   *
   * @param ver an expected version value.
   * @retval true if the given version value is the same as the current one.
   * @retval false otherwise.
   */
  [[nodiscard]] auto TryLockSIX(const uint64_t ver)  //
      -> bool;

  /**
   * @brief Upgrade an SIX lock to an X lock.
   *
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void UpgradeToX();

  /**
   * @brief Release a shared-with-intent-exclusive lock.
   *
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void UnlockSIX();

 private:
  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{0};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_LOCK_OPTIMISTIC_LOCK_HPP
