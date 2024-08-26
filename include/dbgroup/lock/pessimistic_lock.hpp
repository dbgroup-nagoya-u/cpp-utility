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

#ifndef CPP_UTILITY_DBGROUP_LOCK_PESSIMISTIC_LOCK_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_PESSIMISTIC_LOCK_HPP_

// C++ standard libraries
#include <atomic>

namespace dbgroup::lock
{
class PessimisticLock
{
 public:
  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr PessimisticLock() = default;

  PessimisticLock(const PessimisticLock&) = delete;
  PessimisticLock(PessimisticLock&&) = delete;

  auto operator=(const PessimisticLock&) -> PessimisticLock& = delete;
  auto operator=(PessimisticLock&&) -> PessimisticLock& = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~PessimisticLock() = default;

  /*############################################################################
   * Public utility functions
   *##########################################################################*/

  /**
   * @brief Get a shared lock.
   *
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  void LockS();

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
   * @brief Downgrade an X lock to an SIX lock.
   *
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void DowngradeToSIX();

  /**
   * @brief Release an exclusive lock.
   *
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX();

  /**
   * @brief Get a shared-with-intent-exclusive lock.
   *
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  void LockSIX();

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

#endif  // CPP_UTILITY_DBGROUP_LOCK_PESSIMISTIC_LOCK_HPP_
