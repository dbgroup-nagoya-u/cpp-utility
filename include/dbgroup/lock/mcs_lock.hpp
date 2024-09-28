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

#ifndef CPP_UTILITY_DBGROUP_LOCK_MCS_LOCK_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_MCS_LOCK_HPP_

// C++ standard libraries
#include <atomic>
#include <memory>

namespace dbgroup::lock
{
/**
 * @brief A class for representing the MCS queue lock.
 *
 */
class MCSLock
{
 public:
  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr MCSLock() = default;

  MCSLock(const MCSLock &) = delete;
  MCSLock(MCSLock &&) = delete;

  auto operator=(const MCSLock &) -> MCSLock & = delete;
  auto operator=(MCSLock &&) -> MCSLock & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~MCSLock() = default;

  /*############################################################################
   * Public utility functions
   *##########################################################################*/

  /**
   * @brief Get a shared lock.
   *
   * @return A queue node for releasing this lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  auto LockS()  //
      -> MCSLock *;

  /**
   * @brief Release a shared lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an S lock, it will
   * corrupt an internal lock state.
   */
  void UnlockS(  //
      MCSLock *qnode);

  /**
   * @brief Get an exclusive lock.
   *
   * @return A queue node for releasing this lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  auto LockX()  //
      -> MCSLock *;

  /**
   * @brief Downgrade an X lock to an SIX lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void DowngradeToSIX(  //
      MCSLock *qnode);

  /**
   * @brief Release an exclusive lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      MCSLock *qnode);

  /**
   * @brief Get a shared-with-intent-exclusive lock.
   *
   * @return A queue node for releasing this lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  auto LockSIX()  //
      -> MCSLock *;

  /**
   * @brief Upgrade an SIX lock to an X lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void UpgradeToX(  //
      MCSLock *qnode);

  /**
   * @brief Release a shared-with-intent-exclusive lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void UnlockSIX(  //
      MCSLock *qnode);

 private:
  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{0};

  /// @brief A thread local queue node container.
  static thread_local inline std::unique_ptr<MCSLock> tls_node_{};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_MCS_LOCK_HPP_
