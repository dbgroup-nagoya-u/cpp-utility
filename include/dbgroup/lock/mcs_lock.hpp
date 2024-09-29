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
   * Public inner classes
   *##########################################################################*/

  /**
   * @brief A class for representing a guard instance for shared locks.
   *
   */
  class MCSLockSGuard
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr MCSLockSGuard() = default;

    /**
     * @param lock The address of a target lock.
     * @param qnode The corresponding queue node for unlocking.
     */
    constexpr MCSLockSGuard(  //
        MCSLock *lock,
        MCSLock *qnode)
        : lock_{lock}, qnode_{qnode}
    {
    }

    MCSLockSGuard(const MCSLockSGuard &) = delete;

    MCSLockSGuard(  //
        MCSLockSGuard &&) noexcept;

    auto operator=(const MCSLockSGuard &) -> MCSLockSGuard & = delete;

    auto operator=(                 //
        MCSLockSGuard &&) noexcept  //
        -> MCSLockSGuard &;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    ~MCSLockSGuard();

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    MCSLock *lock_{nullptr};

    /// @brief The corresponding queue node for unlocking.
    MCSLock *qnode_{nullptr};
  };

  /**
   * @brief A class for representing a guard instance for exclusive locks.
   *
   */
  class MCSLockXGuard
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr MCSLockXGuard() = default;

    /**
     * @param lock The address of a target lock.
     * @param qnode The corresponding queue node for unlocking.
     */
    constexpr MCSLockXGuard(  //
        MCSLock *lock,
        MCSLock *qnode)
        : lock_{lock}, qnode_{qnode}
    {
    }

    MCSLockXGuard(const MCSLockXGuard &) = delete;

    MCSLockXGuard(MCSLockXGuard &&) noexcept;

    auto operator=(const MCSLockXGuard &) -> MCSLockXGuard & = delete;

    auto operator=(MCSLockXGuard &&) noexcept -> MCSLockXGuard &;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    ~MCSLockXGuard();

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    MCSLock *lock_{nullptr};

    /// @brief The corresponding queue node for unlocking.
    MCSLock *qnode_{nullptr};
  };

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
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto LockS()  //
      -> MCSLockSGuard;

  /**
   * @brief Get an exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto LockX()  //
      -> MCSLockXGuard;

 private:
  /*############################################################################
   * Internal APIs
   *##########################################################################*/

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
   * @brief Release an exclusive lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      MCSLock *qnode);

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
