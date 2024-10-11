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
/**
 * @brief A class for representing a simple pessimistic lock.
 *
 */
class PessimisticLock
{
 public:
  /*############################################################################
   * Public types
   *##########################################################################*/

  // forward declarations
  class XGuard;

  /**
   * @brief A class for representing a guard instance for shared locks.
   *
   */
  class SGuard
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr SGuard() = default;

    /**
     * @param dest The address of a target lock.
     */
    constexpr SGuard(  //
        PessimisticLock *dest)
        : dest_{dest}
    {
    }

    SGuard(const SGuard &) = delete;

    SGuard(  //
        SGuard &&) noexcept;

    auto operator=(const SGuard &) -> SGuard & = delete;

    auto operator=(          //
        SGuard &&) noexcept  //
        -> SGuard &;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    ~SGuard();

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    PessimisticLock *dest_{nullptr};
  };

  /**
   * @brief A class for representing a guard instance for exclusive locks.
   *
   */
  class SIXGuard
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr SIXGuard() = default;

    /**
     * @param dest The address of a target lock.
     */
    constexpr SIXGuard(  //
        PessimisticLock *dest)
        : dest_{dest}
    {
    }

    SIXGuard(const SIXGuard &) = delete;

    SIXGuard(SIXGuard &&) noexcept;

    auto operator=(const SIXGuard &) -> SIXGuard & = delete;

    auto operator=(SIXGuard &&) noexcept -> SIXGuard &;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    ~SIXGuard();

    /*##########################################################################
     * Public APIs
     *########################################################################*/

    /**
     * @brief Upgrade this lock to an X lock.
     *
     * @return The lock guard for an X lock.
     * @note After calling the function, this lock guard abandons the lock's
     * ownership.
     */
    [[nodiscard]] auto UpgradeToX()  //
        -> XGuard;

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    PessimisticLock *dest_{nullptr};
  };

  /**
   * @brief A class for representing a guard instance for exclusive locks.
   *
   */
  class XGuard
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr XGuard() = default;

    /**
     * @param dest The address of a target lock.
     */
    constexpr XGuard(  //
        PessimisticLock *dest)
        : dest_{dest}
    {
    }

    XGuard(const XGuard &) = delete;

    XGuard(XGuard &&) noexcept;

    auto operator=(const XGuard &) -> XGuard & = delete;

    auto operator=(XGuard &&) noexcept -> XGuard &;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    ~XGuard();

    /*##########################################################################
     * Public APIs
     *########################################################################*/

    /**
     * @brief Downgrade this lock to an SIX lock.
     *
     * @return The lock guard for an SIX lock.
     * @note After calling the function, this lock guard abandons the lock's
     * ownership.
     */
    [[nodiscard]] auto DowngradeToSIX()  //
        -> SIXGuard;

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    PessimisticLock *dest_{nullptr};
  };

  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr PessimisticLock() = default;

  PessimisticLock(const PessimisticLock &) = delete;
  PessimisticLock(PessimisticLock &&) = delete;

  auto operator=(const PessimisticLock &) -> PessimisticLock & = delete;
  auto operator=(PessimisticLock &&) -> PessimisticLock & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~PessimisticLock() = default;

  /*############################################################################
   * Public APIs
   *##########################################################################*/

  /**
   * @brief Get a shared lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto LockS()  //
      -> SGuard;

  /**
   * @brief Get a shared-with-intent-exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto LockSIX()  //
      -> SIXGuard;

  /**
   * @brief Get an exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto LockX()  //
      -> XGuard;

 private:
  /*############################################################################
   * Internal APIs
   *##########################################################################*/

  /**
   * @brief Release a shared lock.
   *
   * @note If a thread calls this function without acquiring an S lock, it will
   * corrupt an internal lock state.
   */
  void UnlockS();

  /**
   * @brief Release a shared-with-intent-exclusive lock.
   *
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void UnlockSIX();

  /**
   * @brief Release an exclusive lock.
   *
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX();

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{0};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_PESSIMISTIC_LOCK_HPP_
