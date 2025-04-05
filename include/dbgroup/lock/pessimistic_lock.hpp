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
  /*##########################################################################*
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
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr SGuard() noexcept = default;

    /**
     * @param dest The address of a target lock.
     */
    constexpr explicit SGuard(  //
        PessimisticLock *dest) noexcept
        : dest_{dest}
    {
    }

    SGuard(const SGuard &) = delete;

    constexpr SGuard(  //
        SGuard &&obj) noexcept
        : dest_{obj.dest_}
    {
      obj.dest_ = nullptr;
    }

    auto operator=(const SGuard &) -> SGuard & = delete;

    auto operator=(             //
        SGuard &&rhs) noexcept  //
        -> SGuard &;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~SGuard();

    /*########################################################################*
     * Public APIs
     *########################################################################*/

    /**
     * @retval true if this instance has the lock ownership.
     * @retval false otherwise.
     */
    constexpr explicit
    operator bool() const noexcept
    {
      return dest_;
    }

   private:
    /*########################################################################*
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
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr SIXGuard() noexcept = default;

    /**
     * @param dest The address of a target lock.
     */
    constexpr explicit SIXGuard(  //
        PessimisticLock *dest) noexcept
        : dest_{dest}
    {
    }

    SIXGuard(const SIXGuard &) = delete;

    constexpr SIXGuard(  //
        SIXGuard &&obj) noexcept
        : dest_{obj.dest_}
    {
      obj.dest_ = nullptr;
    }

    auto operator=(const SIXGuard &) -> SIXGuard & = delete;

    auto operator=(               //
        SIXGuard &&rhs) noexcept  //
        -> SIXGuard &;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~SIXGuard();

    /*########################################################################*
     * Public APIs
     *########################################################################*/

    /**
     * @retval true if this instance has the lock ownership.
     * @retval false otherwise.
     */
    constexpr explicit
    operator bool() const noexcept
    {
      return dest_;
    }

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
    /*########################################################################*
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
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr XGuard() noexcept = default;

    /**
     * @param dest The address of a target lock.
     */
    constexpr explicit XGuard(  //
        PessimisticLock *dest) noexcept
        : dest_{dest}
    {
    }

    XGuard(const XGuard &) = delete;

    constexpr XGuard(  //
        XGuard &&obj) noexcept
        : dest_{obj.dest_}
    {
      obj.dest_ = nullptr;
    }

    auto operator=(const XGuard &) -> XGuard & = delete;

    auto operator=(             //
        XGuard &&rhs) noexcept  //
        -> XGuard &;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~XGuard();

    /*########################################################################*
     * Public APIs
     *########################################################################*/

    /**
     * @retval true if this instance has the lock ownership.
     * @retval false otherwise.
     */
    constexpr explicit
    operator bool() const noexcept
    {
      return dest_;
    }

    /**
     * @brief Downgrade this lock to an SIX lock.
     *
     * @return The lock guard for an SIX lock.
     * @note After calling the function, this lock guard abandons the lock's
     * ownership.
     */
    [[nodiscard]] auto DowngradeToSIX() noexcept  //
        -> SIXGuard;

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    PessimisticLock *dest_{nullptr};
  };

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr PessimisticLock() noexcept = default;

  PessimisticLock(const PessimisticLock &) = delete;
  PessimisticLock(PessimisticLock &&) = delete;

  auto operator=(const PessimisticLock &) -> PessimisticLock & = delete;
  auto operator=(PessimisticLock &&) -> PessimisticLock & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~PessimisticLock() = default;

  /*##########################################################################*
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
  /*##########################################################################*
   * Internal APIs
   *##########################################################################*/

  /**
   * @brief Release a shared lock.
   *
   * @note If a thread calls this function without acquiring an S lock, it will
   * corrupt an internal lock state.
   */
  void UnlockS() noexcept;

  /**
   * @brief Release a shared-with-intent-exclusive lock.
   *
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void UnlockSIX() noexcept;

  /**
   * @brief Release an exclusive lock.
   *
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX() noexcept;

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{0};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_PESSIMISTIC_LOCK_HPP_
