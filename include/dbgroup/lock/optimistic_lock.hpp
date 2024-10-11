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

#ifndef CPP_UTILITY_DBGROUP_LOCK_OPTIMISTIC_LOCK_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_OPTIMISTIC_LOCK_HPP_

// C++ standard libraries
#include <atomic>
#include <cstdint>

namespace dbgroup::lock
{
/**
 * @brief A class for representing simple optimistic locks.
 *
 */
class OptimisticLock
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
        OptimisticLock *dest)
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

    /*##########################################################################
     * Public APIs
     *########################################################################*/

    /**
     * @retval true if this instance has the lock ownership.
     * @retval false otherwise.
     */
    constexpr explicit
    operator bool() const
    {
      return dest_;
    }

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OptimisticLock *dest_{nullptr};
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
        OptimisticLock *dest)
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
     * @retval true if this instance has the lock ownership.
     * @retval false otherwise.
     */
    constexpr explicit
    operator bool() const
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
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OptimisticLock *dest_{nullptr};
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
        OptimisticLock *dest)
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
     * @retval true if this instance has the lock ownership.
     * @retval false otherwise.
     */
    constexpr explicit
    operator bool() const
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
    [[nodiscard]] auto DowngradeToSIX()  //
        -> SIXGuard;

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OptimisticLock *dest_{nullptr};
  };

  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr OptimisticLock() = default;

  OptimisticLock(const OptimisticLock &) = delete;
  OptimisticLock(OptimisticLock &&) = delete;

  auto operator=(const OptimisticLock &) -> OptimisticLock & = delete;
  auto operator=(OptimisticLock &&) -> OptimisticLock & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~OptimisticLock() = default;

  /*############################################################################
   * Optimistic lock APIs
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
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto LockS()  //
      -> SGuard;

  /**
   * @brief Get a shared lock if a given version is the same as the current one.
   *
   * @param ver An expected version value.
   * @retval A guard instance if the lock is acquired.
   * @retval An empty guard instance otherwise.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto TryLockS(  //
      const uint64_t ver)       //
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
   * @brief Get an SIX lock if a given version is the same as the current one.
   *
   * @param ver an expected version value.
   * @retval A guard instance if the lock is acquired.
   * @retval An empty guard instance otherwise.
   */
  [[nodiscard]] auto TryLockSIX(  //
      const uint64_t ver)         //
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

  /**
   * @brief Get an X lock if a given version is the same as the current one.
   *
   * @param ver An expected version value.
   * @retval A guard instance if the lock is acquired.
   * @retval An empty guard instance otherwise.
   */
  [[nodiscard]] auto TryLockX(  //
      const uint64_t ver)       //
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

#endif  // CPP_UTILITY_DBGROUP_LOCK_OPTIMISTIC_LOCK_HPP_
