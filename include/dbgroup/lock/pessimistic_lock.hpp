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
#include <cstdint>
#include <utility>

// local sources
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
/**
 * @brief A class for representing simple pessimistic locks.
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
        PessimisticLock *dest,
        const uint32_t ver) noexcept
        : dest_{dest}, ver_{ver}
    {
    }

    SGuard(const SGuard &) = delete;

    constexpr SGuard(  //
        SGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}, ver_{obj.ver_}
    {
    }

    auto operator=(const SGuard &) -> SGuard & = delete;

    auto operator=(             //
        SGuard &&rhs) noexcept  //
        -> SGuard &;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    /**
     * @brief Destroy this instance and release a lock if holding.
     *
     */
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

    /**
     * @return The version when this guard was created.
     */
    [[nodiscard]] constexpr auto
    GetVersion() const noexcept  //
        -> uint32_t
    {
      return ver_;
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    PessimisticLock *dest_{};

    /// @brief A version when creating this guard.
    uint32_t ver_{};
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
        PessimisticLock *dest,
        const uint32_t ver) noexcept
        : dest_{dest}, ver_{ver}
    {
    }

    SIXGuard(const SIXGuard &) = delete;

    constexpr SIXGuard(  //
        SIXGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}, ver_{obj.ver_}
    {
    }

    auto operator=(const SIXGuard &) -> SIXGuard & = delete;

    auto operator=(               //
        SIXGuard &&rhs) noexcept  //
        -> SIXGuard &;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    /**
     * @brief Destroy this instance and release a lock if holding.
     *
     */
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
     * @return The version when this guard was created.
     */
    [[nodiscard]] constexpr auto
    GetVersion() const noexcept  //
        -> uint32_t
    {
      return ver_;
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
    PessimisticLock *dest_{};

    /// @brief A version when creating this guard.
    uint32_t ver_{};
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
     * @param ver The current version.
     */
    constexpr XGuard(  //
        PessimisticLock *dest,
        const uint32_t ver) noexcept
        : dest_{dest}, old_ver_{ver}, new_ver_{ver + 1U}
    {
    }

    XGuard(const XGuard &) = delete;

    constexpr XGuard(  //
        XGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}, old_ver_{obj.old_ver_}, new_ver_{obj.new_ver_}
    {
    }

    auto operator=(const XGuard &) -> XGuard & = delete;

    auto operator=(             //
        XGuard &&rhs) noexcept  //
        -> XGuard &;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    /**
     * @brief Destroy this instance and release a lock if holding.
     *
     */
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
     * @return The version when this guard was created.
     */
    [[nodiscard]] constexpr auto
    GetVersion() const noexcept  //
        -> uint32_t
    {
      return old_ver_;
    }

    /**
     * @brief Set a desired version after unlocking.
     *
     * @param ver A desired version after unlocking.
     */
    constexpr void
    SetVersion(  //
        const uint32_t ver) noexcept
    {
      new_ver_ = ver;
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
    PessimisticLock *dest_{};

    /// @brief A version when creating this guard.
    uint32_t old_ver_{};

    /// @brief A version when failing verification.
    uint32_t new_ver_{};
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
   * @param old_ver A version before unlocking.
   * @param new_ver A desired version after unlocking.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      uint32_t old_ver,
      uint32_t new_ver) noexcept;

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{};
};

/*############################################################################*
 * Static assertions
 *############################################################################*/

static_assert(Lockable<PessimisticLock>);
static_assert(PessimisticallyLockable<PessimisticLock>);

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_PESSIMISTIC_LOCK_HPP_
