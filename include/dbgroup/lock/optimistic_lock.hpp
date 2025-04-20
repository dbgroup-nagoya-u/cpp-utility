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
#include <limits>

// local sources
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
/**
 * @brief A class for representing simple optimistic locks.
 *
 */
class OptimisticLock
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
        OptimisticLock *dest) noexcept
        : dest_{dest}
    {
    }

    SGuard(const SGuard &) = delete;

    constexpr SGuard(  //
        SGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
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

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OptimisticLock *dest_{};
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
        OptimisticLock *dest) noexcept
        : dest_{dest}
    {
    }

    SIXGuard(const SIXGuard &) = delete;

    constexpr SIXGuard(  //
        SIXGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
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
    OptimisticLock *dest_{};
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
        OptimisticLock *dest,
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
    OptimisticLock *dest_{};

    /// @brief A version when creating this guard.
    uint32_t old_ver_{};

    /// @brief A version when failing verification.
    uint32_t new_ver_{};
  };

  /**
   * @brief A class for representing a guard instance for optimistic locking.
   *
   */
  class OptGuard
  {
   public:
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr OptGuard() noexcept = default;

    /**
     * @param dest The address of a target lock.
     * @param ver The current version.
     */
    constexpr OptGuard(  //
        OptimisticLock *dest,
        const uint32_t ver) noexcept
        : dest_{dest}, ver_{ver}
    {
    }

    OptGuard(const OptGuard &) = delete;

    constexpr OptGuard(  //
        OptGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)},
          ver_{obj.ver_},
          retry_num_{obj.retry_num_},
          has_lock_{std::exchange(obj.has_lock_, false)}
    {
    }

    auto operator=(const OptGuard &) -> OptGuard & = delete;

    auto operator=(               //
        OptGuard &&rhs) noexcept  //
        -> OptGuard &;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~OptGuard();

    /*########################################################################*
     * Public getters
     *########################################################################*/

    /**
     * @retval true if this instance has a valid version.
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

    /*########################################################################*
     * Public APIs
     *########################################################################*/

    /**
     * @param mask A bitmask for representing bits to be verified.
     * @param max_retry Using a shared lock after retrying this number of times.
     * @retval true if a target version does not change from an expected one.
     * @retval false otherwise.
     * @note If the version verification failed, this function may wait to
     * update internal fields.
     */
    [[nodiscard]] auto VerifyVersion(  //
        uint32_t mask = kNoMask,
        size_t max_retry = std::numeric_limits<size_t>::max()) noexcept  //
        -> bool;

    /**
     * @param mask A bitmask for representing bits to be verified.
     * @retval true if a target version does not change from an expected one.
     * @retval false otherwise.
     */
    [[nodiscard]] auto ImmediateVerify(    //
        uint32_t mask = kNoMask) noexcept  //
        -> bool;

    /**
     * @brief Get a shared lock if a given version is the same as the current one.
     *
     * @param mask A bitmask for representing bits to be verified.
     * @retval A guard instance if the lock is acquired.
     * @retval An empty guard instance otherwise.
     * @note This function does not give up acquiring a lock and continues with
     * spinlock and back-off.
     */
    [[nodiscard]] auto TryLockS(  //
        uint32_t mask = kNoMask)  //
        -> SGuard;

    /**
     * @brief Get an SIX lock if a given version is the same as the current one.
     *
     * @param mask A bitmask for representing bits to be verified.
     * @retval A guard instance if the lock is acquired.
     * @retval An empty guard instance otherwise.
     */
    [[nodiscard]] auto TryLockSIX(  //
        uint32_t mask = kNoMask)    //
        -> SIXGuard;

    /**
     * @brief Get an X lock if a given version is the same as the current one.
     *
     * @param mask A bitmask for representing bits to be verified.
     * @retval A guard instance if the lock is acquired.
     * @retval An empty guard instance otherwise.
     */
    [[nodiscard]] auto TryLockX(  //
        uint32_t mask = kNoMask)  //
        -> XGuard;

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OptimisticLock *dest_{};

    /// @brief A version when creating this guard.
    uint32_t ver_{};

    /// @brief The number of retries for version verification.
    uint16_t retry_num_{};

    /// @brief A flag indicating whether this instance is holding a lock.
    bool has_lock_{};
  };

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr OptimisticLock() noexcept = default;

  OptimisticLock(const OptimisticLock &) = delete;
  OptimisticLock(OptimisticLock &&) = delete;

  auto operator=(const OptimisticLock &) -> OptimisticLock & = delete;
  auto operator=(OptimisticLock &&) -> OptimisticLock & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~OptimisticLock() = default;

  /*##########################################################################*
   * Optimistic lock APIs
   *##########################################################################*/

  /**
   * @return An empty guard instance with the current version value.
   *
   * @note This function does not give up reading a version value and continues
   * with spinlock and back-off.
   */
  [[nodiscard]] auto GetVersion() noexcept  //
      -> OptGuard;

  /*##########################################################################*
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
   * @param ver A desired version after unlocking.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      uint64_t ver) noexcept;

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{};
};

/*############################################################################*
 * Static assertions
 *############################################################################*/

static_assert(Lockable<OptimisticLock>);
static_assert(PessimisticallyLockable<OptimisticLock>);
static_assert(OptimisticallyLockable<OptimisticLock>);

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_OPTIMISTIC_LOCK_HPP_
