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

#ifndef CPP_UTILITY_DBGROUP_LOCK_OMCSLock_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_OMCSLock_HPP_

// C++ standard libraries
#include <atomic>
#include <cstdint>
#include <utility>

// local sources
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
/**
 * @brief A class for representing the MCS queue lock.
 *
 */
class OMCSLock
{
 public:
  /*##########################################################################*
   * Public constants
   *##########################################################################*/

  /// @brief The maximum number of queue nodes.
  static constexpr uint64_t kQNodeNum = 1UL << 16UL;

  /*##########################################################################*
   * Public inner classes
   *##########################################################################*/

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
     * @param ver The current version.
     */
    constexpr SGuard(  //
        OMCSLock *dest,
        const uint64_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}, qid_{qid}, old_ver_{ver}, new_ver_{ver + 1U}
    {
    }

    SGuard(const SGuard &) = delete;

    constexpr SGuard(  //
        SGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)},
          qid_{obj.qid_},
          old_ver_{obj.old_ver_},
          new_ver_{obj.new_ver_}
    {
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
    OMCSLock *dest_{};

    /// @brief The corresponding queue node for unlocking.
    OMCSLock *qnode_{};
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
     * @param ver The current version.
     */
    constexpr SIXGuard(  //
        OMCSLock *dest,
        const uint64_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}, qid_{qid}, old_ver_{ver}, new_ver_{ver + 1U}
    {
    }

    SIXGuard(const SIXGuard &) = delete;

    constexpr SIXGuard(  //
        SIXGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)},
          qid_{obj.qid_},
          old_ver_{obj.old_ver_},
          new_ver_{obj.new_ver_}
    {
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
    OMCSLock *dest_{};

    /// @brief The corresponding queue node for unlocking.
    OMCSLock *qnode_{};
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
        OMCSLock *dest,
        const uint64_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}, qid_{qid}, old_ver_{ver}, new_ver_{ver + 1U}
    {
    }

    XGuard(const XGuard &) = delete;

    constexpr XGuard(  //
        XGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)},
          qid_{obj.qid_},
          old_ver_{obj.old_ver_},
          new_ver_{obj.new_ver_}
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

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OMCSLock *dest_{};

    /// @brief The corresponding queue node for unlocking.
    uint64_t qid_{};

    /// @brief A version when creating this guard.
    uint32_t old_ver_{};

    /// @brief A version when failing verification.
    uint32_t new_ver_{};
  };

  /**
   * @brief Downgrade this lock to an SIX lock.
   *
   * @return The lock guard for an SIX lock.
   * @note After calling the function, this lock guard abandons the lock's
   * ownership.
   * @note This function does not do anything actually due to a queue lock
   * structure.
   */
  [[nodiscard]] auto
  DowngradeToSIX()  //
      -> SIXGuard
  {
    return SIXGuard{std::exchange(dest_, nullptr), qnode_};
  }

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
        const OMCSLock *dest,
        const uint32_t ver) noexcept
        : dest_{dest}, ver_{ver}
    {
    }

    constexpr OptGuard(const OptGuard &) noexcept = default;
    constexpr OptGuard(OptGuard &&) noexcept = default;

    constexpr auto operator=(const OptGuard &) noexcept -> OptGuard & = default;
    constexpr auto operator=(OptGuard &&) noexcept -> OptGuard & = default;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~OptGuard() = default;

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
     * @retval true if a target version does not change from an expected one.
     * @retval false otherwise.
     */
    [[nodiscard]] auto VerifyVersion(
        size_t max_retry = std::numeric_limits<size_t>::max()) noexcept  //
        -> bool;

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    const OMCSLock *dest_{};

    /// @brief A version when creating this guard.
    uint32_t ver_{};
  };

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr OMCSLock() noexcept = default;

  OMCSLock(const OMCSLock &) = delete;
  OMCSLock(OMCSLock &&) = delete;

  auto operator=(const OMCSLock &) -> OMCSLock & = delete;
  auto operator=(OMCSLock &&) -> OMCSLock & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~OMCSLock() = default;

  /*##########################################################################*
   * Public APIs
   *##########################################################################*/

  /**
   * @return An empty guard instance with the current version value.
   *
   * @note This function does not give up reading a version value and continues
   * with spinlock and back-off.
   */
  [[nodiscard]] auto GetVersion() const noexcept  //
      -> OptGuard;

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
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an S lock, it will
   * corrupt an internal lock state.
   */
  void UnlockS(  //
      uint64_t qid,
      uint64_t ver);

  /**
   * @brief Release a shared-with-intent-exclusive lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void UnlockSIX(  //
      uint64_t qid,
      uint64_t ver);

  /**
   * @brief Release an exclusive lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @param ver A desired version after unlocking.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      uint64_t qid,
      uint64_t ver);

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_OMCSLock_HPP_
