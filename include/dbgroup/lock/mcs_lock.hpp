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
#include <utility>

// local sources
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
/**
 * @brief A class for representing the MCS queue lock.
 *
 */
class MCSLock
{
 public:
  /*##########################################################################*
   * Public types
   *##########################################################################*/

  // forward declarations
  class XGuard;
  class VerGuard;

  /**
   * @brief A class for representing a guard instance for shared locks.
   *
   */
  class SGuard
  {
   public:
    // friend declarations
    friend VerGuard;

    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr SGuard() noexcept = default;

    /**
     * @param dest The address of a target lock.
     * @param qid The corresponding queue node ID for unlocking.
     */
    constexpr SGuard(  //
        MCSLock *dest,
        uint32_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}, qid_{qid}, ver_{ver}
    {
    }

    SGuard(const SGuard &) = delete;

    constexpr SGuard(  //
        SGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}, qid_{obj.qid_}, ver_{obj.ver_}
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
    MCSLock *dest_{};

    /// @brief The corresponding queue node ID for unlocking.
    uint32_t qid_{};

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
    // friend declarations
    friend VerGuard;

    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr SIXGuard() noexcept = default;

    /**
     * @param dest The address of a target lock.
     * @param qid The corresponding queue node ID for unlocking.
     */
    constexpr SIXGuard(  //
        MCSLock *dest,
        uint32_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}, qid_{qid}, ver_{ver}
    {
    }

    SIXGuard(const SIXGuard &) = delete;

    constexpr SIXGuard(  //
        SIXGuard &&obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}, qid_{obj.qid_}, ver_{obj.ver_}
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
    MCSLock *dest_{};

    /// @brief The corresponding queue node ID for unlocking.
    uint32_t qid_{};

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
    // friend declarations
    friend VerGuard;

    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr XGuard() noexcept = default;

    /**
     * @param dest The address of a target lock.
     * @param qid The corresponding queue node ID for unlocking.
     * @param ver The current version.
     */
    constexpr XGuard(  //
        MCSLock *dest,
        uint32_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}, qid_{qid}, old_ver_{ver}, new_ver_{ver + 1}
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
     * @note This function does not do anything actually due to a queue lock
     * structure.
     */
    [[nodiscard]] auto
    DowngradeToSIX()  //
        -> SIXGuard
    {
      return SIXGuard{std::exchange(dest_, nullptr), qid_, new_ver_};
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    MCSLock *dest_{};

    /// @brief The corresponding queue node ID for unlocking.
    uint32_t qid_{};

    /// @brief A version when creating this guard.
    uint32_t old_ver_{};

    /// @brief A version when failing verification.
    uint32_t new_ver_{};
  };

  /**
   * @brief A class for representing a guard instance for version verification.
   *
   */
  class VerGuard
  {
   public:
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr VerGuard() noexcept = default;

    /**
     * @param dest The address of a target lock.
     * @param ver The current version.
     */
    constexpr VerGuard(  //
        MCSLock *dest,
        const uint32_t ver) noexcept
        : dest_{dest}, ver_{ver}
    {
    }

    /**
     * @param grd A guard instance that holds the current version.
     */
    constexpr explicit VerGuard(  //
        const SGuard &grd) noexcept
        : dest_{grd.dest_}, ver_{grd.ver_}
    {
    }

    /**
     * @param grd A guard instance that holds the current version.
     */
    constexpr explicit VerGuard(  //
        const SIXGuard &grd) noexcept
        : dest_{grd.dest_}, ver_{grd.ver_}
    {
    }

    /**
     * @param grd A guard instance that holds the current version.
     */
    constexpr explicit VerGuard(  //
        const XGuard &grd) noexcept
        : dest_{grd.dest_}, ver_{grd.new_ver_}
    {
    }

    constexpr VerGuard(const VerGuard &) = default;
    constexpr VerGuard(VerGuard &&) noexcept = default;

    constexpr auto operator=(const VerGuard &) noexcept -> VerGuard & = default;
    constexpr auto operator=(VerGuard &&) noexcept -> VerGuard & = default;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~VerGuard() = default;

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
     * @retval true if a target version does not change from an expected one.
     * @retval false otherwise.
     */
    [[nodiscard]] auto ImmediateVerify(    //
        uint32_t mask = kNoMask) noexcept  //
        -> bool;

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    MCSLock *dest_{};

    /// @brief A version when creating this guard.
    uint32_t ver_{};
  };

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr MCSLock() noexcept = default;

  MCSLock(const MCSLock &) = delete;
  MCSLock(MCSLock &&) = delete;

  auto operator=(const MCSLock &) -> MCSLock & = delete;
  auto operator=(MCSLock &&) -> MCSLock & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~MCSLock() = default;

  /*##########################################################################*
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
   * @param qid The queue node ID corresponding to this lock.
   * @note If a thread calls this function without acquiring an S lock, it will
   * corrupt an internal lock state.
   */
  void UnlockS(  //
      uint32_t qid);

  /**
   * @brief Release a shared-with-intent-exclusive lock.
   *
   * @param qid The queue node ID corresponding to this lock.
   * @param ver A desired version after unlocking.
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void UnlockSIX(  //
      uint32_t qid,
      uint64_t ver);

  /**
   * @brief Release an exclusive lock.
   *
   * @param qid The queue node ID corresponding to this lock.
   * @param ver A desired version after unlocking.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      uint32_t qid,
      uint64_t ver);

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{};
};

/*############################################################################*
 * Static assertions
 *############################################################################*/

static_assert(Lockable<MCSLock>);
static_assert(PessimisticallyLockable<MCSLock>);

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_MCS_LOCK_HPP_
