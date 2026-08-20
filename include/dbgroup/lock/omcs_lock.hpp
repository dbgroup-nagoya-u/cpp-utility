/*
 * Copyright 2026 Database Group, Nagoya University
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

#ifndef CPP_UTILITY_DBGROUP_LOCK_OMCS_LOCK_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_OMCS_LOCK_HPP_

// C++ standard libraries
#include <atomic>
#include <cstdint>
#include <utility>

// local sources
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
/**
 * @brief A class for representing an optimistic MCS queue lock.
 *
 */
class OMCSLock
{
 public:
  /*##########################################################################*
   * Public types
   *##########################################################################*/

  // forward declarations
  class XGuard;

  /*##########################################################################*
   * Public constants
   *##########################################################################*/

  /// @brief The maximum number of queue nodes.
  static constexpr uint64_t kQNodeNum = 1UL << 16UL;

  /// @brief A lock state representing an opportunistic lock.
  static constexpr uint64_t kInitLock = 1UL << 62UL;

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
     * @param qid A queue node ID to be held.
     * @param ver The current version.
     */
    constexpr SGuard(  //
        OMCSLock* dest,
        const uint64_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}
        , qid_{qid}
        , ver_{ver}
    {
    }

    constexpr SGuard(  //
        SGuard&& obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
        , qid_{obj.qid_}
        , ver_{obj.ver_}
    {
    }

    auto operator=(             //
        SGuard&& rhs) noexcept  //
        -> SGuard&;

    // forbid copying
    SGuard(const SGuard&) = delete;
    auto operator=(const SGuard&) -> SGuard& = delete;

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
    OMCSLock* dest_{};

    /// @brief The corresponding queue node for unlocking.
    uint64_t qid_{};

    /// @brief A version when creating this guard.
    uint32_t ver_{};
  };

  /**
   * @brief A class for representing a guard instance for SIX locks.
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
     * @param qid A queue node ID to be held.
     * @param ver The current version.
     */
    constexpr SIXGuard(  //
        OMCSLock* dest,
        const uint64_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}
        , qid_{qid}
        , ver_{ver}
    {
    }


    constexpr SIXGuard(  //
        SIXGuard&& obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
        , qid_{obj.qid_}
        , ver_{obj.ver_}
    {
    }


    auto operator=(               //
        SIXGuard&& rhs) noexcept  //
        -> SIXGuard&;

    // forbid copying
    SIXGuard(const SIXGuard&) = delete;
    auto operator=(const SIXGuard&) -> SIXGuard& = delete;


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
    OMCSLock* dest_{};

    /// @brief The corresponding queue node for unlocking.
    uint64_t qid_{};

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
     * @param qid A queue node ID to be held.
     * @param ver The current version.
     */
    constexpr XGuard(  //
        OMCSLock* dest,
        const uint64_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}
        , qid_{qid}
        , old_ver_{ver}
        , new_ver_{ver + 1U}
    {
    }

    constexpr XGuard(  //
        XGuard&& obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
        , qid_{obj.qid_}
        , old_ver_{obj.old_ver_}
        , new_ver_{obj.new_ver_}
    {
    }

    auto operator=(             //
        XGuard&& rhs) noexcept  //
        -> XGuard&;

    // forbid copying
    XGuard(const XGuard&) = delete;
    auto operator=(const XGuard&) -> XGuard& = delete;

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
     * @note This function does not do anything actually due to a queue lock
     * structure.
     */
    [[nodiscard]] auto
    DowngradeToSIX()  //
        -> SIXGuard
    {
      return SIXGuard{std::exchange(dest_, nullptr), qid_, old_ver_};
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OMCSLock* dest_{};

    /// @brief The corresponding queue node for unlocking.
    uint64_t qid_{};

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
        OMCSLock* dest,
        const uint32_t ver) noexcept
        : dest_{dest}
        , ver_{ver}
    {
    }

    constexpr OptGuard(  //
        OptGuard&& obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
        , qid_{obj.qid_}
        , ver_{obj.ver_}
        , retry_num_{obj.retry_num_}
        , has_lock_{std::exchange(obj.has_lock_, false)}
    {
    }

    auto operator=(               //
        OptGuard&& rhs) noexcept  //
        -> OptGuard&;

    // forbid copying
    OptGuard(const OptGuard&) = delete;
    auto operator=(const OptGuard&) -> OptGuard& = delete;

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
     * @retval true if a target version does not change from an expected one.
     * @retval false otherwise.
     */
    [[nodiscard]] auto VerifyVersion(
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
    OMCSLock* dest_{};

    /// @brief The corresponding queue node for unlocking.
    uint64_t qid_{};

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

  constexpr OMCSLock() noexcept = default;

  OMCSLock(const OMCSLock&) = delete;
  OMCSLock(OMCSLock&&) = delete;

  auto operator=(const OMCSLock&) -> OMCSLock& = delete;
  auto operator=(OMCSLock&&) -> OMCSLock& = delete;

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
  [[nodiscard]] auto GetVersion() noexcept  //
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
   * @param qid A queue node ID that holds the lock.
   * @note If a thread calls this function without acquiring an S lock, it will
   * corrupt an internal lock state.
   */
  void UnlockS(  //
      uint64_t qid);

  /**
   * @brief Release a shared-with-intent-exclusive lock.
   *
   * @param qid A queue node ID that holds the lock.
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void UnlockSIX(  //
      uint64_t qid);

  /**
   * @brief Release an exclusive lock.
   *
   * @param qid A queue node ID that holds the lock.
   * @param ver A desired version after unlocking.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      uint64_t qid,
      uint64_t old_ver,
      uint64_t new_ver);

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{kInitLock};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_OMCSLOCK_HPP_
