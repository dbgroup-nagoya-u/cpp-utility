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

#ifndef CPP_UTILITY_DBGROUP_LOCK_OPTIQL_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_OPTIQL_HPP_

// C++ standard libraries
#include <atomic>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <utility>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
/**
 * @brief A class for representing the MCS queue lock.
 *
 */
class OptiQL
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
        OptiQL* const dest,
        const uint64_t qid,
        const uint32_t ver) noexcept
        : dest_{dest}
        , qid_{qid}
        , old_ver_{ver}
        , new_ver_{ver + 1U}
    {
    }

    XGuard(const XGuard&) = delete;

    constexpr XGuard(  //
        XGuard&& obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
        , qid_{obj.qid_}
        , old_ver_{obj.old_ver_}
        , new_ver_{obj.new_ver_}
    {
    }

    auto operator=(const XGuard&) -> XGuard& = delete;

    auto operator=(             //
        XGuard&& rhs) noexcept  //
        -> XGuard&;

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
    [[nodiscard]]
    constexpr auto
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
    OptiQL* dest_{};

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
        OptiQL* const dest,
        const uint32_t ver) noexcept
        : dest_{dest}
        , ver_{ver}
    {
    }

    constexpr OptGuard(const OptGuard&) noexcept = default;
    constexpr OptGuard(OptGuard&&) noexcept = default;

    constexpr auto operator=(const OptGuard&) noexcept -> OptGuard& = default;
    constexpr auto operator=(OptGuard&&) noexcept -> OptGuard& = default;

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
    [[nodiscard]]
    constexpr auto
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
    [[nodiscard]]
    auto
    VerifyVersion(                               //
        const uint32_t mask = kNoMask) noexcept  //
        -> bool
    {
      uint64_t cur{};
      while (true) {
        std::atomic_thread_fence(kAcquire);
        cur = dest_->LockWord().load(kRelaxed);
        if ((cur & kXAndOPReadMask) != kXLock) break;  // released or opportunistically readable
        std::this_thread::yield();
      }

      const auto expected = ver_;
      ver_ = static_cast<uint32_t>(cur & kVersionMask);
      return ((ver_ ^ expected) & mask) == 0;
    }

    /**
     * @brief Get an X lock if a given version is the same as the current one.
     *
     * @param mask A bitmask for representing bits to be verified.
     * @retval A guard instance if the lock is acquired.
     * @retval An empty guard instance otherwise.
     * @note This function verifies the version while entering the queue and
     * after acquiring the lock. If the version has been changed after
     * acquiring the lock, this function releases it without modifying the
     * version.
     */
    [[nodiscard]]
    auto TryLockX(  //
        uint32_t mask = kNoMask) -> XGuard;

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OptiQL* dest_{};

    /// @brief A version when creating this guard.
    uint32_t ver_{};
  };

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr OptiQL() noexcept = default;

  OptiQL(const OptiQL&) = delete;
  OptiQL(OptiQL&&) = delete;

  auto operator=(const OptiQL&) -> OptiQL& = delete;
  auto operator=(OptiQL&&) -> OptiQL& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~OptiQL() = default;

  /*##########################################################################*
   * Public APIs
   *##########################################################################*/

  /**
   * @return An empty guard instance with the current version value.
   *
   * @note This function does not give up reading a version value and continues
   * with spinlock and back-off.
   */
  [[nodiscard]]
  auto
  GetVersion() noexcept  //
      -> OptGuard
  {
    uint64_t cur{};
    while (true) {
      cur = LockWord().load(kAcquire);
      if ((cur & kXAndOPReadMask) != kXLock) break;  // released or opportunistically readable
      std::this_thread::yield();
    }

    return OptGuard{this, static_cast<uint32_t>(cur & kVersionMask)};
  }

  /**
   * @brief Get an exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]]
  auto LockX()  //
      -> XGuard;

 private:
  /*##########################################################################*
   * Internal APIs
   *##########################################################################*/

  /**
   * @return An atomic reference to the lock state.
   * @note All the accesses to the lock state must be done via this function.
   */
  [[nodiscard]]
  auto
  LockWord() const noexcept  //
      -> std::atomic_ref<uint64_t>
  {
    return std::atomic_ref{lock_};
  }

  /**
   * @brief Get an X lock if a given version is the same as the current one.
   *
   * @param[in,out] ver An expected version. If acquiring the lock fails, this
   * is updated to the current version.
   * @param mask A bitmask for representing bits to be verified (zero for
   * skipping verification, i.e., this function never fails).
   * @retval A guard instance if the lock is acquired.
   * @retval An empty guard instance otherwise.
   */
  [[nodiscard]]
  auto AcquireX(  //
      uint32_t& ver,
      uint32_t mask)  //
      -> XGuard;

  /**
   * @brief Release an exclusive lock.
   *
   * @param qid The ID of the queue node corresponding to this lock.
   * @param old_ver A version when acquiring this lock.
   * @param new_ver A desired version after unlocking.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      uint64_t qid,
      uint64_t old_ver,
      uint64_t new_ver);

  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  /// @brief A lock state representing no locks.
  static constexpr uint64_t kNoLocks = 0b000;

  /// @brief A lock state representing an exclusive lock.
  static constexpr uint64_t kXLock = 1UL << 63UL;

  /// @brief A lock state representing an opportunistic lock.
  static constexpr uint64_t kOPReadFlag = 1UL << 62UL;

  /// @brief A bit mask for extracting an X-lock state and opportunistic read flag.
  static constexpr uint64_t kXAndOPReadMask = kXLock | kOPReadFlag;

  /// @brief A bit mask for extracting a version value.
  static constexpr uint64_t kVersionMask = ~(~0UL << 32UL);

  /// @brief A bit mask for extracting a node pointer.
  static constexpr uint64_t kQIDMask = (kOPReadFlag - 1UL) ^ kVersionMask;

  /// @brief A bit shift for QNode.
  static constexpr uint64_t kQIDShift = 32UL;

  /// @brief A bit mask for extracting a lock state.
  static constexpr uint64_t kLockMask = ~(kVersionMask | kQIDMask);

  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  static_assert(std::atomic_ref<uint64_t>::is_always_lock_free);

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state (accessed only via `LockWord`).
  alignas(std::atomic_ref<uint64_t>::required_alignment) mutable uint64_t lock_;
};

/*############################################################################*
 * Static assertions
 *############################################################################*/

static_assert(Lockable<OptiQL>);
static_assert(OptimisticallyLockable<OptiQL>);
static_assert(std::is_trivially_default_constructible_v<OptiQL>);
static_assert(std::is_trivially_destructible_v<OptiQL>);

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_OPTIQL_HPP_
