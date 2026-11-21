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
#include <cassert>
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
class MCSLock
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
     * @param qnode The corresponding queue node for unlocking.
     */
    constexpr SGuard(  //
        MCSLock* const dest,
        MCSLock* const qnode) noexcept
        : dest_{dest}
        , qnode_{qnode}
    {
    }

    SGuard(const SGuard&) = delete;

    constexpr SGuard(  //
        SGuard&& obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
        , qnode_{obj.qnode_}
    {
    }

    auto operator=(const SGuard&) -> SGuard& = delete;

    auto
    operator=(                  //
        SGuard&& rhs) noexcept  //
        -> SGuard&
    {
      if (dest_) {
        dest_->UnlockS(qnode_);
      }
      dest_ = std::exchange(rhs.dest_, nullptr);
      qnode_ = rhs.qnode_;
      return *this;
    }

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    /**
     * @brief Destroy this instance and release a lock if holding.
     *
     */
    ~SGuard()
    {
      if (dest_) {
        dest_->UnlockS(qnode_);
      }
    }

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
    MCSLock* dest_{};

    /// @brief The corresponding queue node for unlocking.
    MCSLock* qnode_{};
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
     * @param qnode The corresponding queue node for unlocking.
     */
    constexpr SIXGuard(  //
        MCSLock* const dest,
        MCSLock* const qnode) noexcept
        : dest_{dest}
        , qnode_{qnode}
    {
    }

    SIXGuard(const SIXGuard&) = delete;

    constexpr SIXGuard(  //
        SIXGuard&& obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
        , qnode_{obj.qnode_}
    {
    }

    auto operator=(const SIXGuard&) -> SIXGuard& = delete;

    auto
    operator=(                    //
        SIXGuard&& rhs) noexcept  //
        -> SIXGuard&
    {
      if (dest_) {
        dest_->UnlockSIX(qnode_);
      }
      dest_ = std::exchange(rhs.dest_, nullptr);
      qnode_ = rhs.qnode_;
      return *this;
    }

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    /**
     * @brief Destroy this instance and release a lock if holding.
     *
     */
    ~SIXGuard()
    {
      if (dest_) {
        dest_->UnlockSIX(qnode_);
      }
    }

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
    [[nodiscard]]
    auto
    UpgradeToX()  //
        -> XGuard
    {
      if (!dest_) [[unlikely]] {
        return XGuard{};
      }

      while (qnode_->LockWord().load(kAcquire) & kSMask) {
        // wait for shared lock holders to release their locks
        std::this_thread::yield();
      }
      return XGuard{std::exchange(dest_, nullptr), qnode_};
    }

    /**
     * @brief Downgrade this lock to an S lock.
     *
     * @return The lock guard for an S lock.
     * @note After calling the function, this lock guard abandons the lock's
     * ownership.
     */
    [[nodiscard]]
    auto DowngradeToS()  //
        -> SGuard;

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    MCSLock* dest_{};

    /// @brief The corresponding queue node for unlocking.
    MCSLock* qnode_{};
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
     * @param qnode The corresponding queue node for unlocking.
     */
    constexpr XGuard(  //
        MCSLock* const dest,
        MCSLock* const qnode) noexcept
        : dest_{dest}
        , qnode_{qnode}
    {
    }

    XGuard(const XGuard&) = delete;

    constexpr XGuard(  //
        XGuard&& obj) noexcept
        : dest_{std::exchange(obj.dest_, nullptr)}
        , qnode_{obj.qnode_}
    {
    }

    auto operator=(const XGuard&) -> XGuard& = delete;

    auto
    operator=(                  //
        XGuard&& rhs) noexcept  //
        -> XGuard&
    {
      if (dest_) {
        dest_->UnlockX(qnode_);
      }
      dest_ = std::exchange(rhs.dest_, nullptr);
      qnode_ = rhs.qnode_;
      return *this;
    }

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    /**
     * @brief Destroy this instance and release a lock if holding.
     *
     */
    ~XGuard()
    {
      if (dest_) {
        dest_->UnlockX(qnode_);
      }
    }

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
     * @note This function does not do anything actually due to a queue lock
     * structure.
     */
    [[nodiscard]]
    auto
    DowngradeToSIX()  //
        -> SIXGuard
    {
      return SIXGuard{std::exchange(dest_, nullptr), qnode_};
    }

    /**
     * @brief Downgrade this lock to an S lock.
     *
     * @return The lock guard for an S lock.
     * @note After calling the function, this lock guard abandons the lock's
     * ownership.
     */
    [[nodiscard]]
    auto
    DowngradeToS()  //
        -> SGuard
    {
      return DowngradeToSIX().DowngradeToS();
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    MCSLock* dest_{};

    /// @brief The corresponding queue node for unlocking.
    MCSLock* qnode_{};
  };

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr MCSLock() noexcept = default;

  MCSLock(const MCSLock&) = delete;
  MCSLock(MCSLock&&) = delete;

  auto operator=(const MCSLock&) -> MCSLock& = delete;
  auto operator=(MCSLock&&) -> MCSLock& = delete;

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
  [[nodiscard]]
  auto LockS()  //
      -> SGuard;

  /**
   * @brief Get a shared-with-intent-exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]]
  auto LockSIX()  //
      -> SIXGuard;

  /**
   * @brief Get an exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]]
  auto
  LockX()  //
      -> XGuard
  {
    return LockSIX().UpgradeToX();
  }

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
   * @brief Release a shared lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an S lock, it will
   * corrupt an internal lock state.
   */
  void UnlockS(  //
      MCSLock* qnode);

  /**
   * @brief Release a shared-with-intent-exclusive lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void
  UnlockSIX(  //
      MCSLock* const qnode)
  {
    if (qnode->LockWord().fetch_xor(kOwned, kAcqRel) & kSMask) {
      return;  // the predecessor's last shared lock holder will pass the lock
    }
    PassLock(qnode);
  }

  /**
   * @brief Pass a lock to the successor and release a given queue node.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note This function must be called by the last thread that releases the
   * lock, i.e., after both the owner of the queue node and the predecessor's
   * shared lock holders have released the lock.
   */
  void PassLock(  //
      MCSLock* qnode);

  /**
   * @brief Release an exclusive lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void
  UnlockX(  //
      MCSLock* const qnode)
  {
    // X locks have waited for the predecessor's shared lock holders, so no one
    // refers to the owner flag of this node
    assert((qnode->LockWord().load(kRelaxed) & kWaitMask) == kNoLocks);
    PassLock(qnode);
  }

  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  /// @brief The `uintptr_t` of nullptr.
  static constexpr uint64_t kNull = 0;

  /// @brief A lock state representing no locks.
  static constexpr uint64_t kNoLocks = 0b000;

  /// @brief A lock state representing a shared lock.
  static constexpr uint64_t kSLock = 1UL << 47UL;

  /// @brief A flag in queue nodes for indicating that their owners hold locks.
  static constexpr uint64_t kOwned = 1UL << 62UL;

  /// @brief A lock state representing an exclusive lock.
  static constexpr uint64_t kXLock = 1UL << 63UL;

  /// @brief A bit mask for extracting a node pointer.
  static constexpr uint64_t kPtrMask = kSLock - 1UL;

  /// @brief A bit mask for extracting a lock state.
  static constexpr uint64_t kLockMask = ~kPtrMask;

  /// @brief A bit mask for extracting a sharedlock state.
  static constexpr uint64_t kSMask = kLockMask ^ kXLock ^ kOwned;

  /// @brief A bit mask for extracting lock states except for the owner flag.
  static constexpr uint64_t kWaitMask = kSMask | kXLock;

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

static_assert(Lockable<MCSLock>);
static_assert(PessimisticallyLockable<MCSLock>);
static_assert(std::is_trivially_default_constructible_v<MCSLock>);
static_assert(std::is_trivially_destructible_v<MCSLock>);

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_MCS_LOCK_HPP_
