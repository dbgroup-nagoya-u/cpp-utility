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
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <thread>
#include <type_traits>
#include <utility>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/component/queue_node_holder.hpp"
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
/**
 * @brief An optimistic MCS lock.
 *
 */
class OMCSLock
{
 public:
  /*##########################################################################*
   * Public type declarations
   *##########################################################################*/

  /**
   * @brief A scoped guard for shared locks.
   *
   */
  class SGuard;

  /**
   * @brief A scoped guard for shared-with-intent-exclusive locks.
   *
   */
  class SIXGuard;

  /**
   * @brief A scoped guard for exclusive locks.
   *
   */
  class XGuard;

  /**
   * @brief A guard for optimistic reads with version-based verification.
   *
   */
  class OptGuard;

  /*##########################################################################*
   * Public constants
   *##########################################################################*/

  /// @brief The maximum number of queue nodes.
  static constexpr uint64_t kQNodeNum = 1UL << 16UL;

  /// @brief A lock state representing an exclusive lock.
  static constexpr uint64_t kXLock = 1UL << 63UL;

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
  [[nodiscard]]
  auto
  GetVersion() noexcept  //
      -> OptGuard
  {
    uint64_t cur{};
    while (true) {
      cur = LockWord().load(kAcquire);
      if ((cur & kXLock) == 0) break;
      std::this_thread::yield();
    }

    return OptGuard{this, static_cast<uint32_t>(cur)};
  }

  /**
   * @brief Get a shared lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]]
  auto
  LockS() noexcept  //
      -> SGuard
  {
    uint32_t ver{};
    return AcquireS(ver, kSkipVerify);
  }

  /**
   * @brief Get a shared-with-intent-exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]]
  auto
  LockSIX() noexcept  //
      -> SIXGuard
  {
    uint32_t ver{};
    return AcquireSIX(ver, kSkipVerify);
  }

  /**
   * @brief Get an exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]]
  auto
  LockX() noexcept  //
      -> XGuard
  {
    return LockSIX().UpgradeToX();
  }

  /*##########################################################################*
   * Public type definitions
   *##########################################################################*/

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
        OMCSLock* const dest,
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

    auto
    operator=(                  //
        SGuard&& rhs) noexcept  //
        -> SGuard&
    {
      if (dest_) {
        dest_->UnlockS(qid_);
      }
      dest_ = std::exchange(rhs.dest_, nullptr);
      qid_ = rhs.qid_;
      ver_ = rhs.ver_;
      return *this;
    }

    // forbid copying
    SGuard(const SGuard&) = delete;
    auto operator=(const SGuard&) -> SGuard& = delete;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~SGuard()
    {
      if (dest_) {
        dest_->UnlockS(qid_);
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
     * @return The version when this guard was created.
     */
    [[nodiscard]]
    constexpr auto
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
    OMCSLock* dest_{};

    /// @brief The corresponding queue node for unlocking.
    uint64_t qid_{};

    /// @brief A version when creating this guard.
    uint32_t ver_{};
  };

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
        OMCSLock* const dest,
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

    auto
    operator=(                    //
        SIXGuard&& rhs) noexcept  //
        -> SIXGuard&
    {
      if (dest_) {
        dest_->UnlockSIX(qid_);
      }
      dest_ = std::exchange(rhs.dest_, nullptr);
      qid_ = rhs.qid_;
      ver_ = rhs.ver_;
      return *this;
    }

    // forbid copying
    SIXGuard(const SIXGuard&) = delete;
    auto operator=(const SIXGuard&) -> SIXGuard& = delete;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~SIXGuard()
    {
      if (dest_) {
        dest_->UnlockSIX(qid_);
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
     * @return The version when this guard was created.
     */
    [[nodiscard]]
    constexpr auto
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
    [[nodiscard]]
    auto
    UpgradeToX() noexcept  //
        -> XGuard
    {
      if (!dest_) [[unlikely]] {
        return XGuard{};
      }

      auto* const qnode = QNodeHolder::GetQNode(qid_);
      while (qnode->lock_state.load(kAcquire) & kSMask) {
        // wait for shared lock holders to release their locks
        CPP_UTILITY_SPINLOCK_HINT
      }
      dest_->LockWord().fetch_xor(kXLock, kAcquire);
      std::atomic_thread_fence(kRelease);  // make the X bit visible before writes

      return XGuard{std::exchange(dest_, nullptr), qid_, ver_};
    }

    /**
     * @brief Downgrade this lock to an S lock.
     *
     * @return The lock guard for an S lock.
     * @note After calling the function, this lock guard abandons the lock's
     * ownership.
     */
    [[nodiscard]]
    auto DowngradeToS() noexcept  //
        -> SGuard;

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
        OMCSLock* const dest,
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

    auto
    operator=(                  //
        XGuard&& rhs) noexcept  //
        -> XGuard&
    {
      if (dest_) {
        dest_->UnlockX(qid_, old_ver_, new_ver_);
      }
      dest_ = std::exchange(rhs.dest_, nullptr);
      qid_ = rhs.qid_;
      old_ver_ = rhs.old_ver_;
      new_ver_ = rhs.new_ver_;
      return *this;
    }

    // forbid copying
    XGuard(const XGuard&) = delete;
    auto operator=(const XGuard&) -> XGuard& = delete;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~XGuard()
    {
      if (dest_) {
        dest_->UnlockX(qid_, old_ver_, new_ver_);
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
    DowngradeToSIX() noexcept  //
        -> SIXGuard
    {
      if (!dest_) [[unlikely]] {
        return SIXGuard{};
      }

      const auto ver_xor = static_cast<uint64_t>(old_ver_ ^ new_ver_);
      dest_->LockWord().fetch_xor(kXLock | ver_xor, kRelease);
      return SIXGuard{std::exchange(dest_, nullptr), qid_, new_ver_};
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
    DowngradeToS() noexcept  //
        -> SGuard
    {
      return DowngradeToSIX().DowngradeToS();
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
        OMCSLock* const dest,
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

    auto
    operator=(                    //
        OptGuard&& rhs) noexcept  //
        -> OptGuard&
    {
      if (dest_ && has_lock_) [[unlikely]] {
        dest_->UnlockS(qid_);
      }
      dest_ = std::exchange(rhs.dest_, nullptr);
      qid_ = rhs.qid_;
      ver_ = rhs.ver_;
      retry_num_ = rhs.retry_num_;
      has_lock_ = std::exchange(rhs.has_lock_, false);
      return *this;
    }

    // forbid copying
    OptGuard(const OptGuard&) = delete;
    auto operator=(const OptGuard&) -> OptGuard& = delete;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~OptGuard()
    {
      if (dest_ && has_lock_) [[unlikely]] {
        dest_->UnlockS(qid_);
      }
    }

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
     * @param max_retry The maximum number of optimistic reads.
     * @retval true if a target version does not change from an expected one.
     * @retval false otherwise.
     */
    [[nodiscard]]
    auto
    VerifyVersion(  //
        const uint32_t mask = kNoMask,
        const size_t max_retry = std::numeric_limits<size_t>::max()) noexcept  //
        -> bool
    {
      if (has_lock_) [[unlikely]] {
        dest_->UnlockS(qid_);
        has_lock_ = false;
        return true;
      }

      // verify using the optimistic read procedure
      std::atomic_thread_fence(kAcquire);
      uint64_t cur;
      while (true) {
        cur = dest_->LockWord().load(kRelaxed);
        if ((cur & kXLock) == 0) break;
        std::this_thread::yield();
      }

      const auto expected = ver_;
      ver_ = static_cast<uint32_t>(cur);
      if (((ver_ ^ expected) & mask) == 0) return true;
      if (++retry_num_ < max_retry) return false;  // continue with OCC

      FallBackToSLock(cur);
      return false;
    }

    /**
     * @param mask A bitmask for representing bits to be verified.
     * @retval true if a target version does not change from an expected one.
     * @retval false otherwise.
     */
    [[nodiscard]]
    auto
    ImmediateVerify(                             //
        const uint32_t mask = kNoMask) noexcept  //
        -> bool
    {
      if (has_lock_) [[unlikely]] {
        dest_->UnlockS(qid_);
        has_lock_ = false;
        return true;
      }

      // verify using the optimistic read procedure
      const auto expected = ver_;
      std::atomic_thread_fence(kAcquire);
      const auto cur = dest_->LockWord().load(kRelaxed);
      ver_ = static_cast<uint32_t>(cur);
      return ((ver_ ^ expected) & mask) == 0 && (cur & kXLock) == 0;
    }

    /**
     * @brief Get a shared lock if a given version is the same as the current one.
     *
     * @param mask A bitmask for representing bits to be verified.
     * @retval A guard instance if the lock is acquired.
     * @retval An empty guard instance otherwise.
     * @note This function does not give up acquiring a lock and continues with
     * spinlock and back-off.
     */
    [[nodiscard]]
    auto
    TryLockS(                                    //
        const uint32_t mask = kNoMask) noexcept  //
        -> SGuard
    {
      std::atomic_thread_fence(kAcquire);
      auto guard = dest_->AcquireS(ver_, mask);
      if (guard) {
        dest_ = nullptr;
      }
      return guard;
    }

    /**
     * @brief Get an SIX lock if a given version is the same as the current one.
     *
     * @param mask A bitmask for representing bits to be verified.
     * @retval A guard instance if the lock is acquired.
     * @retval An empty guard instance otherwise.
     */
    [[nodiscard]]
    auto
    TryLockSIX(                                  //
        const uint32_t mask = kNoMask) noexcept  //
        -> SIXGuard
    {
      std::atomic_thread_fence(kAcquire);
      auto guard = dest_->AcquireSIX(ver_, mask);
      if (guard) {
        dest_ = nullptr;
      }
      return guard;
    }

    /**
     * @brief Get an X lock if a given version is the same as the current one.
     *
     * @param mask A bitmask for representing bits to be verified.
     * @retval A guard instance if the lock is acquired.
     * @retval An empty guard instance otherwise.
     */
    [[nodiscard]]
    auto
    TryLockX(                              //
        uint32_t mask = kNoMask) noexcept  //
        -> XGuard
    {
      return TryLockSIX(mask).UpgradeToX();
    }

   private:
    /*########################################################################*
     * Internal utilities
     *########################################################################*/

    /**
     * @brief Try to acquire a shared lock if the lock is held without readers.
     *
     * @param cur The lock state observed in the last verification.
     */
    void FallBackToSLock(  //
        uint64_t cur) noexcept;

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

 private:
  /*##########################################################################*
   * Internal types
   *##########################################################################*/

  /**
   * @brief A class for representing nodes in queue locks.
   *
   */
  struct QNode {
    /// @brief The next queue node if exist.
    std::atomic<QNode*> next;

    /// @brief The state of this node (an owner flag, lock states, and this node's ID).
    std::atomic<uint64_t> lock_state;
  };

  /// @brief A class for managing thread local queue nodes.
  using QNodeHolder = component::QueueNodeHolder<QNode, kQNodeNum>;

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
   * @brief Get a shared lock if a given version is the same as the current one.
   *
   * @param[in,out] ver An expected version. If acquiring the lock fails, this
   * is updated to the current version.
   * @param mask A bitmask for representing bits to be verified (zero for
   * skipping verification, i.e., this function never fails).
   * @retval A guard instance if the lock is acquired.
   * @retval An empty guard instance otherwise.
   */
  [[nodiscard]]
  auto AcquireS(  //
      uint32_t& ver,
      uint32_t mask) noexcept  //
      -> SGuard;

  /**
   * @brief Get an SIX lock if a given version is the same as the current one.
   *
   * @param[in,out] ver An expected version. If acquiring the lock fails, this
   * is updated to the current version.
   * @param mask A bitmask for representing bits to be verified (zero for
   * skipping verification, i.e., this function never fails).
   * @retval A guard instance if the lock is acquired.
   * @retval An empty guard instance otherwise.
   */
  [[nodiscard]]
  auto AcquireSIX(  //
      uint32_t& ver,
      uint32_t mask) noexcept  //
      -> SIXGuard;

  /**
   * @brief Release a shared lock.
   *
   * @param qid A queue node ID that holds the lock.
   * @note If a thread calls this function without acquiring an S lock, it will
   * corrupt an internal lock state.
   */
  void UnlockS(  //
      uint64_t qid) noexcept;

  /**
   * @brief Release a shared-with-intent-exclusive lock.
   *
   * @param qid A queue node ID that holds the lock.
   * @note If a thread calls this function without acquiring an SIX lock, it
   * will corrupt an internal lock state.
   */
  void
  UnlockSIX(  //
      const uint64_t qid) noexcept
  {
    auto* const qnode = QNodeHolder::GetQNode(qid);
    if (qnode->lock_state.fetch_xor(kOwned, kAcqRel) & kSMask) {
      return;  // the predecessor's last shared lock holder will pass the lock
    }
    PassLock(qid);
  }

  /**
   * @brief Pass a lock to the successor and release a given queue node.
   *
   * @param qid A queue node ID that holds the lock.
   * @note This function must be called by the last thread that releases the
   * lock, i.e., after both the owner of the queue node and the predecessor's
   * shared lock holders have released the lock.
   */
  void PassLock(  //
      uint64_t qid) noexcept;

  /**
   * @brief Release an exclusive lock.
   *
   * @param qid A queue node ID that holds the lock.
   * @param old_ver The current version before unlocking.
   * @param new_ver The desired version after unlocking.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void
  UnlockX(  //
      uint64_t qid,
      uint64_t old_ver,
      uint64_t new_ver) noexcept
  {
    // X locks have waited for the predecessor's shared lock holders, so no one
    // refers to the owner flag of this node
    assert((QNodeHolder::GetQNode(qid)->lock_state.load(kRelaxed) & kSMask) == 0);
    LockWord().fetch_xor(kXLock | (old_ver ^ new_ver), kRelease);
    PassLock(qid);
  }

  /**
   * @brief Wait for shared lock acquisition.
   *
   * @param[in,out] cur The current lock state.
   * @return The queue node ID associated with the acquired lock.
   */
  auto WaitSLock(              //
      uint64_t& cur) noexcept  //
      -> uint64_t;

  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  /// @brief A lock state representing an intent-exclusive lock.
  static constexpr uint64_t kIXLock = 1UL << 62UL;

  /// @brief A lock state representing a shared lock.
  static constexpr uint64_t kSLock = 1UL << 48UL;

  /// @brief A bit shift for QNode.
  static constexpr uint64_t kQIDShift = 32UL;

  /// @brief A bit mask for extracting a version value.
  static constexpr uint64_t kVersionMask = ~(~0UL << kQIDShift);

  /// @brief A bit mask for extracting a node pointer.
  static constexpr uint64_t kQIDMask = (kSLock - 1UL) ^ kVersionMask;

  /// @brief A bit mask for extracting a lock state.
  static constexpr uint64_t kLockMask = ~(kVersionMask | kQIDMask | kXLock);

  /// @brief A bit mask for extracting a sharedlock state.
  static constexpr uint64_t kSMask = kLockMask ^ kIXLock;

  /// @brief A bit mask for extracting an X-lock state and a version value.
  static constexpr uint64_t kXAndVersionMask = kXLock | kVersionMask;

  /// @brief A flag in queue nodes for indicating that their owners hold locks.
  static constexpr uint64_t kOwned = 1UL << 63UL;

  /// @brief A bitmask for skipping version verification.
  static constexpr uint32_t kSkipVerify = 0U;

  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  static_assert(std::atomic_ref<uint64_t>::is_always_lock_free);

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state (accessed only via `LockWord`).
  alignas(std::atomic_ref<uint64_t>::required_alignment) mutable uint64_t lock_;

  /*##########################################################################*
   * Static variables
   *##########################################################################*/

  /// @brief A thread local queue node container.
  static thread_local QNodeHolder tls_holder_;  // NOLINT
};

/*############################################################################*
 * Static assertions
 *############################################################################*/

static_assert(Lockable<OMCSLock>);
static_assert(PessimisticallyLockable<OMCSLock>);
static_assert(OptimisticallyLockable<OMCSLock>);
static_assert(HybridLockable<OMCSLock>);
static_assert(std::is_trivially_default_constructible_v<OMCSLock>);
static_assert(std::is_trivially_destructible_v<OMCSLock>);

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_OMCS_LOCK_HPP_
