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

// corresponding header
#include "dbgroup/lock/omcs_lock.hpp"

// C++ standard libraries
#include <atomic>
#include <cstdint>
#include <thread>
#include <utility>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/component/queue_node_holder.hpp"
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
namespace
{
/*############################################################################*
 * Local types
 *############################################################################*/

/**
 * @brief A class for representing nodes in queue locks.
 *
 */
struct QNode {
  /// @brief The next queue node if exist.
  std::atomic<QNode*> next;

  /// @brief A flag for indicating this node's owner holds a lock.
  std::atomic<uint64_t> lock_state;
};

/// @brief A class for managing thread local queue nodes.
using QNodeHolder = component::QueueNodeHolder<QNode, OMCSLock::kQNodeNum>;

/*############################################################################*
 * Local constants
 *############################################################################*/

/// @brief The `uintptr_t` of nullptr.
constexpr uint64_t kNull = 0;

/// @brief A lock state representing an opportunistic lock.
constexpr uint64_t kSFlag = OMCSLock::kSFlag;

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 62UL;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << 48UL;

/// @brief A bit shift for QNode.
constexpr uint64_t kQIDShift = 32UL;

/// @brief A bit mask for extracting a version value.
constexpr uint64_t kVersionMask = ~(~0UL << kQIDShift);

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kQIDMask = (kSLock - 1UL) ^ kVersionMask;

/// @brief A bit mask for extracting a lock state.
constexpr uint64_t kLockMask = ~(kVersionMask | kQIDMask | kSFlag);

/// @brief A bit mask for extracting a sharedlock state.
constexpr uint64_t kSMask = kLockMask ^ kXLock;

/// @brief A bit mask for extracting a opportunistic read flag and a version value.
constexpr uint64_t kSAndVersionMask = kSFlag | kVersionMask;

/*############################################################################*
 * Static variables
 *############################################################################*/
// NOLINTBEGIN

/// @brief A thread local queue node container.
thread_local QNodeHolder tls_holder{};

// NOLINTEND
}  // namespace

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
OMCSLock::GetVersion() noexcept  //
    -> OptGuard
{
  uint64_t cur{};
  while (true) {
    cur = lock_.load(kAcquire);
    if (cur & kSFlag) break;
    std::this_thread::yield();
  }

  return OptGuard{this, static_cast<uint32_t>(cur & kVersionMask)};
}

auto
OMCSLock::LockS()  //
    -> SGuard
{
  auto qid = tls_holder.GetQID();
  new (QNodeHolder::GetQNode(qid)) QNode{};
  const auto ebd_id = (static_cast<uint64_t>(qid) << kQIDShift) | kSFlag | kSLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    if (cur & kQIDMask) {  // there is the predecessor
      if (lock_.compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed)) {
        tls_holder.ReleaseQID(qid);
        qid = WaitSLock(cur);
        break;
      }
    } else {  // the initial shared lock
      const auto new_tail = ebd_id | (cur & kVersionMask);
      if (lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    }
    CPP_UTILITY_SPINLOCK_HINT
  }

  return SGuard{this, qid, static_cast<uint32_t>(cur & kVersionMask)};
}

auto
OMCSLock::LockSIX()  //
    -> SIXGuard
{
  const auto qid = tls_holder.GetQID();
  auto* qnode = new (QNodeHolder::GetQNode(qid)) QNode{};
  const auto ebd_id = (static_cast<uint64_t>(qid) << kQIDShift) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    qnode->lock_state.store(cur & kLockMask, kRelaxed);
    const auto new_tail = ebd_id | (cur & kSAndVersionMask);
    if (lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if (cur & kLockMask) {
    // wait until predecessor gives up the lock
    auto* pred_qnode = QNodeHolder::GetQNode((cur & kQIDMask) >> kQIDShift);
    pred_qnode->next.store(qnode, kRelaxed);
    while (qnode->lock_state.load(kRelaxed) & kXLock) {
      std::this_thread::yield();
    }
    cur = lock_.load(kAcquire);
  }

  return SIXGuard{this, qid, static_cast<uint32_t>(cur & kVersionMask)};
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

void
OMCSLock::UnlockS(  //
    const uint64_t qid)
{
  auto* qnode = QNodeHolder::GetQNode(qid);

  auto* next_node = qnode->next.load(kRelaxed);
  if (next_node == nullptr) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid) {
      const auto unlock = cur - kSLock;
      if (unlock & kLockMask) {
        if (lock_.compare_exchange_weak(cur, unlock, kRelease, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, (cur & kVersionMask) | kSFlag,  //
                                             kRelease, kRelaxed)) {
        tls_holder.ReleaseQID(qid);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next_node = qnode->next.load(kRelaxed);
      if (next_node) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  if ((next_node->lock_state.fetch_sub(kSLock, kRelease) & kLockMask) == kSLock) {
    tls_holder.ReleaseQID(qid);
  }
}

void
OMCSLock::UnlockSIX(  //
    const uint64_t qid)
{
  auto* qnode = QNodeHolder::GetQNode(qid);

  auto* next_ptr = qnode->next.load(kRelaxed);
  if (next_ptr == nullptr) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid) {
      if (lock_.compare_exchange_weak(cur, cur ^ kXLock, kRelease, kRelaxed)) {
        if ((cur & kSMask) == kNoLocks) {
          tls_holder.ReleaseQID(qid);
        }
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  while (true) {  // wait until successor fills in its next field
    next_ptr = qnode->next.load(kRelaxed);
    if (next_ptr) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if ((next_ptr->lock_state.fetch_xor(kXLock, kRelease) & kLockMask) == kXLock) {
    tls_holder.ReleaseQID(qid);
  }
}

auto
OMCSLock::WaitSLock(         //
    uint64_t& cur) noexcept  //
    -> uint64_t
{
  auto qid = (cur & kQIDMask) >> kQIDShift;
  auto* qnode = QNodeHolder::GetQNode(qid);
  if (cur & kXLock) {  // wait for the predecessor to release the lock
    while (((cur & kQIDMask) >> kQIDShift) == qid && (cur & kXLock)) {
      std::this_thread::yield();
      cur = lock_.load(kAcquire);
    }
    if (((cur & kQIDMask) >> kQIDShift) != qid) {
      QNode* next_ptr;
      while (true) {  // wait until successor fills in its next field
        next_ptr = qnode->next.load(kRelaxed);
        if (next_ptr) break;
        CPP_UTILITY_SPINLOCK_HINT
      }

      while (next_ptr->lock_state.load(kRelaxed) & kXLock) {
        std::this_thread::yield();
      }
      cur = lock_.load(kAcquire);
    }
  }
  return qid;
}

/*############################################################################*
 * Shared-with-intent-exclusive lock guards
 *############################################################################*/

auto
OMCSLock::SIXGuard::UpgradeToX()  //
    -> XGuard
{
  if (dest_ == nullptr) return XGuard{};

  auto* qnode = QNodeHolder::GetQNode(qid_);
  while (qnode->lock_state.load(kRelaxed) & kSMask) {
    // wait for shared lock holders to release their locks
    CPP_UTILITY_SPINLOCK_HINT
  }
  dest_->lock_.fetch_xor(kSFlag, kAcquire);

  return XGuard{std::exchange(dest_, nullptr), qid_, ver_};
}

/*############################################################################*
 * Optimistic lock guards
 *############################################################################*/

auto
OMCSLock::OptGuard::VerifyVersion(  //
    const uint32_t mask,
    const size_t max_retry) noexcept  //
    -> bool
{
  if (has_lock_) {
    dest_->UnlockS(qid_);
    has_lock_ = false;
    return true;
  }

  // verify using the optimistic read procedure
  std::atomic_thread_fence(kAcquire);
  uint64_t cur;
  while (true) {
    cur = dest_->lock_.load(kRelaxed);
    if (cur & kSFlag) break;
    std::this_thread::yield();
  }

  const auto expected = ver_;
  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  if (((ver_ ^ expected) & mask) == 0) return true;
  if (++retry_num_ < max_retry) return false;  // continue with OCC

  // try to acquire a shared lock
  retry_num_ = 0;
  while ((cur & kSMask) == kNoLocks && (cur & kQIDMask) != kNull) {
    has_lock_ = dest_->lock_.compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed);
    if (has_lock_) {
      qid_ = dest_->WaitSLock(cur);
      break;
    }
    CPP_UTILITY_SPINLOCK_HINT
  }
  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  return false;
}

auto
OMCSLock::OptGuard::ImmediateVerify(  //
    const uint32_t mask) noexcept     //
    -> bool
{
  if (has_lock_) {
    dest_->UnlockS(ver_);
    has_lock_ = false;
    return true;
  }

  // verify using the optimistic read procedure
  const auto expected = ver_;
  std::atomic_thread_fence(kAcquire);
  const auto cur = dest_->lock_.load(kRelaxed);
  ver_ = static_cast<uint32_t>(cur);
  return ((ver_ ^ expected) & mask) == 0 && (cur & kSFlag) > 0;
}

auto
OMCSLock::OptGuard::TryLockS(  //
    const uint32_t mask)       //
    -> SGuard
{
  const auto expected = ver_;
  auto qid = tls_holder.GetQID();
  new (QNodeHolder::GetQNode(qid)) QNode{};
  const auto ebd_id = (static_cast<uint64_t>(qid) << kQIDShift) | kSLock | kSFlag;

  std::atomic_thread_fence(kAcquire);
  auto cur = dest_->lock_.load(kRelaxed);
  auto tail_qid = (cur & kQIDMask) >> kQIDShift;
  while (true) {
    ver_ = static_cast<uint32_t>(cur & kVersionMask);
    if ((cur ^ expected) & mask) {
      tls_holder.ReleaseQID(qid);
      return SGuard{};
    }
    if (tail_qid) {  // there is the predecessor
      if (dest_->lock_.compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed)) {
        tls_holder.ReleaseQID(qid);
        qid = dest_->WaitSLock(cur);
        ver_ = static_cast<uint32_t>(cur & kVersionMask);
        break;
      }
    } else {  // the initial shared lock
      const auto new_tail = ebd_id | (cur & kVersionMask);
      if (dest_->lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    }
    CPP_UTILITY_SPINLOCK_HINT
  }

  if ((ver_ ^ expected) & mask) {
    dest_->UnlockS(qid);
    return SGuard{};
  }
  return SGuard{std::exchange(dest_, nullptr), qid, ver_};
}

auto
OMCSLock::OptGuard::TryLockSIX(  //
    const uint32_t mask)         //
    -> SIXGuard
{
  const auto expected = ver_;
  const auto qid = tls_holder.GetQID();
  auto* qnode = new (QNodeHolder::GetQNode(qid)) QNode{};
  const auto ebd_id = (static_cast<uint64_t>(qid) << kQIDShift) | kXLock;

  std::atomic_thread_fence(kAcquire);
  auto cur = dest_->lock_.load(kRelaxed);
  while (true) {
    ver_ = static_cast<uint32_t>(cur & kVersionMask);
    if ((cur ^ expected) & mask) {
      tls_holder.ReleaseQID(qid);
      return SIXGuard{};
    }
    qnode->lock_state.store(cur & kLockMask, kRelaxed);
    const auto new_tail = ebd_id | (cur & kSAndVersionMask);
    if (dest_->lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if (cur & kLockMask) {
    // wait until predecessor gives up the lock
    auto* pred_qnode = QNodeHolder::GetQNode((cur & kQIDMask) >> kQIDShift);
    pred_qnode->next.store(qnode, kRelaxed);
    while (qnode->lock_state.load(kRelaxed) & kXLock) {
      std::this_thread::yield();
    }
  }

  cur = dest_->lock_.load(kAcquire);
  ver_ = static_cast<uint32_t>(cur & kVersionMask);

  if ((ver_ ^ expected) & mask) {
    dest_->UnlockSIX(qid);
    return SIXGuard{};
  }
  return SIXGuard{std::exchange(dest_, nullptr), qid, ver_};
}

}  // namespace dbgroup::lock
