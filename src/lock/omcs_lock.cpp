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

// the corresponding header
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

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = OMCSLock::kXLock;

/// @brief A lock state representing an intent-exclusive lock.
constexpr uint64_t kIXLock = 1UL << 62UL;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << 48UL;

/// @brief A bit shift for QNode.
constexpr uint64_t kQIDShift = 32UL;

/// @brief A bit mask for extracting a version value.
constexpr uint64_t kVersionMask = ~(~0UL << kQIDShift);

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kQIDMask = (kSLock - 1UL) ^ kVersionMask;

/// @brief A bit mask for extracting a lock state.
constexpr uint64_t kLockMask = ~(kVersionMask | kQIDMask | kXLock);

/// @brief A bit mask for extracting a sharedlock state.
constexpr uint64_t kSMask = kLockMask ^ kIXLock;

/// @brief A bit mask for extracting an X-lock state and a version value.
constexpr uint64_t kXAndVersionMask = kXLock | kVersionMask;

/// @brief A bitmask for skipping version verification.
constexpr uint32_t kSkipVerify = 0U;

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
OMCSLock::LockS() noexcept  //
    -> SGuard
{
  uint32_t ver{};
  return AcquireS(ver, kSkipVerify);
}

auto
OMCSLock::LockSIX() noexcept  //
    -> SIXGuard
{
  uint32_t ver{};
  return AcquireSIX(ver, kSkipVerify);
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

auto
OMCSLock::AcquireS(  //
    uint32_t& ver,
    const uint32_t mask) noexcept  //
    -> SGuard
{
  auto qid = tls_holder.GetQID();
  new (QNodeHolder::GetQNode(qid)) QNode{};
  const auto ebd_id = (static_cast<uint64_t>(qid) << kQIDShift) | kSLock;

  auto cur = LockWord().load(kRelaxed);
  while (true) {
    const auto cur_ver = static_cast<uint32_t>(cur);
    if ((ver ^ cur_ver) & mask) {
      ver = cur_ver;
      tls_holder.ReleaseQID(qid);
      return SGuard{};
    }
    if (cur & kLockMask) {  // there is the predecessor
      if (LockWord().compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed)) {
        tls_holder.ReleaseQID(qid);
        qid = WaitSLock(cur);
        break;
      }
    } else {  // the initial shared lock
      const auto new_tail = ebd_id | (cur & kVersionMask);
      if (LockWord().compare_exchange_weak(cur, new_tail, kAcqRel, kRelaxed)) break;
    }
    CPP_UTILITY_SPINLOCK_HINT
  }

  const auto cur_ver = static_cast<uint32_t>(cur);
  if ((ver ^ cur_ver) & mask) {
    ver = cur_ver;
    UnlockS(qid);
    return SGuard{};
  }
  return SGuard{this, qid, cur_ver};
}

auto
OMCSLock::AcquireSIX(  //
    uint32_t& ver,
    const uint32_t mask) noexcept  //
    -> SIXGuard
{
  const auto qid = tls_holder.GetQID();
  auto* const qnode = new (QNodeHolder::GetQNode(qid)) QNode{};
  const auto ebd_id = (static_cast<uint64_t>(qid) << kQIDShift) | kIXLock;

  auto cur = LockWord().load(kRelaxed);
  while (true) {
    const auto cur_ver = static_cast<uint32_t>(cur);
    if ((ver ^ cur_ver) & mask) {
      ver = cur_ver;
      tls_holder.ReleaseQID(qid);
      return SIXGuard{};
    }
    qnode->lock_state.store(cur & kLockMask, kRelaxed);
    const auto new_tail = ebd_id | (cur & kXAndVersionMask);
    if (LockWord().compare_exchange_weak(cur, new_tail, kAcqRel, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if (cur & kLockMask) {
    // wait until predecessor gives up the lock
    auto* const pred_qnode = QNodeHolder::GetQNode((cur & kQIDMask) >> kQIDShift);
    pred_qnode->next.store(qnode, kRelease);
    // the acquire load also makes the following load read the latest version
    while (qnode->lock_state.load(kAcquire) & kIXLock) {
      std::this_thread::yield();
    }
    cur = LockWord().load(kAcquire);
  }

  const auto cur_ver = static_cast<uint32_t>(cur);
  if ((ver ^ cur_ver) & mask) {
    ver = cur_ver;
    UnlockSIX(qid);
    return SIXGuard{};
  }
  return SIXGuard{this, qid, cur_ver};
}

void
OMCSLock::UnlockS(  //
    const uint64_t qid) noexcept
{
  auto* const qnode = QNodeHolder::GetQNode(qid);

  auto* next_node = qnode->next.load(kAcquire);
  if (!next_node) {  // this is the tail node
    auto cur = LockWord().load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid) {
      const auto unlock = cur - kSLock;
      if (unlock & kLockMask) {
        if (LockWord().compare_exchange_weak(cur, unlock, kRelease, kRelaxed)) return;
      } else if (LockWord().compare_exchange_weak(cur, cur & kVersionMask, kAcqRel, kRelaxed)) {
        tls_holder.ReleaseQID(qid);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next_node = qnode->next.load(kAcquire);
      if (next_node) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  if ((next_node->lock_state.fetch_sub(kSLock, kAcqRel) & kLockMask) == kSLock) {
    tls_holder.ReleaseQID(qid);
  }
}

void
OMCSLock::UnlockSIX(  //
    const uint64_t qid) noexcept
{
  auto* const qnode = QNodeHolder::GetQNode(qid);

  auto* next_ptr = qnode->next.load(kAcquire);
  if (!next_ptr) {  // this is the tail node
    auto cur = LockWord().load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid) {
      if (cur & kSMask) {  // the shared lock holders take over this node
        if (LockWord().compare_exchange_weak(cur, cur ^ kIXLock, kRelease, kRelaxed)) return;
      } else if (LockWord().compare_exchange_weak(cur, cur & kXAndVersionMask, kRelease,
                                                  kRelaxed)) {
        tls_holder.ReleaseQID(qid);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  while (true) {  // wait until successor fills in its next field
    next_ptr = qnode->next.load(kAcquire);
    if (next_ptr) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if ((next_ptr->lock_state.fetch_xor(kIXLock, kRelease) & kLockMask) == kIXLock) {
    tls_holder.ReleaseQID(qid);
  }
}

auto
OMCSLock::WaitSLock(         //
    uint64_t& cur) noexcept  //
    -> uint64_t
{
  auto qid = (cur & kQIDMask) >> kQIDShift;
  auto* const qnode = QNodeHolder::GetQNode(qid);
  if (cur & kIXLock) {  // wait for the predecessor to release the lock
    while (((cur & kQIDMask) >> kQIDShift) == qid && (cur & kIXLock)) {
      std::this_thread::yield();
      cur = LockWord().load(kAcquire);
    }
    if (((cur & kQIDMask) >> kQIDShift) != qid) {
      QNode* next_ptr;
      while (true) {  // wait until successor fills in its next field
        next_ptr = qnode->next.load(kAcquire);
        if (next_ptr) break;
        CPP_UTILITY_SPINLOCK_HINT
      }

      while (next_ptr->lock_state.load(kAcquire) & kIXLock) {
        std::this_thread::yield();
      }
      cur = LockWord().load(kAcquire);
    }
  }
  return qid;
}

/*############################################################################*
 * Shared-with-intent-exclusive lock guards
 *############################################################################*/

auto
OMCSLock::SIXGuard::UpgradeToX() noexcept  //
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

/*############################################################################*
 * Optimistic lock guards
 *############################################################################*/

void
OMCSLock::OptGuard::FallBackToSLock(  //
    uint64_t cur) noexcept
{
  retry_num_ = 0;
  while ((cur & kLockMask) == kIXLock) {  // an SIX/X lock without shared lock holders
    has_lock_ = dest_->LockWord().compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed);
    if (has_lock_) {
      qid_ = dest_->WaitSLock(cur);
      break;
    }
    CPP_UTILITY_SPINLOCK_HINT
  }
  ver_ = static_cast<uint32_t>(cur);
}

auto
OMCSLock::OptGuard::TryLockS(      //
    const uint32_t mask) noexcept  //
    -> SGuard
{
  std::atomic_thread_fence(kAcquire);
  auto guard = dest_->AcquireS(ver_, mask);
  if (guard) {
    dest_ = nullptr;
  }
  return guard;
}

auto
OMCSLock::OptGuard::TryLockSIX(    //
    const uint32_t mask) noexcept  //
    -> SIXGuard
{
  std::atomic_thread_fence(kAcquire);
  auto guard = dest_->AcquireSIX(ver_, mask);
  if (guard) {
    dest_ = nullptr;
  }
  return guard;
}

}  // namespace dbgroup::lock
