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
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
/*############################################################################*
 * Static variables
 *############################################################################*/

thread_local OMCSLock::QNodeHolder OMCSLock::tls_holder_{};  // NOLINT

/*############################################################################*
 * Internal APIs
 *############################################################################*/

auto
OMCSLock::AcquireS(  //
    uint32_t& ver,
    const uint32_t mask) noexcept  //
    -> SGuard
{
  auto qid = tls_holder_.GetQID();
  const auto qid_bits = static_cast<uint64_t>(qid) << kQIDShift;
  new (QNodeHolder::GetQNode(qid)) QNode{nullptr, qid_bits};
  const auto ebd_id = qid_bits | kSLock;

  auto cur = LockWord().load(kRelaxed);
  while (true) {
    const auto cur_ver = static_cast<uint32_t>(cur);
    if ((ver ^ cur_ver) & mask) {
      ver = cur_ver;
      tls_holder_.ReleaseQID(qid);
      return SGuard{};
    }
    if (cur & kLockMask) {  // there is the predecessor
      if (LockWord().compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed)) {
        tls_holder_.ReleaseQID(qid);
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
  const auto qid = tls_holder_.GetQID();
  const auto qid_bits = static_cast<uint64_t>(qid) << kQIDShift;
  auto* const qnode = new (QNodeHolder::GetQNode(qid)) QNode{};
  const auto ebd_id = qid_bits | kIXLock;

  auto cur = LockWord().load(kRelaxed);
  while (true) {
    const auto cur_ver = static_cast<uint32_t>(cur);
    if ((ver ^ cur_ver) & mask) {
      ver = cur_ver;
      tls_holder_.ReleaseQID(qid);
      return SIXGuard{};
    }
    qnode->lock_state.store((cur & kLockMask) | kOwned | qid_bits, kRelaxed);
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
        tls_holder_.ReleaseQID(qid);
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

  const auto old = next_node->lock_state.fetch_sub(kSLock, kAcqRel);
  if ((old & kLockMask) == kSLock) {
    tls_holder_.ReleaseQID(qid);
  }
  if ((old & (kOwned | kLockMask)) == kSLock) {  // the successor's owner has released the lock
    PassLock((old & kQIDMask) >> kQIDShift);
  }
}

void
OMCSLock::PassLock(  //
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
        tls_holder_.ReleaseQID(qid);
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
    tls_holder_.ReleaseQID(qid);
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
OMCSLock::SIXGuard::DowngradeToS() noexcept  //
    -> SGuard
{
  if (!dest_) [[unlikely]] {
    return SGuard{};
  }

  // join the shared lock holders that take over this node, and then release
  // the SIX lock as usual (i.e., the predecessor's last shared lock holder
  // may pass this node instead of this thread)
  auto* const lock = std::exchange(dest_, nullptr);
  auto* const qnode = QNodeHolder::GetQNode(qid_);
  auto* next_node = qnode->next.load(kAcquire);
  if (!next_node) {  // this is the tail node
    auto cur = lock->LockWord().load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid_) {
      if (lock->LockWord().compare_exchange_weak(cur, cur + kSLock, kRelaxed, kRelaxed)) {
        lock->UnlockSIX(qid_);
        return SGuard{lock, qid_, ver_};
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next_node = qnode->next.load(kAcquire);
      if (next_node) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  next_node->lock_state.fetch_add(kSLock, kRelaxed);
  lock->UnlockSIX(qid_);
  return SGuard{lock, qid_, ver_};
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

}  // namespace dbgroup::lock
