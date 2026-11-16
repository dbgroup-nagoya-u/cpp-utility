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

// the corresponding header
#include "dbgroup/lock/optiql.hpp"

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
  std::atomic_bool hold_lock;
};

/// @brief A class for managing thread local queue nodes.
using QNodeHolder = component::QueueNodeHolder<QNode, OptiQL::kQNodeNum>;

/*############################################################################*
 * Local constants
 *############################################################################*/

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
OptiQL::LockX()  //
    -> XGuard
{
  uint32_t ver{};
  return AcquireX(ver, kSkipVerify);
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

auto
OptiQL::AcquireX(  //
    uint32_t& ver,
    const uint32_t mask)  //
    -> XGuard
{
  const auto qid = tls_holder.GetQID();
  auto* const qnode = new (QNodeHolder::GetQNode(qid)) QNode{};
  const auto qid_bits = (static_cast<uint64_t>(qid) << kQIDShift) | kXLock;

  auto cur = LockWord().load(kRelaxed);
  while (true) {
    const auto cur_ver = static_cast<uint32_t>(cur & kVersionMask);
    if ((ver ^ cur_ver) & mask) {
      ver = cur_ver;
      tls_holder.ReleaseQID(qid);
      return XGuard{};
    }
    // replace the tail and clear the opportunistic read flag, keeping the version
    const auto new_tail = (cur & kVersionMask) | qid_bits;
    if (LockWord().compare_exchange_weak(cur, new_tail, kAcqRel, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if ((cur & kLockMask) != kNoLocks) {
    // wait until predecessor gives up the lock
    auto* const pred_qnode = QNodeHolder::GetQNode((cur & kQIDMask) >> kQIDShift);
    pred_qnode->next.store(qnode, kRelease);
    while (!qnode->hold_lock.load(kAcquire)) {
      std::this_thread::yield();
    }
    // disable opportunistic read (the flag may have been cleared by successors)
    cur = LockWord().fetch_and(~kOPReadFlag, kAcquire);
  }

  const auto cur_ver = static_cast<uint32_t>(cur & kVersionMask);
  if ((ver ^ cur_ver) & mask) {
    ver = cur_ver;
    UnlockX(qid, cur_ver, cur_ver);  // release the lock without modifying the version
    return XGuard{};
  }
  std::atomic_thread_fence(kRelease);  // make the X bit visible before writes
  return XGuard{this, qid, cur_ver};
}

void
OptiQL::UnlockX(  //
    const uint64_t qid,
    const uint64_t old_ver,
    const uint64_t new_ver)
{
  auto* const qnode = QNodeHolder::GetQNode(qid);

  auto* next_ptr = qnode->next.load(kAcquire);
  if (!next_ptr) {  // this is the tail node
    auto cur = LockWord().load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid) {
      if (LockWord().compare_exchange_weak(cur, new_ver, kRelease, kRelaxed)) {
        tls_holder.ReleaseQID(qid);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  // enable opportunistic read and replace the version (`old_ver` -> `new_ver`)
  LockWord().fetch_xor(kOPReadFlag | (old_ver ^ new_ver), kRelease);
  while (true) {  // wait until successor fills in its next field
    next_ptr = qnode->next.load(kAcquire);
    if (next_ptr) break;
    CPP_UTILITY_SPINLOCK_HINT
  }
  next_ptr->hold_lock.store(true, kRelease);
  tls_holder.ReleaseQID(qid);
}

/*############################################################################*
 * Exclusive lock guards
 *############################################################################*/

auto
OptiQL::XGuard::operator=(  //
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

OptiQL::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX(qid_, old_ver_, new_ver_);
  }
}

/*############################################################################*
 * Optimistic lock guards
 *############################################################################*/

auto
OptiQL::OptGuard::TryLockX(  //
    const uint32_t mask)     //
    -> XGuard
{
  std::atomic_thread_fence(kAcquire);
  auto guard = dest_->AcquireX(ver_, mask);
  if (guard) {
    dest_ = nullptr;
  }
  return guard;
}

}  // namespace dbgroup::lock
