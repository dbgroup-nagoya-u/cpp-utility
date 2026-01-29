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

// corresponding header
#include "dbgroup/lock/optiql.hpp"

// C++ standard libraries
#include <atomic>
#include <bit>
#include <cstdint>
#include <functional>
#include <thread>
#include <utility>
#include <vector>

// temp
#include <stdexcept>

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
  std::atomic<QNode *> next;

  /// @brief A flag for indicating this node's owner holds a lock.
  std::atomic_bool hold_lock;
};

/// @brief A class for managing thread local queue nodes.
using QNodeHolder = component::QueueNodeHolder<QNode, OptiQL::kQNodeNum>;

/*############################################################################*
 * Local constants
 *############################################################################*/

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 63UL;

/// @brief A lock state representing an opportunistic lock.
constexpr uint64_t kOPReadFlag = 1UL << 62UL;

/// @brief A bit mask for extracting an X-lock state and opportunistic read flag.
constexpr uint64_t kXAndOPReadMask = kXLock | kOPReadFlag;

/// @brief A bit mask for extracting a version value.
constexpr uint64_t kVersionMask = ~(~0UL << 32UL);

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kQIDMask = (kOPReadFlag - 1UL) ^ kVersionMask;

/// @brief A bit shift for QNode.
constexpr uint64_t kQIDShift = 32UL;

/// @brief A bit mask for extracting a lock state.
constexpr uint64_t kLockMask = ~(kVersionMask | kQIDMask);

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
OptiQL::GetVersion() const noexcept  //
    -> OptGuard
{
  uint64_t cur{};
  while (true) {
    cur = lock_.load(kAcquire);
    if ((cur & kXAndOPReadMask) != kXLock) break;  // lock released or opportunistically readable
    std::this_thread::yield();
  }

  return OptGuard{this, static_cast<uint32_t>(cur & kVersionMask)};
}

auto
OptiQL::LockX()  //
    -> XGuard
{
  const auto qid = tls_holder.GetQID();
  auto *qnode = new (QNodeHolder::GetQNode(qid)) QNode{};
  const auto new_tail = (static_cast<uint64_t>(qid) << kQIDShift) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    if (lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if ((cur & kLockMask) != kNoLocks) {
    // wait until predecessor gives up the lock
    auto *pred_qnode = QNodeHolder::GetQNode((cur & kQIDMask) >> kQIDShift);
    pred_qnode->next.store(qnode, kRelaxed);
    while (!qnode->hold_lock.load(kRelaxed)) {
      std::this_thread::yield();
    }
    // disable opportunistic read
    cur = lock_.fetch_xor(kOPReadFlag, kAcquire);
  }

  return XGuard{this, qid, static_cast<uint32_t>(cur & kVersionMask)};
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

void
OptiQL::UnlockX(  //
    const uint64_t qid,
    const uint64_t ver)
{
  auto *qnode = QNodeHolder::GetQNode(qid);

  auto *next_ptr = qnode->next.load(kAcquire);
  if (next_ptr == nullptr) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid) {
      if (lock_.compare_exchange_weak(cur, ver, kRelease, kRelaxed)) {
        tls_holder.ReleaseQID(qid);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  // enable opportunistic read
  lock_.fetch_or(kOPReadFlag | ver, kRelease);
  while (true) {  // wait until successor fills in its next field
    next_ptr = qnode->next.load(kRelaxed);
    if (next_ptr) break;
    CPP_UTILITY_SPINLOCK_HINT
  }
  next_ptr->hold_lock.store(true, kRelaxed);
  tls_holder.ReleaseQID(qid);
}

/*############################################################################*
 * Exclusive lock guards
 *############################################################################*/

auto
OptiQL::XGuard::operator=(  //
    XGuard &&rhs) noexcept  //
    -> XGuard &
{
  if (dest_) {
    dest_->UnlockX(qid_, new_ver_);
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
    dest_->UnlockX(qid_, new_ver_);
  }
}

/*############################################################################*
 * Optimistic lock guards
 *############################################################################*/

auto
OptiQL::OptGuard::VerifyVersion() noexcept  //
    -> bool
{
  uint64_t cur{};
  while (true) {
    std::atomic_thread_fence(kRelease);
    cur = dest_->lock_.load(kRelaxed);
    if ((cur & kXAndOPReadMask) != kXLock) break;  // lock released or opportunistically readable
    std::this_thread::yield();
  }

  const auto expected = ver_;
  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  return ver_ == expected;
}

}  // namespace dbgroup::lock
