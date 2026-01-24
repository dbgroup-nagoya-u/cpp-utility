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
#include "dbgroup/lock/omcs_lock.hpp"

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
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
namespace
{
/*############################################################################*
 * Local types
 *############################################################################*/

struct QNode {
  /// @brief The next queue node if exist.
  std::atomic<QNode *> next;

  /// @brief A flag for indicating this node's owner holds a lock.
  std::atomic_bool hold_lock;
};

/*############################################################################*
 * Local constants
 *############################################################################*/

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 63UL;

/// @brief A lock state representing an opportunistic lock.
constexpr uint64_t kOPReadFlag = 1UL << 62UL;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << 48UL;

/// @brief A bit mask for extracting an X-lock state and opportunistic read flag.
constexpr uint64_t kXAndOPReadMask = kXLock | kOPReadFlag;

/// @brief A bit mask for extracting a version value.
constexpr uint64_t kVersionMask = ~(~0UL << 32UL);

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kQIDMask = (kSLock - 1UL) ^ kVersionMask;

/// @brief A bit mask for extracting a sharedlock state.
constexpr uint64_t kSMask = kLockMask ^ kXAndOPReadMask;

/// @brief A bit shift for QNode.
constexpr uint64_t kQIDShift = 32UL;

/// @brief A bit mask for extracting a lock state.
constexpr uint64_t kLockMask = ~(kVersionMask | kQIDMask);

/// @brief A bit mask for extracting a sharedlock state.
constexpr uint64_t kSMask = kLockMask ^ kXLock;

/// @brief The number of bits in one word.
constexpr uint64_t kBitNum = 64UL;

/// @brief The maximum number of thread local queue nodes.
constexpr uint32_t kMaxTLSNum = 8;

/// @brief The size of a buffer for managing queue node IDs.
constexpr uint64_t kIDBufSize = OMCSLock::kQNodeNum / kBitNum;

/*############################################################################*
 * Static variables
 *############################################################################*/

// The definition of a static member.
QNode _qnodes[OMCSLock::kQNodeNum] = {};  // NOLINT

/// @brief The container of queue node IDs.
alignas(kVMPageSize) std::atomic_uint64_t _id_buf[kIDBufSize] = {};  // NOLINT

/// @brief A thread local queue node container.
thread_local std::vector<uint32_t> _tls{};  // NOLINT

/*############################################################################*
 * Local utilities
 *############################################################################*/

/**
 * @return A unique QID.
 */
auto
GetQID()  //
    -> uint32_t
{
  if (!_tls.empty()) {
    const auto qid = _tls.back();
    _tls.pop_back();
    return qid;
  }

  constexpr uint32_t kIDBufMask = kIDBufSize - 1U;
  constexpr uint64_t kFilledIDs = ~0UL;
  thread_local const auto base = std::hash<std::thread::id>{}(std::this_thread::get_id());
  for (uint32_t pos = base; true; ++pos) {
    pos &= kIDBufMask;
    auto cur = _id_buf[pos].load(kRelaxed);
    while (cur < kFilledIDs) {
      const uint32_t cto = std::countr_one(cur);
      const auto flag = 1UL << cto;
      cur = _id_buf[pos].fetch_or(flag, kRelaxed);
      if ((cur & flag) == 0UL) return pos * kBitNum + cto;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }
}

/**
 * @brief Retain a QID for reusing in the future.
 *
 * @param qid A QID to be reused.
 */
void
RetainQID(  //
    const uint32_t qid)
{
  auto *qnode = &(_qnodes[qid]);
  qnode->next.store(nullptr, kRelaxed);
  qnode->hold_lock.store(false, kRelaxed);

  if (_tls.size() < kMaxTLSNum) {
    _tls.emplace_back(qid);
  } else {
    constexpr uint32_t kFlagMask = kBitNum - 1U;
    _id_buf[qid / kBitNum].fetch_xor(qid & kFlagMask, kRelaxed);
  }
}

}  // namespace

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
OMCSLock::GetVersion() const noexcept  //
    -> OptGuard
{
  uint64_t cur{};
  while (true) {
    cur = lock_.load(kAcquire);
    if ((cur & kXAndOPReadMask) != kXLock) break;  // lock released or opportunistically readable
    std::this_thread::yield();
  }

  return OptGuard{this, static_cast<uint32_t>(cur & kVersionMask)};
}  // namespace

auto
OMCSLock::LockS()  //
    -> SGuard
{
  const auto qid = GetQID();
  auto *qnode = &(_qnodes[qid]);
  const auto new_tail = (static_cast<uint64_t>(qid) << kQIDShift) | kSLock;

  auto cur = lock_.load(kRelaxed);
  auto tail_qid = (cur & kQIDMask) >> kQIDShift;
  while (true) {
    if (tail_qid) {  // there is the predecessor
      if (lock_.compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed)) break;
    } else if (lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) {
      goto end;  // the initial shared lock
    }
    CPP_UTILITY_SPINLOCK_HINT
  }

  Retain(qid);
  qid = cur & kQIDMask;
  auto *qnode = &(_qnodes[qid]);
  if (cur & kXLock) {  // wait for the predecessor to release the lock
    while (((cur & kQIDMask) >> kQIDShift) == qid && (cur & kXLock)) {
      std::this_thread::yield();
      cur = lock_.load(kAcquire);
    }
    if (((cur & kQIDMask) >> kQIDShift) != qid) {
      while (true) {  // wait until successor fills in its next field
        next_ptr = qnode->next.load(kRelaxed);
        if (next_ptr) break;
        CPP_UTILITY_SPINLOCK_HINT
      }
      auto *pred_qnode = &(_qnodes[(cur & kQIDMask) >> kQIDShift]);
      pred_qnode->next.store(qnode, kRelaxed);
      while (!qnode->hold_lock.load(kRelaxed)) {
        std::this_thread::yield();
      }
    }
  }
end:
  return SGuard{this, qid, static_cast<uint32_t>(cur & kVersionMask)};
}

auto
OMCSLock::LockSIX()  //
    -> SIXGuard
{
  const auto qid = GetQID();
  auto *qnode = &(_qnodes[qid]);
  const auto new_tail = (static_cast<uint64_t>(qid) << kQIDShift) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    if (lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if ((cur & kLockMask) != kNoLocks) {
    // wait until predecessor gives up the lock
    auto *pred_qnode = &(_qnodes[(cur & kQIDMask) >> kQIDShift]);
    pred_qnode->next.store(qnode, kRelaxed);
    while (!qnode->hold_lock.load(kRelaxed)) {
      std::this_thread::yield();
    }
    // disable opportunistic read
    cur = lock_.fetch_xor(kOPReadFlag, kAcquire);
  }

  return SIXGuard{this, qid, static_cast<uint32_t>(cur & kVersionMask)};
}

auto
OMCSLock::LockX()  //
    -> XGuard
{
  const auto qid = GetQID();
  auto *qnode = &(_qnodes[qid]);
  const auto new_tail = (static_cast<uint64_t>(qid) << kQIDShift) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    if (lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if ((cur & kLockMask) != kNoLocks) {
    // wait until predecessor gives up the lock
    auto *pred_qnode = &(_qnodes[(cur & kQIDMask) >> kQIDShift]);
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
OMCSLock::UnlockS(  //
    const uint64_t qid,
    const uint64_t ver)
{
  auto *qnode = &(_qnodes[qid]);

  auto *next_ptr = qnode->next.load(kAcquire);
  if (next_ptr == nullptr) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while ((cur & kQIDMask) == qid) {
      const auto unlock = cur - kSLock;
      if (unlock & kLockMask) {
        if (lock_.compare_exchange_weak(cur, unlock, kRelaxed, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, kNull, kRelaxed, kRelaxed)) {
        _tls.reset(qnode);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next_ptr = qnode->next.load(kRelaxed);
      if (next_ptr) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }
  next_ptr->hold_lock.store(true, kRelaxed);
  RetainQID(qid);
}

void
OMCSLock::UnlockSIX(  //
    const uint64_t qid,
    const uint64_t ver)
{
  auto *qnode = &(_qnodes[qid]);

  auto *next_ptr = qnode->next.load(kAcquire);
  if (next_ptr == nullptr) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid) {
      if (cur & kSMask) {
        if (lock_.compare_exchange_weak(cur, cur ^ kXLock, kRelease, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, ver, kRelease, kRelaxed)) {
        RetainQID(qid);
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
  RetainQID(qid);
}

void
OMCSLock::UnlockX(  //
    const uint64_t qid,
    const uint64_t ver)
{
  auto *qnode = &(_qnodes[qid]);

  auto *next_ptr = qnode->next.load(kAcquire);
  if (next_ptr == nullptr) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid) {
      if (cur & kSMask) {
        if (lock_.compare_exchange_weak(cur, cur ^ kXLock, kRelease, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, ver, kRelease, kRelaxed)) {
        RetainQID(qid);
        return;
      }
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
  RetainQID(qid);
}

/*############################################################################*
 * Exclusive lock guards
 *############################################################################*/

auto
OMCSLock::XGuard::operator=(  //
    XGuard &&rhs) noexcept    //
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

OMCSLock::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX(qid_, new_ver_);
  }
}

/*############################################################################*
 * Optimistic lock guards
 *############################################################################*/

auto
OptimisticLock::OptGuard::operator=(  //
    OptGuard &&rhs) noexcept          //
    -> OptGuard &
{
  if (dest_ && has_lock_) {
    dest_->UnlockS();
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  ver_ = rhs.ver_;
  retry_num_ = rhs.retry_num_;
  has_lock_ = std::exchange(rhs.has_lock_, false);
  return *this;
}

OptimisticLock::OptGuard::~OptGuard()
{
  if (dest_ && has_lock_) {
    dest_->UnlockS();
  }
}

auto
OMCSLock::OptGuard::VerifyVersion(const size_t max_retry) noexcept  //
    -> bool
{
  if (has_lock_) {
    dest_->UnlockS();
    has_lock_ = false;
    return true;
  }

  // verify using the optimistic read procedure
  uint64_t cur{};
  while (true) {
    std::atomic_thread_fence(kRelease);
    cur = dest_->lock_.load(kRelaxed);
    if ((cur & kXAndOPReadMask) != kXLock) break;  // lock released or opportunistically readable
    std::this_thread::yield();
  }

  const auto expected = ver_;
  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  if (((ver_ ^ expected) & mask) == 0) return true;
  if (++retry_num_ < max_retry) return false;  // continue with OCC

  // try to acquire a shared lock
  while ((cur & kSMask) == kNoLocks) {
    has_lock_ = dest_->lock_.compare_exchange_weak(cur, cur | kSLock, kAcquire, kRelaxed);
    if (has_lock_) break;
    CPP_UTILITY_SPINLOCK_HINT
  }
  return false;
}

}  // namespace dbgroup::lock
