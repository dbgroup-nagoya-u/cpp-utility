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
#include "dbgroup/lock/mcs_lock.hpp"

// C++ standard libraries
#include <atomic>
#include <bitset>
#include <cstdint>
#include <thread>
#include <utility>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
namespace
{
/*############################################################################*
 * Local constants
 *############################################################################*/

/// @brief The `uintptr_t` of nullptr.
constexpr uint64_t kNull = 0UL;

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0UL;

/// @brief A bit shift for QNode.
constexpr uint64_t kQIDShift = 32UL;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << 48UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 63UL;

/// @brief A bit mask for extracting a version value.
constexpr uint64_t kVersionMask = ~(~0UL << kQIDShift);

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kQIDMask = (kSLock - 1UL) ^ kVersionMask;

/// @brief A bit mask for extracting a lock state.
constexpr uint64_t kLockMask = ~(kQIDMask | kVersionMask);

/// @brief A bit mask for extracting a sharedlock state.
constexpr uint64_t kSMask = kLockMask ^ kXLock;

/// @brief The number of bits in one word.
constexpr uint32_t kBitNum = 64;

/// @brief The maximum number of thread local queue nodes.
constexpr uint32_t kMaxTLSNum = 8;

/// @brief The size of a buffer for managing queue node IDs.
constexpr uint32_t kIDBufSize = MCSLock::kQNodeNum / kBitNum;

/*############################################################################*
 * Static variables
 *############################################################################*/
// NOLINTBEGIN

/// @brief The actual queue nodes.
MCSLock::QNode _qnodes[MCSLock::kQNodeNum] = {};

/// @brief The container of queue node IDs.
alignas(kVMPageSize) std::atomic_uint64_t _id_buf[kIDBufSize] = {};

/// @brief A thread local queue node container.
thread_local std::vector<uint32_t> _tls{};

// NOLINTEND

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
  uint32_t qid;
  if (!_tls.empty()) {
    qid = _tls.back();
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
      if ((cur & flag) == 0UL) {
        qid = pos * kBitNum + cto;
        return qid;
      }
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
  qnode->state.store(kNoLocks, kRelaxed);

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
MCSLock::LockS()  //
    -> SGuard
{
  auto qid = GetQID();
  auto *qnode = &(_qnodes[qid]);
  auto this_qid = (static_cast<uint64_t>(qid) << kQIDShift) | kSLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    if (cur & kLockMask) {  // there is the predecessor
      if (lock_.compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed)) break;
    } else if (lock_.compare_exchange_weak(cur, cur | this_qid, kAcquire, kRelaxed)) {
      goto end;  // the initial shared lock
    }
    CPP_UTILITY_SPINLOCK_HINT
  }

  RetainQID(qid);
  this_qid = cur & kQIDMask;
  qid = this_qid >> kQIDShift;
  qnode = &(_qnodes[qid]);
  if (cur & kXLock) {  // wait for the predecessor to release the lock
    while ((cur & kQIDMask) == this_qid && (cur & kXLock)) {
      std::this_thread::yield();
      cur = lock_.load(kAcquire);
    }
    if ((cur & kQIDMask) != this_qid) {
      QNode *next;
      while (true) {  // wait for the successor to fill its pointer
        next = qnode->next.load(kAcquire);
        if (next) break;
        CPP_UTILITY_SPINLOCK_HINT
      }
      while (true) {
        cur = next->state.load(kAcquire);
        if ((cur & kXLock) == kNoLocks) break;
        std::this_thread::yield();
      }
    }
  }
end:
  return SGuard{this, qid, static_cast<uint32_t>(cur)};
}

auto
MCSLock::LockSIX()  //
    -> SIXGuard
{
  const auto qid = GetQID();
  auto *qnode = &(_qnodes[qid]);
  const auto new_tail = (static_cast<uint64_t>(qid) << kQIDShift) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    qnode->state.store(cur & kLockMask, kRelaxed);
    if (lock_.compare_exchange_weak(cur, new_tail, kAcqRel, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if (cur & kLockMask) {  // wait for the predecessor to release the lock
    auto *tail = &(_qnodes[(cur & kQIDMask) >> kQIDShift]);
    tail->next.store(qnode, kRelease);
    while (true) {
      cur = qnode->state.load(kAcquire);
      if ((cur & kXLock) == kNoLocks) break;
      std::this_thread::yield();
    }
  }
  return SIXGuard{this, qid, static_cast<uint32_t>(cur)};
}

auto
MCSLock::LockX()  //
    -> XGuard
{
  const auto qid = GetQID();
  auto *qnode = &(_qnodes[qid]);
  const auto new_tail = (static_cast<uint64_t>(qid) << kQIDShift) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    qnode->state.store(cur & kLockMask, kRelaxed);
    if (lock_.compare_exchange_weak(cur, new_tail, kAcqRel, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if (cur & kLockMask) {  // wait for the predecessor to release the lock
    auto *tail = &(_qnodes[(cur & kQIDMask) >> kQIDShift]);
    tail->next.store(qnode, kRelease);
    while (true) {
      cur = qnode->state.load(kAcquire);
      if ((cur & kLockMask) == kNoLocks) break;
      std::this_thread::yield();
    }
  }
  return XGuard{this, qid, static_cast<uint32_t>(cur)};
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

void
MCSLock::UnlockS(  //
    const uint32_t qid)
{
  auto *qnode = &(_qnodes[qid]);
  auto *next = qnode->next.load(kAcquire);
  if (!next) {  // this is the tail node
    const auto this_qid = static_cast<uint64_t>(qid) << kQIDShift;
    auto cur = lock_.load(kRelaxed);
    while ((cur & kQIDMask) == this_qid) {
      const auto unlock = cur - kSLock;
      if (unlock & kLockMask) {
        if (lock_.compare_exchange_weak(cur, unlock, kRelaxed, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, unlock & kVersionMask, kRelaxed, kRelaxed)) {
        RetainQID(qid);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next = qnode->next.load(kAcquire);
      if (next) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  if ((next->state.fetch_sub(kSLock, kRelease) & kLockMask) == kSLock) {
    RetainQID(qid);
  }
}

void
MCSLock::UnlockSIX(  //
    const uint32_t qid,
    const uint64_t ver)
{
  auto *qnode = &(_qnodes[qid]);
  while (qnode->state.load(kRelaxed) & kSMask) {
    std::this_thread::yield();  // wait for shared lock holders to release their locks
  }

  auto *next = qnode->next.load(kAcquire);
  if (!next) {  // this is the tail node
    const auto this_qid = static_cast<uint64_t>(qid) << kQIDShift;
    auto cur = lock_.load(kRelaxed);
    while ((cur & kQIDMask) == this_qid) {
      if (cur & kSMask) {
        if (lock_.compare_exchange_weak(cur, (cur ^ kXLock) | ver, kRelease, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, ver, kRelease, kRelaxed)) {
        RetainQID(qid);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next = qnode->next.load(kAcquire);
      if (next) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  if ((next->state.fetch_sub(kXLock - ver, kRelease) & kLockMask) == kXLock) {
    RetainQID(qid);
  }
}

void
MCSLock::UnlockX(  //
    const uint32_t qid,
    const uint64_t ver)
{
  auto *qnode = &(_qnodes[qid]);
  auto *next = qnode->next.load(kAcquire);
  if (!next) {  // this is the tail node
    const auto this_qid = static_cast<uint64_t>(qid) << kQIDShift;
    auto cur = lock_.load(kRelaxed);
    while ((cur & kQIDMask) == this_qid) {
      if (cur & kSMask) {
        if (lock_.compare_exchange_weak(cur, (cur ^ kXLock) | ver, kRelease, kRelaxed)) return;
      } else if (lock_.compare_exchange_weak(cur, ver, kRelease, kRelaxed)) {
        RetainQID(qid);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next = qnode->next.load(kAcquire);
      if (next) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  if ((next->state.fetch_sub(kXLock - ver, kRelease) & kLockMask) == kXLock) {
    RetainQID(qid);
  }
}

/*############################################################################*
 * Shared lock guards
 *############################################################################*/

auto
MCSLock::SGuard::operator=(  //
    SGuard &&rhs) noexcept   //
    -> SGuard &
{
  if (dest_) {
    dest_->UnlockS(qid_);
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  qid_ = rhs.qid_;
  ver_ = rhs.ver_;
  return *this;
}

MCSLock::SGuard::~SGuard()
{
  if (dest_) {
    dest_->UnlockS(qid_);
  }
}

/*############################################################################*
 * Shared-with-intent-exclusive lock guards
 *############################################################################*/

auto
MCSLock::SIXGuard::operator=(  //
    SIXGuard &&rhs) noexcept   //
    -> SIXGuard &
{
  if (dest_) {
    dest_->UnlockSIX(qid_, ver_);
  }
  dest_ = std::exchange(rhs.dest_, nullptr);
  qid_ = rhs.qid_;
  ver_ = rhs.ver_;
  return *this;
}

MCSLock::SIXGuard::~SIXGuard()
{
  if (dest_) {
    dest_->UnlockSIX(qid_, ver_);
  }
}

auto
MCSLock::SIXGuard::UpgradeToX()  //
    -> XGuard
{
  if (dest_ == nullptr) return XGuard{};

  auto *qnode = &(_qnodes[qid_]);
  while (qnode->state.load(kRelaxed) & kSMask) {
    std::this_thread::yield();  // wait for shared lock holders to release their locks
  }
  return XGuard{std::exchange(dest_, nullptr), qid_, ver_};
}

/*############################################################################*
 * Exclusive lock guards
 *############################################################################*/

auto
MCSLock::XGuard::operator=(  //
    XGuard &&rhs) noexcept   //
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

MCSLock::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX(qid_, new_ver_);
  }
}

}  // namespace dbgroup::lock
