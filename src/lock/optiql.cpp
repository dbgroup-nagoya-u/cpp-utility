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
#include "dbgroup/lock/common.hpp"

namespace
{
/*##############################################################################
 * Local types
 *############################################################################*/

struct QNode {
  std::atomic_uint64_t ver;

  std::atomic<QNode *> next;
};

/*##############################################################################
 * Constant aliases
 *############################################################################*/

constexpr auto kVMPageSize = ::dbgroup::lock::kVMPageSize;
constexpr auto kQNodeNum = ::dbgroup::lock::OptiQL::kQNodeNum;
constexpr auto kRelaxed = ::dbgroup::lock::kRelaxed;

/*##############################################################################
 * Local constants
 *############################################################################*/

/// @brief The `uintptr_t` of nullptr.
constexpr uint64_t kNull = 0;

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << 48UL;

/// @brief A lock state representing a shared-with-intent-exclusive lock.
constexpr uint64_t kSIXLock = 1UL << 62UL;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << 63UL;

/// @brief A lock state representing an opportunistic lock.
constexpr uint64_t kOPReadLock = 1UL << 61UL;

/// @brief A bit mask for extracting a version value.
constexpr uint64_t kVersionMask = ~(~0UL << 32UL);

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kQIDMask = (kSLock - 1UL) ^ kVersionMask;

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kQIDShift = 32UL;

/// @brief A bit mask for extracting an X-lock state and version values.
constexpr uint64_t kXAndVersionMask = kVersionMask | kXLock;

/// @brief A bit mask for extracting an X-lock state and version values.
constexpr uint64_t kLockMask = ~(kVersionMask | kQIDMask);

/// @brief The number of bits in one word.
constexpr uint64_t kBitNum = 64UL;

/// @brief The maximum number of thread local queue nodes.
constexpr uint32_t kMaxTLSNum = 8;

/// @brief The size of a buffer for managing queue node IDs.
constexpr uint64_t kIDBufSize = kQNodeNum / kBitNum;

/// @brief The version update number
constexpr uint64_t kVersionUpdate = 1;

/*##############################################################################
 * Static variables
 *############################################################################*/

// The definition of a static member.
QNode _qnodes[kQNodeNum] = {};  // NOLINT

/// @brief The container of queue node IDs.
alignas(kVMPageSize) std::atomic_uint64_t _id_buf[kIDBufSize] = {};  // NOLINT

/// @brief A thread local queue node container.
thread_local std::vector<uint32_t> _tls{};  // NOLINT

/*##############################################################################
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
  if (_tls.size() < kMaxTLSNum) {
    _tls.emplace_back(qid);
  } else {
    constexpr uint32_t kFlagMask = kBitNum - 1U;
    _id_buf[qid / kBitNum].fetch_xor(qid & kFlagMask, kRelaxed);
  }
}

}  // namespace

namespace dbgroup::lock
{
/*##############################################################################
 * Public APIs
 *############################################################################*/

auto
OptiQL::GetVersion()  //
    -> OptGuard
{
  uint64_t cur{};
  SpinWithBackoff(
      [](const std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        *cur = lock->load(kAcquire);
        return (*cur & kXLock) == kNoLocks;
      },
      &lock_, &cur);

  return OptGuard{this, static_cast<uint32_t>(cur & kVersionMask)};
}

auto
OptiQL::LockX()  //
    -> XGuard
{
  auto qid = GetQID();
  auto *qnode = &(_qnodes[qid]);
  const auto new_tail = (qid << kQIDShift) | kXLock;

  auto cur = lock_.load(kRelaxed);
  auto pred_qid = (cur & kQIDMask) >> kQIDShift;
  auto *pred_qnode = &(_qnodes[pred_qid]);
  while (true) {
    qnode->ver.store(cur & kLockMask, kRelaxed);
    if (lock_.compare_exchange_weak(cur, new_tail, kAcqRel, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if ((cur & kLockMask) == kNoLocks) {  // wait until predecessor gives up the lock
    qnode->ver = (cur & kVersionMask) + kVersionUpdate;
  } else {
    pred_qnode->next = qnode;
    // ここの構造要チェック
    while (qnode->ver.load(kAcquire) & kLockMask) {
      CPP_UTILITY_SPINLOCK_HINT
      std::this_thread::yield();
    }
    lock_.fetch_and(~(kOPReadLock | kVersionMask));
  }
  // throw std::runtime_error{"not implemented yet"};
end:
  return XGuard{this, qid, static_cast<uint32_t>(cur & kVersionMask)};
}

/*##############################################################################
 * Internal APIs
 *############################################################################*/

void
OptiQL::UnlockX(  //
    const uint64_t qid)
{
  auto *qnode = &(_qnodes[qid]);

  auto *next_ptr = qnode->next.load(kRelaxed);
  auto cur = lock_.load(kRelaxed);
  if (next_ptr == nullptr && lock_.compare_exchange_weak(cur, qnode->ver, kRelease, kRelaxed)) {
    // Do CAS IF
  } else {
    // enable opportunistic read
    lock_.fetch_or(kOPReadLock | qnode->ver);
    while (true) {  // wait until successor fills in its next field
      auto *next_ptr = qnode->next.load(kRelaxed);
      if (next_ptr) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    next_ptr->ver = qnode->ver + kVersionUpdate;
    if ((next_ptr->ver.fetch_sub(kSLock, kRelease)) == kNoLocks) {
    }
  }

  // throw std::runtime_error{"not implemented yet"};
}

/*##############################################################################
 * Exclusive lock guards
 *############################################################################*/

auto
OptiQL::XGuard::operator=(  //
    XGuard &&rhs) noexcept  //
    -> XGuard &
{
  if (dest_) {
    dest_->UnlockX(new_ver_);
  }
  dest_ = rhs.dest_;
  qid_ = rhs.qid_;
  old_ver_ = rhs.old_ver_;
  new_ver_ = rhs.new_ver_;
  rhs.dest_ = nullptr;
  return *this;
}

OptiQL::XGuard::~XGuard()
{
  if (dest_) {
    dest_->UnlockX(new_ver_);
  }
}

/*##############################################################################
 * Optimistic lock guards
 *############################################################################*/

auto
OptiQL::OptGuard::VerifyVersion()  //
    -> bool
{
  uint64_t cur{};
  SpinWithBackoff(
      [](const std::atomic_uint64_t *lock, uint64_t *cur) -> bool {
        std::atomic_thread_fence(kRelease);
        *cur = lock->load(kRelaxed);
        return (*cur & kXLock) == kNoLocks;
      },
      &(dest_->lock_), &cur);

  auto expected = ver_;
  ver_ = static_cast<uint32_t>(cur & kVersionMask);
  return ver_ == expected;
}

}  // namespace dbgroup::lock
