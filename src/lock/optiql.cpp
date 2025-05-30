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
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
namespace
{
/*############################################################################*
 * Local types
 *############################################################################*/

/**
 * @brief A class for representing queue nodes.
 *
 */
struct QNode {
  /// @brief The next queue node if exist.
  std::atomic<QNode *> next;

  /// @brief A flag for indicating this node's owner holds a lock.
  std::atomic_bool hold_lock;
};

/**
 * @brief A class for managing thread local QIDs.
 *
 */
class TLS
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr TLS() = default;

  TLS(const TLS &) = delete;
  TLS(TLS &&) = delete;

  auto operator=(const TLS &) -> TLS & = delete;
  auto operator=(TLS &&) -> TLS & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~TLS()
  {
    for (uint32_t i = 0; i < cnt_; ++i) {
      FreeQID(qid_arr_[i]);
    }
  }

  /*##########################################################################*
   * Public APIs
   *##########################################################################*/

  /**
   * @param qid A target QID.
   * @return The corresponding queue node.
   */
  static constexpr auto
  GetQNode(          //
      uint32_t qid)  //
      -> QNode *
  {
    return &_qnodes[qid];
  }

  /**
   * @retval 1st: A reserved QID.
   * @retval 2nd: The corresponding queue node.
   */
  auto
  GetQID()  //
      -> std::pair<uint32_t, QNode *>
  {
    const auto qid = (cnt_ > 0) ? qid_arr_[--cnt_] : AllocQID();
    return {qid, &_qnodes[qid]};
  }

  /**
   * @brief Retain or release a given QID.
   *
   * @param qid A qid to be retained or released.
   */
  void
  ReleaseQID(  //
      const uint32_t qid)
  {
    auto &qnode = _qnodes[qid];
    qnode.next.store(nullptr, kRelaxed);
    qnode.hold_lock.store(false, kRelaxed);
    if (cnt_ < kMaxTLSNum) {
      qid_arr_[cnt_++] = qid;
    } else {
      FreeQID(qid);
    }
  }

 private:
  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  /// @brief The number of bits in one word.
  static constexpr uint32_t kBitNum = 64;

  /// @brief The maximum number of thread local queue nodes.
  static constexpr uint32_t kMaxTLSNum = 8;

  /// @brief The maximum number of queue nodes.
  static constexpr uint64_t kQNodeNum = 1UL << 16UL;

  /// @brief The size of a buffer for managing queue node IDs.
  static constexpr uint32_t kIDBufSize = kQNodeNum / kBitNum;

  /*##########################################################################*
   * Internal APIs
   *##########################################################################*/

  /**
   * @return A unique QID.
   */
  static auto
  AllocQID()  //
      -> uint32_t
  {
    constexpr uint32_t kIDBufMask = kIDBufSize - 1U;
    constexpr uint64_t kFilledIDs = ~0UL;
    thread_local const auto base = std::hash<std::thread::id>{}(std::this_thread::get_id());
    for (uint32_t pos = base; true; ++pos) {
      pos &= kIDBufMask;
      auto cur = _id_buf[pos].load(kRelaxed);
      while (cur < kFilledIDs) {
        const uint32_t cto = std::countr_one(cur);
        const auto flag = 1UL << cto;
        cur = _id_buf[pos].fetch_or(flag, kAcquire);
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
  static void
  FreeQID(  //
      const uint32_t qid)
  {
    constexpr uint32_t kFlagMask = kBitNum - 1U;
    _id_buf[qid / kBitNum].fetch_xor(1UL << (qid & kFlagMask), kRelease);
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The number of QIDs in a thread local storage.
  uint32_t cnt_{};

  /// @brief Thread local QIDs.
  uint32_t qid_arr_[kMaxTLSNum] = {};

  /*##########################################################################*
   * Static variables
   *##########################################################################*/
  // NOLINTBEGIN

  /// @brief The actual queue nodes.
  static inline QNode _qnodes[kQNodeNum] = {};

  /// @brief The container of queue node IDs.
  alignas(kVMPageSize) static inline std::atomic_uint64_t _id_buf[kIDBufSize] = {};

  // NOLINTEND
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
thread_local TLS _tls{};

// NOLINTEND
}  // namespace

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
OptiQL::GetVersion() const noexcept  //
    -> VerGuard
{
  uint64_t cur{};
  while (true) {
    cur = lock_.load(kAcquire);
    if ((cur & kXAndOPReadMask) != kXLock) break;  // lock released or opportunistically readable
    std::this_thread::yield();
  }

  return VerGuard{this, static_cast<uint32_t>(cur & kVersionMask)};
}

auto
OptiQL::LockX()  //
    -> XGuard
{
  auto [qid, qnode] = _tls.GetQID();
  const auto new_tail = (static_cast<uint64_t>(qid) << kQIDShift) | kXLock;

  auto cur = lock_.load(kRelaxed);
  while (true) {
    if (lock_.compare_exchange_weak(cur, new_tail, kAcquire, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  if ((cur & kLockMask) != kNoLocks) {
    // wait until predecessor gives up the lock
    auto *pred_qnode = TLS::GetQNode((cur & kQIDMask) >> kQIDShift);
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
  auto *qnode = TLS::GetQNode(qid);

  auto *next_ptr = qnode->next.load(kAcquire);
  if (next_ptr == nullptr) {  // this is the tail node
    auto cur = lock_.load(kRelaxed);
    while (((cur & kQIDMask) >> kQIDShift) == qid) {
      if (lock_.compare_exchange_weak(cur, ver, kRelease, kRelaxed)) goto end;
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

end:
  _tls.ReleaseQID(qid);
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
OptiQL::VerGuard::VerifyVersion() noexcept  //
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
