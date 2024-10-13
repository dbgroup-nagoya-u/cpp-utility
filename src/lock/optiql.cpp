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

/**
 * @brief A class for representing a queue node container.
 *
 */
struct alignas(::dbgroup::lock::kVMPageSize) QNodeBuffer {
  ::dbgroup::lock::OptiQL buf[::dbgroup::lock::OptiQL::kQNodeNum];
};

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

/// @brief A bit mask for extracting a version value.
constexpr uint64_t kVersionMask = ~(~0UL << 32UL);

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kQIDMask = (kSLock - 1UL) ^ kVersionMask;

/// @brief A bit mask for extracting an X-lock state and version values.
constexpr uint64_t kXAndVersionMask = kVersionMask | kXLock;

/// @brief The number of bits in one word.
constexpr uint64_t kBitNum = 64UL;

/// @brief Reserve eight IDs by one bit.
constexpr uint64_t kQIDUnit = 8UL;

/// @brief The size of a buffer for managing queue IDs.
constexpr uint64_t kIDBufSize = ::dbgroup::lock::OptiQL::kQNodeNum / kBitNum / kQIDUnit;

/*##############################################################################
 * Static variables
 *############################################################################*/

/// @brief The container of queue node IDs.
alignas(::dbgroup::lock::kCacheLineSize) std::atomic_uint64_t _id_buf[kIDBufSize] = {};  // NOLINT

/// @brief The container of queue nodes.
std::unique_ptr<QNodeBuffer> _qnode = std::make_unique<QNodeBuffer>();  // NOLINT

/// @brief A thread local queue node container.
thread_local std::vector<std::pair<uint64_t, uint64_t>> _tls{};  // NOLINT

/*##############################################################################
 * Local utilities
 *############################################################################*/

/**
 * @return A base QID for representing reserved eight QIDs.
 * @note Reserved IDs: {id, id + 64, ..., id + 7*64}
 */
auto
ReserveQID()  //
    -> uint64_t
{
  constexpr uint64_t kIDBufMask = kIDBufSize - 1UL;
  constexpr uint64_t kFilledIDs = ~0UL;

  thread_local const auto base = std::hash<std::thread::id>{}(std::this_thread::get_id());
  for (uint64_t pos = base; true; ++pos) {
    pos &= kIDBufMask;
    auto cur = _id_buf[pos].load(::dbgroup::lock::kRelaxed);
    while (cur < kFilledIDs) {
      const auto ctz = cur == 0UL ? 0UL : static_cast<uint64_t>(std::countr_zero(cur));
      const auto flag = 1UL << ctz;
      cur = _id_buf[pos].fetch_or(flag, ::dbgroup::lock::kRelaxed);
      if ((cur & flag) == 0) return pos * (kBitNum * kQIDUnit) + ctz;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }
}

/**
 * @brief Release the ownership of reserved QIDs.
 *
 * @param qid A base QID.
 */
void
RestoreQIDs(  //
    const uint64_t qid)
{
  constexpr uint64_t kCTZMask = kBitNum - 1UL;

  _id_buf[qid & ~kCTZMask].fetch_xor(1UL << (qid & kCTZMask), ::dbgroup::lock::kRelaxed);
}

/**
 * @return A unique QID.
 */
auto
GetQID()  //
    -> uint64_t
{
  constexpr uint64_t kLSB = 1UL << kQIDUnit;
  constexpr uint64_t kFilled = kLSB - 1UL;

  for (auto &[base_qid, state] : _tls) {
    if (state == kFilled) continue;
    const auto ctz = static_cast<uint64_t>(std::countr_zero(state | kLSB));
    state |= 1UL << ctz;
    return base_qid + ctz * kBitNum;
  }

  // all the reserved QIDs are used, so get new IDs
  const auto qid = ReserveQID();
  _tls.emplace_back(qid, 1UL);
  return qid;
}

/**
 * @brief Retain a QID for reusing in the future.
 *
 * @param qid A QID to be reused.
 */
void
RetainQID(  //
    const uint64_t qid)
{
  constexpr uint64_t kBaseQIDMask = ~((kQIDUnit - 1UL) << (kBitNum / 8UL));
  constexpr uint64_t kCTZMask = (1UL << kQIDUnit) - 1UL;

  size_t cnt = 0;
  for (auto &[base_qid, state] : _tls) {
    cnt += std::popcount(state);
    if ((qid & kBaseQIDMask) != base_qid) continue;
    state ^= 1UL << (qid & kCTZMask);
  }

  if (_tls.size() > 1UL && cnt > kQIDUnit && _tls.back().second == 0UL) {
    RestoreQIDs(_tls.back().first);
    _tls.pop_back();
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
  auto *qnode = &(_qnode->buf[qid]);

  throw std::runtime_error{"not implemented yet"};

  uint64_t cur{};
  return XGuard{this, qid, static_cast<uint32_t>(cur & kVersionMask)};
}

/*##############################################################################
 * Internal APIs
 *############################################################################*/

void
OptiQL::UnlockX(  //
    const uint64_t qid)
{
  auto *qnode = &(_qnode->buf[qid]);

  throw std::runtime_error{"not implemented yet"};
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
