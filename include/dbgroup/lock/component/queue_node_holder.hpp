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

#ifndef CPP_UTILITY_DBGROUP_LOCK_COMPONENT_QUEUE_NODE_HOLDER_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_COMPONENT_QUEUE_NODE_HOLDER_HPP_

// C++ standard libraries
#include <atomic>
#include <bit>
#include <cstdint>
#include <functional>
#include <thread>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock::component
{
/**
 * @brief A class for managing thread local QIDs.
 *
 */
template <class QNode, size_t kMaxQueueNodeNum = k64Ki>
class QueueNodeHolder
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr QueueNodeHolder() noexcept = default;

  // forbid any copying/moving
  QueueNodeHolder(const QueueNodeHolder &) = delete;
  QueueNodeHolder(QueueNodeHolder &&) = delete;
  auto operator=(const QueueNodeHolder &) -> QueueNodeHolder & = delete;
  auto operator=(QueueNodeHolder &&) -> QueueNodeHolder & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~QueueNodeHolder()
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
  GetQNode(                   //
      uint32_t qid) noexcept  //
      -> QNode *
  {
    return &qnodes_[qid];
  }

  /**
   * @retval 1st: A reserved QID.
   * @retval 2nd: The corresponding queue node.
   */
  auto
  GetQID() noexcept  //
      -> uint32_t
  {
    return (cnt_ > 0) ? qid_arr_[--cnt_] : AllocQID();
  }

  /**
   * @brief Retain or release a given QID.
   *
   * @param qid A qid to be retained or released.
   */
  void
  ReleaseQID(  //
      const uint32_t qid) noexcept
  {
    qnodes_[qid].next.store(nullptr, kRelaxed);
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

  /// @brief The size of a buffer for managing queue node IDs.
  static constexpr uint32_t kIDBufSize = kMaxQueueNodeNum / kBitNum;

  /*##########################################################################*
   * Internal APIs
   *##########################################################################*/

  /**
   * @return A unique QID.
   */
  static auto
  AllocQID() noexcept  //
      -> uint32_t
  {
    constexpr uint32_t kIDBufMask = kIDBufSize - 1U;
    constexpr uint64_t kFilledIDs = ~0UL;
    thread_local const auto base = std::hash<std::thread::id>{}(std::this_thread::get_id());
    for (uint32_t pos = base; true; ++pos) {
      pos &= kIDBufMask;
      auto cur = id_buf_[pos].load(kRelaxed);
      while (cur < kFilledIDs) {
        const uint32_t cto = std::countr_one(cur);
        const auto flag = 1UL << cto;
        cur = id_buf_[pos].fetch_or(flag, kAcquire);
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
      const uint32_t qid) noexcept
  {
    constexpr uint32_t kFlagMask = kBitNum - 1U;
    id_buf_[qid / kBitNum].fetch_xor(1UL << (qid & kFlagMask), kRelease);
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
  static inline QNode qnodes_[kMaxQueueNodeNum] = {};

  /// @brief The container of queue node IDs.
  alignas(kVMPageSize) static inline std::atomic_uint64_t id_buf_[kIDBufSize] = {};

  // NOLINTEND
};

}  // namespace dbgroup::lock::component

#endif  // CPP_UTILITY_DBGROUP_LOCK_COMPONENT_QUEUE_NODE_HOLDER_HPP_
