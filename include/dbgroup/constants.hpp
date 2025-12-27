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

#ifndef CPP_UTILITY_DBGROUP_CONSTANTS_HPP_
#define CPP_UTILITY_DBGROUP_CONSTANTS_HPP_

// C++ standard libraries
#include <atomic>
#include <cstddef>

namespace dbgroup
{
/*############################################################################*
 * Global constants
 *############################################################################*/

/// @brief An alias of the sequential-consistency memory order.
constexpr std::memory_order kSeqCst = std::memory_order_seq_cst;

/// @brief An alias of the acquire&release memory order.
constexpr std::memory_order kAcqRel = std::memory_order_acq_rel;

/// @brief An alias of the acquire memory order.
constexpr std::memory_order kAcquire = std::memory_order_acquire;

/// @brief An alias of the release memory order.
constexpr std::memory_order kRelease = std::memory_order_release;

/// @brief An alias of the relaxed memory order.
constexpr std::memory_order kRelaxed = std::memory_order_relaxed;

/*############################################################################*
 * Global tuning parameters
 *############################################################################*/

/// @brief The size of words.
constexpr size_t kWordSize = static_cast<size_t>(DBGROUP_WORD_SIZE);

/// @brief The expected cache-line size.
constexpr size_t kCacheLineSize = static_cast<size_t>(DBGROUP_CACHE_LINE_SIZE);

/// @brief The expected cache-line size.
constexpr size_t kVMPageSize = static_cast<size_t>(DBGROUP_VIRTUAL_PAGE_SIZE);

/// @brief The number of logical cores.
constexpr size_t kLogicalCoreNum = static_cast<size_t>(DBGROUP_LOGICAL_CORE_NUM);

/// @brief The capacity of threads used in a process.
constexpr size_t kMaxThreadCapacity{kVMPageSize};

}  // namespace dbgroup

#endif  // CPP_UTILITY_DBGROUP_CONSTANTS_HPP_
