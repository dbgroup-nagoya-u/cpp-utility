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
#include <new>

namespace dbgroup
{
/*############################################################################*
 * Global constants
 *############################################################################*/

/// @brief 2
constexpr size_t k2 = 1UL << 1UL;

/// @brief 4
constexpr size_t k4 = 1UL << 2UL;

/// @brief 8
constexpr size_t k8 = 1UL << 3UL;

/// @brief 16
constexpr size_t k16 = 1UL << 4UL;

/// @brief 32
constexpr size_t k32 = 1UL << 5UL;

/// @brief 64
constexpr size_t k64 = 1UL << 6UL;

/// @brief 128
constexpr size_t k128 = 1UL << 7UL;

/// @brief 256
constexpr size_t k256 = 1UL << 8UL;

/// @brief 512
constexpr size_t k512 = 1UL << 9UL;

/// @brief 1,024
constexpr size_t k1Ki = 1UL << 10UL;

/// @brief 2,048
constexpr size_t k2Ki = 1UL << 11UL;

/// @brief 4,096
constexpr size_t k4Ki = 1UL << 12UL;

/// @brief 8,192
constexpr size_t k8Ki = 1UL << 13UL;

/// @brief 16,384
constexpr size_t k16Ki = 1UL << 14UL;

/// @brief 32,768
constexpr size_t k32Ki = 1UL << 15UL;

/// @brief 65,536
constexpr size_t k64Ki = 1UL << 16UL;

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

/// @brief The expected virtual page size.
constexpr size_t kVMPageSize = static_cast<size_t>(DBGROUP_VIRTUAL_PAGE_SIZE);

/// @brief The number of logical cores.
constexpr size_t kLogicalCoreNum = static_cast<size_t>(DBGROUP_LOGICAL_CORE_NUM);

/// @brief The capacity of threads used in a process.
constexpr size_t kMaxThreadCapacity{kVMPageSize};

/*############################################################################*
 * Global utilities for constants
 *############################################################################*/

/**
 * @param size A target page size.
 * @return The alignment size that fits in virtual pages.
 */
constexpr auto
GetAlignValOnVirtualPages(       //
    const size_t size) noexcept  //
    -> std::align_val_t
{
  return static_cast<std::align_val_t>(size < kVMPageSize ? size : kVMPageSize);
}

}  // namespace dbgroup

#endif  // CPP_UTILITY_DBGROUP_CONSTANTS_HPP_
