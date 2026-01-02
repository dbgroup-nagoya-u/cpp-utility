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
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <new>

namespace dbgroup
{
/*############################################################################*
 * Global constants
 *############################################################################*/

/// @brief K (10^3)
constexpr size_t kK = 1E3;

/// @brief M (10^6)
constexpr size_t kM = 1E6;

/// @brief G (10^9)
constexpr size_t kG = 1E9;

/// @brief T (10^12)
constexpr size_t kT = 1E12;

/// @brief P (10^15)
constexpr size_t kP = 1E15;

/// @brief E (10^18)
constexpr size_t kE = 1E18;

/// @brief Ki (2^10)
constexpr size_t kKi = 1UL << 10UL;

/// @brief Mi (2^20)
constexpr size_t kMi = 1UL << 20UL;

/// @brief Gi (2^30)
constexpr size_t kGi = 1UL << 30UL;

/// @brief Ti (2^40)
constexpr size_t kTi = 1UL << 40UL;

/// @brief Pi (2^50)
constexpr size_t kPi = 1UL << 50UL;

/// @brief Ei (2^60)
constexpr size_t kEi = 1UL << 60UL;

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
constexpr size_t k1Ki = kKi;

/// @brief 2,048
constexpr size_t k2Ki = 2 * kKi;

/// @brief 4,096
constexpr size_t k4Ki = 4 * kKi;

/// @brief 8,192
constexpr size_t k8Ki = 8 * kKi;

/// @brief 16,384
constexpr size_t k16Ki = 16 * kKi;

/// @brief 32,768
constexpr size_t k32Ki = 32 * kKi;

/// @brief 65,536
constexpr size_t k64Ki = 64 * kKi;

/// @brief 131,072
constexpr size_t k128Ki = 128 * kKi;

/// @brief 262,144
constexpr size_t k256Ki = 256 * kKi;

/// @brief 524,288
constexpr size_t k512Ki = 512 * kKi;

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

/**
 * @param addr A target address or size.
 * @return The aligned address on virtual pages.
 * @note The addresss may be rounded down for alignment.
 */
constexpr auto
FloorOnVirtualPages(               //
    const uint64_t addr) noexcept  //
    -> uint64_t
{
  return addr & ~(kVMPageSize - 1UL);
}

/**
 * @param addr A target address or size.
 * @return The aligned address on virtual pages.
 * @note The addresss may be rounded up for alignment.
 */
constexpr auto
CeilOnVirtualPages(                //
    const uint64_t addr) noexcept  //
    -> uint64_t
{
  constexpr auto kFillBits = kVMPageSize - 1UL;
  return (addr + kFillBits) & ~kFillBits;
}

/**
 * @brief Shift a memory address by a byte offset.
 *
 * @tparam T An integral type.
 * @param addr An original address.
 * @param offset An offset in bytes.
 * @return A shifted address.
 */
template <std::integral T>
constexpr auto
ShiftAddr(  //
    const void *addr,
    const T offset) noexcept  //
    -> void *
{
  return std::bit_cast<std::byte *>(addr) + offset;
}

/**
 * @param base A base address.
 * @param rel A destination address.
 * @return The offset between `base` and `rel` (`rel` - `base`).
 */
constexpr auto
GetOffsetBetween(  //
    const void *base,
    const void *rel) noexcept  //
    -> int64_t
{
  return std::bit_cast<std::int64_t>(rel) - std::bit_cast<std::int64_t>(base);
}

}  // namespace dbgroup

#endif  // CPP_UTILITY_DBGROUP_CONSTANTS_HPP_
