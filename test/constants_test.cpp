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

// the corresponding header
#include "dbgroup/constants.hpp"

// C++ standard libraries
#include <bit>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>

// external libraries
#include "gtest/gtest.h"

namespace dbgroup::test
{
/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TEST(  //
    ConstantsTest,
    AlignValOnVirtualPagesIsPowerOfTwoUpToPageSize)
{
  for (const size_t size : {0UL, 1UL, 3UL, 24UL, 64UL, 1'000UL, kVMPageSize - 1, kVMPageSize,
                            kVMPageSize + 1, 2 * kVMPageSize}) {
    const auto align = static_cast<size_t>(GetAlignValOnVirtualPages(size));
    EXPECT_TRUE(std::has_single_bit(align)) << "size=" << size;
    EXPECT_LE(align, kVMPageSize) << "size=" << size;
    if (size <= kVMPageSize) {
      EXPECT_GE(align, size) << "size=" << size;
    }

    // the alignment can be used for aligned allocation
    const auto alloc_size = size == 0 ? 1 : size;
    auto* const ptr = ::operator new(alloc_size, GetAlignValOnVirtualPages(size));
    EXPECT_EQ(std::bit_cast<uintptr_t>(ptr) % align, 0) << "size=" << size;
    ::operator delete(ptr, GetAlignValOnVirtualPages(size));
  }
}

TEST(  //
    ConstantsTest,
    ShiftAddrKeepsConstness)
{
  // the constness of addresses is kept
  static_assert(std::is_same_v<decltype(ShiftAddr(std::declval<void*>(), 1)), void*>);
  static_assert(std::is_same_v<decltype(ShiftAddr(std::declval<const void*>(), 1)), const void*>);
  static_assert(std::is_same_v<decltype(ShiftAddr(std::declval<uint64_t*>(), 1)), void*>);
  static_assert(
      std::is_same_v<decltype(ShiftAddr(std::declval<const uint64_t*>(), 1)), const void*>);

  // addresses are shifted in bytes with signed/unsigned offsets
  uint64_t buf[4]{};
  const auto* const cbuf = buf;
  EXPECT_EQ(ShiftAddr(buf, sizeof(uint64_t)), &buf[1]);
  EXPECT_EQ(ShiftAddr(cbuf, 2 * sizeof(uint64_t)), &cbuf[2]);
  EXPECT_EQ(ShiftAddr(&buf[3], -static_cast<int64_t>(sizeof(uint64_t))), &buf[2]);
  EXPECT_EQ(GetOffsetBetween(&buf[0], &buf[3]), 3 * static_cast<int64_t>(sizeof(uint64_t)));
}

}  // namespace dbgroup::test
