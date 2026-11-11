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

}  // namespace dbgroup::test
