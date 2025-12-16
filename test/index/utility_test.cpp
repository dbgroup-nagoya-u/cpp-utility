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

#include "dbgroup/index/utility.hpp"

// C++ standard libraries
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <type_traits>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// library sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/optimistic_lock.hpp"

// local sources
#include "common.hpp"

namespace dbgroup::index::test
{
/*############################################################################*
 * Global constants
 *############################################################################*/

constexpr size_t kSize = 1E5;

/*############################################################################*
 * Fixture definitions
 *############################################################################*/

template <class T>
class BinaryConverterFixture : public ::testing::Test
{
 protected:
  /*##########################################################################*
   * Types
   *##########################################################################*/

  using Dist = std::conditional_t<std::is_integral_v<T>,
                                  std::uniform_int_distribution<T>,
                                  std::uniform_real_distribution<T>>;

  /*##########################################################################*
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    constexpr auto kRange = 1E8;
    constexpr T kMin = std::is_unsigned_v<T> ? 0 : -(kRange / 2);
    constexpr T kMax = std::is_unsigned_v<T> ? kRange : (kRange / 2);

    std::mt19937_64 rand_engine{kRandomSeed};  // NOLINT
    Dist dist{kMin, kMax};
    values_.reserve(kSize);
    for (size_t i = 0; i < kSize; ++i) {
      values_.emplace_back(dist(rand_engine));
    }
    std::sort(values_.begin(), values_.end());
    values_.erase(std::unique(values_.begin(), values_.end()), values_.end());
  }

  void
  TearDown() override
  {
  }

  /*##########################################################################*
   * Functions for verification
   *##########################################################################*/

  void
  VerifyMemCmp()
  {
    constexpr size_t kLen = sizeof(T);
    uint8_t lhs[kLen];
    uint8_t rhs[kLen];

    const auto size = values_.size() - 1;
    for (size_t i = 0; i < size; ++i) {
      std::memcpy(&lhs, ConvertToBinaryData(values_[i]), kLen);
      std::memcpy(&rhs, ConvertToBinaryData(values_[i + 1]), kLen);
      ASSERT_LT(std::memcmp(&lhs[0], &rhs[0], kLen), 0);
    }
  }

  void
  VerifyCmpAsUInt64()
  {
    const auto size = values_.size() - 1;
    for (size_t i = 0; i < size; ++i) {
      const auto lhs = ByteSwap(*ConvertToBinaryData<T, uint64_t>(values_[i]));
      const auto rhs = ByteSwap(*ConvertToBinaryData<T, uint64_t>(values_[i + 1]));
      ASSERT_LT(lhs, rhs);
    }
  }

  void
  VerifyRestore()
  {
    for (const auto &v : values_) {
      const auto *bin_data = ConvertToBinaryData(v);
      const auto restored = ConvertFromBinaryData<T>(bin_data);
      ASSERT_EQ(v, restored);
    }
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  std::vector<T> values_{};
};

/*############################################################################*
 * Preparation for typed testing
 *############################################################################*/

using Targets = ::testing::Types<uint64_t, uint32_t, int64_t, int32_t, float, double>;
TYPED_TEST_SUITE(BinaryConverterFixture, Targets);

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TYPED_TEST(  //
    BinaryConverterFixture,
    ConvertedDataAreComparableUsingMemCmp)
{
  TestFixture::VerifyMemCmp();
}

TYPED_TEST(  //
    BinaryConverterFixture,
    ConvertedDataAreComparableAsUInt64)
{
  TestFixture::VerifyCmpAsUInt64();
}

TYPED_TEST(  //
    BinaryConverterFixture,
    RestoredValuesAreEquivalentWithSrcValues)
{
  TestFixture::VerifyRestore();
}

TEST(  //
    VerIncrementTest,
    IncrementVersionBasedOnGivenMask)
{
  lock::OptimisticLock lock{};
  {
    auto &&grd = lock.LockX();
    VerIncrement<kInsDelMask>(grd);
  }
  auto &&grd = lock.GetVersion();
  EXPECT_EQ(grd.GetVersion() + kInsDelMask, 0);
}

}  // namespace dbgroup::index::test
