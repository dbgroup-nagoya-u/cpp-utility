/*
 * Copyright 2021 Database Group, Nagoya University
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

#include "random/zipf.hpp"

#include <algorithm>
#include <cmath>
#include <thread>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::random::test
{

template <class IntType>
class ZipfDistributionFixture : public ::testing::Test
{
  using ZipfDist_t = ZipfDistribution<IntType>;

 protected:
  /*####################################################################################
   * Public constants
   *##################################################################################*/

  static constexpr IntType kSmallBinNum = 1000;
  static constexpr size_t kSkew = 1.0;
  static constexpr size_t kRandomSeed = 20;
  static constexpr IntType kMin = std::numeric_limits<IntType>::min();
  static constexpr IntType kMax = std::numeric_limits<IntType>::max() - kSmallBinNum;

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyConstructorWOArgs()
  {
    std::mt19937_64 rand_engine{kRandomSeed};  // NOLINT

    ZipfDist_t zipf_dist{};
    for (size_t i = 0; i < kRepeatNum; ++i) {
      EXPECT_EQ(zipf_dist(rand_engine), 0);
    }
  }

  void
  VerifyConstructorWithArgs()
  {
    std::mt19937_64 rand_engine{kRandomSeed};  // NOLINT
    std::uniform_int_distribution<IntType> uniform_dist{kMin, kMax};

    constexpr size_t kMaxAlphaUL = 20;
    for (size_t i = 0; i < kMaxAlphaUL; ++i) {
      const auto min = uniform_dist(rand_engine);
      const auto max = min + kSmallBinNum;
      const auto alpha = i / static_cast<double>(kMaxAlphaUL);
      const ZipfDist_t zipf_dist{min, max, alpha};

      const auto generated_ids = RunZipfEngine(zipf_dist, min, max, rand_engine());
      CheckGeneratedIDsObeyZipfLaw(generated_ids, min, alpha);
    }
  }

  void
  VerifyCopyInitializers()
  {
    std::mt19937_64 rand_engine{kRandomSeed};  // NOLINT
    std::uniform_int_distribution<IntType> uniform_dist{kMin, kMax};

    // prepare a base distribution
    const auto min = uniform_dist(rand_engine);
    const auto max = min + kSmallBinNum;
    ZipfDist_t orig_engine{min, max, kSkew};
    const auto orig = RunZipfEngine(orig_engine, min, max, kRandomSeed);

    // copy the instance by using constructors
    ZipfDist_t copied_engine{orig_engine};  // NOLINT
    auto copied = RunZipfEngine(copied_engine, min, max, kRandomSeed);
    EXPECT_TRUE(std::equal(orig.begin(), orig.end(), copied.begin(), copied.end()));

    // copy the instance by using operators
    copied_engine = orig_engine;
    copied = RunZipfEngine(copied_engine, min, max, kRandomSeed);
    EXPECT_TRUE(std::equal(orig.begin(), orig.end(), copied.begin(), copied.end()));
  }

  void
  VerifyMoveInitializers()
  {
    std::mt19937_64 rand_engine{kRandomSeed};  // NOLINT
    std::uniform_int_distribution<IntType> uniform_dist{kMin, kMax};

    // prepare a base distribution
    const auto min = uniform_dist(rand_engine);
    const auto max = min + kSmallBinNum;
    ZipfDist_t orig_engine{min, max, kSkew};
    const auto orig = RunZipfEngine(orig_engine, min, max, kRandomSeed);

    // move the instance by using constructors
    ZipfDist_t moved_engine{std::move(orig_engine)};  // NOLINT
    auto copied = RunZipfEngine(moved_engine, min, max, kRandomSeed);
    EXPECT_TRUE(std::equal(orig.begin(), orig.end(), copied.begin(), copied.end()));

    // move the instance by using operators
    auto re_moved_engine = std::move(moved_engine);
    copied = RunZipfEngine(re_moved_engine, min, max, kRandomSeed);
    EXPECT_TRUE(std::equal(orig.begin(), orig.end(), copied.begin(), copied.end()));
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  [[nodiscard]] static auto
  RunZipfEngine(  //
      const ZipfDist_t &zipf_dist,
      const IntType min,
      const IntType max,
      const size_t seed)  //
      -> std::vector<IntType>
  {
    std::mt19937_64 rand_engine{seed};

    std::vector<IntType> generated_ids;
    generated_ids.reserve(kRepeatNum);
    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto id = zipf_dist(rand_engine);
      generated_ids.emplace_back(id);

      EXPECT_GE(id, min);
      EXPECT_LT(id, max);
    }

    return generated_ids;
  }

  static void
  CheckGeneratedIDsObeyZipfLaw(  //
      const std::vector<IntType> &generated_ids,
      const IntType min,
      const double alpha)
  {
    // count ID frequency
    std::vector<size_t> freq_dist(kSmallBinNum, 0);
    for (auto &&id : generated_ids) {
      ++freq_dist[id - min];
    }

    // check ID frequency approximately equals to Zipf's law
    const auto base_prob = static_cast<double>(freq_dist[0]) / kRepeatNum;
    for (size_t k = 2; k <= kSmallBinNum; ++k) {
      const auto kth_prob = static_cast<double>(freq_dist[k - 1]) / kRepeatNum;
      const auto error = abs(kth_prob - base_prob / pow(k, alpha));

      EXPECT_LT(error, kAllowableError);
    }
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kRepeatNum = 1e6;
  static constexpr double kAllowableError = 0.01;
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using IntegralTypes = ::testing::Types<int32_t, int64_t, uint32_t, uint64_t>;
TYPED_TEST_SUITE(ZipfDistributionFixture, IntegralTypes);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------*/

TYPED_TEST(ZipfDistributionFixture, ConstructWithoutArgsAlwaysGenerateZero)
{
  TestFixture::VerifyConstructorWOArgs();
}

TYPED_TEST(ZipfDistributionFixture, ConstructWithDifferentSkewsGenerateCorrectSkewValues)
{
  TestFixture::VerifyConstructorWithArgs();
}

TYPED_TEST(ZipfDistributionFixture, CopiedInstanceGenerateSameIDs)
{
  TestFixture::VerifyCopyInitializers();
}

TYPED_TEST(ZipfDistributionFixture, MovedInstanceGenerateSameIDs)
{
  TestFixture::VerifyMoveInitializers();
}

}  // namespace dbgroup::random::test
