// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "random/zipf.hpp"

#include <gtest/gtest.h>

#include <cmath>

namespace dbgroup::random::zipf
{
class ZipfGeneratorFixture : public ::testing::Test
{
 public:
  static constexpr size_t kRepeatNum = 10000;
  static constexpr size_t kBinNum = 100;
  static constexpr double kAllowableError = 0.01;

  void
  CheckProbsObeyZipfLaw(  //
      const std::vector<size_t> &freq_dist,
      const double alpha)
  {
    const auto base_prob = static_cast<double>(freq_dist[0]) / kRepeatNum;
    for (size_t k = 2; k <= kBinNum; ++k) {
      const auto kth_prob = static_cast<double>(freq_dist[k - 1]) / kRepeatNum;
      const auto error = abs(kth_prob - base_prob / pow(k, alpha));

      EXPECT_LT(error, kAllowableError);
    }
  }

 protected:
  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }
};

/*--------------------------------------------------------------------------------------------------
 * Public utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(ZipfGeneratorFixture, Construct_WithoutArgs_ZipfGenerateAlwaysZero)
{
  ZipfGenerator zipf_gen{};

  for (size_t i = 0; i < kRepeatNum; ++i) {
    const auto zipf_val = zipf_gen.Zipf();
    EXPECT_EQ(zipf_val, 0);
  }
}

TEST_F(ZipfGeneratorFixture, Construct_WithArgs_ZipfGenerateCorrectSkewVal)
{
  for (double alpha = 0; alpha < 2; alpha += 0.1) {
    ZipfGenerator zipf_gen{kBinNum, alpha, 0};

    std::vector<size_t> freq_dist(kBinNum, 0);
    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto zipf_val = zipf_gen.Zipf();

      ASSERT_GE(zipf_val, 0);
      ASSERT_LT(zipf_val, kBinNum);

      ++freq_dist[zipf_val];
    }

    CheckProbsObeyZipfLaw(freq_dist, alpha);
  }
}

}  // namespace dbgroup::random::zipf
