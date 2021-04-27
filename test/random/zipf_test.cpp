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

  bool
  VecHaveSameElements(  //
      const std::vector<size_t> &first_vec,
      const std::vector<size_t> &second_vec)
  {
    if (first_vec.size() != second_vec.size()) {
      return false;
    }

    for (size_t i = 0; i < first_vec.size(); ++i) {
      if (first_vec[i] != second_vec[i]) {
        return false;
      }
    }

    return true;
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
    const auto zipf_val = zipf_gen();
    EXPECT_EQ(zipf_val, 0);
  }
}

TEST_F(ZipfGeneratorFixture, Construct_WithArgs_ZipfGenerateCorrectSkewVal)
{
  for (double alpha = 0; alpha < 2; alpha += 0.1) {
    ZipfGenerator zipf_gen{kBinNum, alpha, 0};

    std::vector<size_t> freq_dist(kBinNum, 0);
    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto zipf_val = zipf_gen();

      ASSERT_GE(zipf_val, 0);
      ASSERT_LT(zipf_val, kBinNum);

      ++freq_dist[zipf_val];
    }

    CheckProbsObeyZipfLaw(freq_dist, alpha);
  }
}

TEST_F(ZipfGeneratorFixture, Construct_WithSameArgs_ZipfGenerateSameVal)
{
  ZipfGenerator zipf_gen{kBinNum, 1, 0};

  std::vector<size_t> first_genrated_vals;
  first_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    first_genrated_vals.emplace_back(zipf_gen());
  }

  zipf_gen = ZipfGenerator{kBinNum, 1, 0};

  std::vector<size_t> second_genrated_vals;
  second_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    second_genrated_vals.emplace_back(zipf_gen());
  }

  EXPECT_TRUE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
}

TEST_F(ZipfGeneratorFixture, Construct_WithDifferentSkew_ZipfGenerateDifferentVal)
{
  ZipfGenerator zipf_gen{kBinNum, 1, 0};

  std::vector<size_t> first_genrated_vals;
  first_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    first_genrated_vals.emplace_back(zipf_gen());
  }

  zipf_gen = ZipfGenerator{kBinNum, 2, 0};

  std::vector<size_t> second_genrated_vals;
  second_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    second_genrated_vals.emplace_back(zipf_gen());
  }

  EXPECT_FALSE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
}

TEST_F(ZipfGeneratorFixture, Construct_WithDifferentSeed_ZipfGenerateDifferentVal)
{
  ZipfGenerator zipf_gen{kBinNum, 1, 0};

  std::vector<size_t> first_genrated_vals;
  first_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    first_genrated_vals.emplace_back(zipf_gen());
  }

  zipf_gen = ZipfGenerator{kBinNum, 1, 1};

  std::vector<size_t> second_genrated_vals;
  second_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    second_genrated_vals.emplace_back(zipf_gen());
  }

  EXPECT_FALSE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
}

TEST_F(ZipfGeneratorFixture, SetZipfParameters_SetSameSkew_ZipfGenerateSameVal)
{
  ZipfGenerator zipf_gen{kBinNum, 1, 0};

  std::vector<size_t> first_genrated_vals;
  first_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    first_genrated_vals.emplace_back(zipf_gen());
  }

  zipf_gen = ZipfGenerator{kBinNum, 2, 0};  // initialize a random seed
  zipf_gen.SetZipfParameters(kBinNum, 1);

  std::vector<size_t> second_genrated_vals;
  second_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    second_genrated_vals.emplace_back(zipf_gen());
  }

  EXPECT_TRUE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
}

TEST_F(ZipfGeneratorFixture, SetZipfParameters_SetDifferentSkew_ZipfGenerateDifferentVal)
{
  ZipfGenerator zipf_gen{kBinNum, 1, 0};

  std::vector<size_t> first_genrated_vals;
  first_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    first_genrated_vals.emplace_back(zipf_gen());
  }

  zipf_gen = ZipfGenerator{kBinNum, 1, 0};  // initialize a random seed
  zipf_gen.SetZipfParameters(kBinNum, 2);

  std::vector<size_t> second_genrated_vals;
  second_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    second_genrated_vals.emplace_back(zipf_gen());
  }

  EXPECT_FALSE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
}

TEST_F(ZipfGeneratorFixture, SetRandomSeed_SetSameSeed_ZipfGenerateSameVal)
{
  ZipfGenerator zipf_gen{kBinNum, 1, 0};

  std::vector<size_t> first_genrated_vals;
  first_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    first_genrated_vals.emplace_back(zipf_gen());
  }

  zipf_gen.SetRandomSeed(0);

  std::vector<size_t> second_genrated_vals;
  second_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    second_genrated_vals.emplace_back(zipf_gen());
  }

  EXPECT_TRUE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
}

TEST_F(ZipfGeneratorFixture, SetRandomSeed_SetDifferentSeed_ZipfGenerateDifferentVal)
{
  ZipfGenerator zipf_gen{kBinNum, 1, 0};

  std::vector<size_t> first_genrated_vals;
  first_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    first_genrated_vals.emplace_back(zipf_gen());
  }

  zipf_gen.SetRandomSeed(1);

  std::vector<size_t> second_genrated_vals;
  second_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    second_genrated_vals.emplace_back(zipf_gen());
  }

  EXPECT_FALSE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
}

}  // namespace dbgroup::random::zipf
