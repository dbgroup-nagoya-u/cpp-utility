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

#include <gtest/gtest.h>

#include <cmath>
#include <thread>
#include <vector>

namespace dbgroup::random::zipf
{
class ZipfGeneratorFixture : public ::testing::Test
{
 public:
  static constexpr size_t kRepeatNum = 100000;
  static constexpr size_t kBinNum = 100;
  static constexpr double kAllowableError = 0.01;
  static constexpr size_t kThreadNum = 8;

  std::mt19937_64 rand_engine_;
  ZipfGenerator zipf_gen_;

  void
  CheckProbsObeyZipfLaw(  //
      const std::vector<size_t> &freq_dist,
      const double alpha) const
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
      const std::vector<size_t> &second_vec) const
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

  void
  RunZipfEngine(const double alpha)
  {
    std::vector<size_t> freq_dist(kBinNum, 0);
    std::mt19937_64 rand_engine{0};

    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto zipf_val = zipf_gen_(rand_engine);

      ASSERT_GE(zipf_val, 0);
      ASSERT_LT(zipf_val, kBinNum);

      ++freq_dist[zipf_val];
    }

    CheckProbsObeyZipfLaw(freq_dist, alpha);
  }

 protected:
  void
  SetUp() override
  {
    rand_engine_ = std::mt19937_64{0};
    zipf_gen_ = ZipfGenerator{kBinNum, 1};
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
  zipf_gen_ = ZipfGenerator{};

  for (size_t i = 0; i < kRepeatNum; ++i) {
    const auto zipf_val = zipf_gen_(rand_engine_);
    EXPECT_EQ(zipf_val, 0);
  }
}

TEST_F(ZipfGeneratorFixture, ParenOps_WithArgs_ZipfGenerateCorrectSkewVal)
{
  for (double alpha = 0; alpha < 2; alpha += 0.1) {
    zipf_gen_ = ZipfGenerator{kBinNum, alpha};

    std::thread{&ZipfGeneratorFixture::RunZipfEngine, this, alpha}.join();
  }
}

TEST_F(ZipfGeneratorFixture, ParenOps_WithDifferentSkew_ZipfGenerateDifferentVal)
{
  std::vector<size_t> first_genrated_vals;
  first_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    first_genrated_vals.emplace_back(zipf_gen_(rand_engine_));
  }

  zipf_gen_ = ZipfGenerator{kBinNum, 2};
  rand_engine_ = std::mt19937_64{0};  // initialize a random seed

  std::vector<size_t> second_genrated_vals;
  second_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    second_genrated_vals.emplace_back(zipf_gen_(rand_engine_));
  }

  EXPECT_FALSE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
}

TEST_F(ZipfGeneratorFixture, ParenOps_MultiThreads_ZipfGenerateCorrectSkewVal)
{
  for (double alpha = 0; alpha < 2; alpha += 0.1) {
    zipf_gen_ = ZipfGenerator{kBinNum, alpha};

    std::vector<std::thread> threads;
    for (size_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(std::thread{&ZipfGeneratorFixture::RunZipfEngine, this, alpha});
    }
    for (auto &&thread : threads) {
      thread.join();
    }
  }
}

TEST_F(ZipfGeneratorFixture, SetZipfParameters_SetDifferentSkew_ZipfGenerateDifferentVal)
{
  std::vector<size_t> first_genrated_vals;
  first_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    first_genrated_vals.emplace_back(zipf_gen_(rand_engine_));
  }

  zipf_gen_.SetZipfParameters(kBinNum, 2);
  rand_engine_ = std::mt19937_64{0};  // initialize a random seed

  std::vector<size_t> second_genrated_vals;
  second_genrated_vals.reserve(kRepeatNum);
  for (size_t i = 0; i < kRepeatNum; ++i) {
    second_genrated_vals.emplace_back(zipf_gen_(rand_engine_));
  }

  EXPECT_FALSE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
}

}  // namespace dbgroup::random::zipf
