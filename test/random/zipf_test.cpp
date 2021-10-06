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

#include <algorithm>
#include <cmath>
#include <thread>
#include <vector>

namespace dbgroup::random::zipf
{
class ZipfGeneratorFixture : public ::testing::Test
{
 protected:
  /*################################################################################################
   * Public constants
   *##############################################################################################*/

  static constexpr size_t kBinNum = 1000;
  static constexpr size_t kSkew = 0;
  static constexpr size_t kRandomSeed = 20;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

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

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  std::vector<size_t>
  RunZipfEngine(  //
      ZipfGenerator zipf_gen,
      const size_t seed) const
  {
    std::mt19937_64 rand_engine{seed};

    std::vector<size_t> generated_ids;
    generated_ids.reserve(kRepeatNum);
    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto id = zipf_gen(rand_engine);
      generated_ids.emplace_back(id);

      EXPECT_GE(id, 0);
      EXPECT_LT(id, kBinNum);
    }

    return generated_ids;
  }

  void
  CheckGeneratedIDsObeyZipfLaw(  //
      const std::vector<size_t> &generated_ids,
      const double alpha) const
  {
    // count ID frequency
    std::vector<size_t> freq_dist(kBinNum, 0);
    for (auto &&id : generated_ids) {
      ++freq_dist[id];
    }

    // check ID frequency approximately equals to Zipf's law
    const auto base_prob = static_cast<double>(freq_dist[0]) / kRepeatNum;
    for (size_t k = 2; k <= kBinNum; ++k) {
      const auto kth_prob = static_cast<double>(freq_dist[k - 1]) / kRepeatNum;
      const auto error = abs(kth_prob - base_prob / pow(k, alpha));

      EXPECT_LT(error, kAllowableError);
    }
  }

  //
  //
  //

  static constexpr size_t kRepeatNum = 100000;
  static constexpr double kAllowableError = 0.01;
  static constexpr size_t kThreadNum = 8;

  std::mt19937_64 rand_engine_;
  ZipfGenerator zipf_gen_;

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

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/
};

/*--------------------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(ZipfGeneratorFixture, Construct_WithoutArgs_AlwaysGenerateZero)
{
  ZipfGenerator zipf_gen{};

  const auto generated_ids = RunZipfEngine(zipf_gen, kRandomSeed);
  for (auto &&id : generated_ids) {
    EXPECT_EQ(id, 0);
  }
}

TEST_F(ZipfGeneratorFixture, Construct_WithArgs_ZipfGenerateCorrectSkewVal)
{
  for (double alpha = 0; alpha < 2; alpha += 0.1) {
    ZipfGenerator zipf_gen{kBinNum, alpha};

    const auto generated_ids = RunZipfEngine(zipf_gen, kRandomSeed);
    CheckGeneratedIDsObeyZipfLaw(generated_ids, alpha);
  }
}

TEST_F(ZipfGeneratorFixture, CopyConstruct_WithArgs_CopiedInstanceGenerateSameIDs)
{
  ZipfGenerator orig_engine{kBinNum, kSkew};
  const auto orig = RunZipfEngine(orig_engine, kRandomSeed);

  ZipfGenerator copied_engine{orig_engine};
  auto copied = RunZipfEngine(copied_engine, kRandomSeed);

  EXPECT_TRUE(std::equal(orig.begin(), orig.end(), copied.begin(), copied.end()));
}

TEST_F(ZipfGeneratorFixture, CopyAssignment_WithArgs_CopiedInstanceGenerateSameIDs)
{
  ZipfGenerator orig_engine{kBinNum, kSkew};
  const auto orig = RunZipfEngine(orig_engine, kRandomSeed);

  ZipfGenerator copied_engine = orig_engine;
  auto copied = RunZipfEngine(copied_engine, kRandomSeed);

  EXPECT_TRUE(std::equal(orig.begin(), orig.end(), copied.begin(), copied.end()));
}

TEST_F(ZipfGeneratorFixture, MoveConstruct_WithArgs_MovedInstanceGenerateSameIDs)
{
  ZipfGenerator orig_engine{kBinNum, kSkew};
  const auto orig = RunZipfEngine(orig_engine, kRandomSeed);

  ZipfGenerator moved_engine{std::move(orig_engine)};
  auto moved = RunZipfEngine(moved_engine, kRandomSeed);

  EXPECT_TRUE(std::equal(orig.begin(), orig.end(), moved.begin(), moved.end()));
}

TEST_F(ZipfGeneratorFixture, MoveAssignment_WithArgs_MovedInstanceGenerateSameIDs)
{
  ZipfGenerator orig_engine{kBinNum, kSkew};
  const auto orig = RunZipfEngine(orig_engine, kRandomSeed);

  ZipfGenerator moved_engine = std::move(orig_engine);
  auto moved = RunZipfEngine(moved_engine, kRandomSeed);

  EXPECT_TRUE(std::equal(orig.begin(), orig.end(), moved.begin(), moved.end()));
}

/*--------------------------------------------------------------------------------------------------
 * Public utility tests
 *------------------------------------------------------------------------------------------------*/

// TEST_F(ZipfGeneratorFixture, ParenOps_WithDifferentSkew_ZipfGenerateDifferentVal)
// {
//   std::vector<size_t> first_genrated_vals;
//   first_genrated_vals.reserve(kRepeatNum);
//   for (size_t i = 0; i < kRepeatNum; ++i) {
//     first_genrated_vals.emplace_back(zipf_gen_(rand_engine_));
//   }

//   zipf_gen_ = ZipfGenerator{kBinNum, 2};
//   rand_engine_ = std::mt19937_64{0};  // initialize a random seed

//   std::vector<size_t> second_genrated_vals;
//   second_genrated_vals.reserve(kRepeatNum);
//   for (size_t i = 0; i < kRepeatNum; ++i) {
//     second_genrated_vals.emplace_back(zipf_gen_(rand_engine_));
//   }

//   EXPECT_FALSE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
// }

// TEST_F(ZipfGeneratorFixture, ParenOps_MultiThreads_ZipfGenerateCorrectSkewVal)
// {
//   for (double alpha = 0; alpha < 2; alpha += 0.1) {
//     zipf_gen_ = ZipfGenerator{kBinNum, alpha};

//     std::vector<std::thread> threads;
//     for (size_t i = 0; i < kThreadNum; ++i) {
//       threads.emplace_back(std::thread{&ZipfGeneratorFixture::RunZipfEngine, this, alpha});
//     }
//     for (auto &&thread : threads) {
//       thread.join();
//     }
//   }
// }

// TEST_F(ZipfGeneratorFixture, SetZipfParameters_SetDifferentSkew_ZipfGenerateDifferentVal)
// {
//   std::vector<size_t> first_genrated_vals;
//   first_genrated_vals.reserve(kRepeatNum);
//   for (size_t i = 0; i < kRepeatNum; ++i) {
//     first_genrated_vals.emplace_back(zipf_gen_(rand_engine_));
//   }

//   zipf_gen_.SetZipfParameters(kBinNum, 2);
//   rand_engine_ = std::mt19937_64{0};  // initialize a random seed

//   std::vector<size_t> second_genrated_vals;
//   second_genrated_vals.reserve(kRepeatNum);
//   for (size_t i = 0; i < kRepeatNum; ++i) {
//     second_genrated_vals.emplace_back(zipf_gen_(rand_engine_));
//   }

//   EXPECT_FALSE(VecHaveSameElements(first_genrated_vals, second_genrated_vals));
// }

}  // namespace dbgroup::random::zipf
