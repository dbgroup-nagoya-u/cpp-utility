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

#include "../common.hpp"

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

 private:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kRepeatNum = 1e6;
  static constexpr double kAllowableError = 0.01;
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

TEST_F(ZipfGeneratorFixture, Construct_WithDifferentSkews_GenerateCorrectSkewValues)
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

TEST_F(ZipfGeneratorFixture, ParenOps_WithMultiThreads_GenerateCorrectSkewValues)
{
  for (double alpha = 0; alpha < 2; alpha += 0.1) {
    ZipfGenerator zipf_gen{kBinNum, alpha};

    auto f = [&]() {
      const auto generated_ids = RunZipfEngine(zipf_gen, kRandomSeed);
      CheckGeneratedIDsObeyZipfLaw(generated_ids, alpha);
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < kThreadNum; ++i) threads.emplace_back(f);
    for (auto &&thread : threads) thread.join();
  }
}

TEST_F(ZipfGeneratorFixture, SetZipfParameters_SetSameSkew_GenerateSameValues)
{
  ZipfGenerator zipf_gen{kBinNum, kSkew};
  const auto first = RunZipfEngine(zipf_gen, kRandomSeed);

  zipf_gen.SetZipfParameters(kBinNum, kSkew);
  const auto second = RunZipfEngine(zipf_gen, kRandomSeed);

  EXPECT_TRUE(std::equal(first.begin(), first.end(), second.begin(), second.end()));
}

TEST_F(ZipfGeneratorFixture, SetZipfParameters_SetDifferentSkew_GenerateDifferentValues)
{
  constexpr double kFirstSkew = 0;
  constexpr double kSecondSkew = 1.0;

  ZipfGenerator zipf_gen{kBinNum, kFirstSkew};
  const auto first = RunZipfEngine(zipf_gen, kRandomSeed);

  zipf_gen.SetZipfParameters(kBinNum, kSecondSkew);
  const auto second = RunZipfEngine(zipf_gen, kRandomSeed);

  EXPECT_FALSE(std::equal(first.begin(), first.end(), second.begin(), second.end()));
}

}  // namespace dbgroup::random::zipf
