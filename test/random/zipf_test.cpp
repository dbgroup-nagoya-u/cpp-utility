// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "random/zipf.hpp"

#include <gtest/gtest.h>

namespace dbgroup::random::zipf
{
class ZipfGeneratorFixture : public ::testing::Test
{
 public:
  static constexpr size_t kRepeatNum = 10000;

  ZipfGenerator zipf_gen;

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

TEST_F(ZipfGeneratorFixture, Construct_WithoutArgs_MemberVariablesCorrectlyInitialized)
{
  zipf_gen = ZipfGenerator{};

  for (size_t i = 0; i < kRepeatNum; ++i) {
    const auto zipf_val = zipf_gen.Zipf();
    EXPECT_EQ(zipf_val, 0);
  }
}

}  // namespace dbgroup::random::zipf
