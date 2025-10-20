/*
 * Copyright 2025 Database Group, Nagoya University
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
#include "dbgroup/types.hpp"

// C++ standard libraries
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

namespace dbgroup::test
{
template <class UInt>
class SerialFixture : public ::testing::Test
{
 protected:
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Serial_t = Serial<UInt>;

  /*##########################################################################*
   * Constants
   *##########################################################################*/

  static constexpr UInt kOne = UInt{1};
  static constexpr UInt kMax = ~UInt{};
  static constexpr UInt kHalf = (kMax >> kOne) + kOne;

  /*##########################################################################*
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*##########################################################################*
   * Functions for verification
   *##########################################################################*/

  void
  AssignmentOperators() const
  {
    Serial_t a;

    // +=
    a = {};
    a += kOne;
    EXPECT_EQ(a.v, kOne) << "+=";

    a = {};
    a += Serial_t{kOne};
    EXPECT_EQ(a.v, kOne) << "+=";

    a = {kMax};
    a += kOne;
    EXPECT_EQ(a.v, 0) << "+=";

    a = {kMax};
    a += Serial_t{kOne};
    EXPECT_EQ(a.v, 0) << "+=";

    // -=
    a = {kOne};
    a -= kOne;
    EXPECT_EQ(a.v, 0) << "-=";

    a = {kOne};
    a -= Serial_t{kOne};
    EXPECT_EQ(a.v, 0) << "-=";

    a = {};
    a -= kOne;
    EXPECT_EQ(a.v, kMax) << "-=";

    a = {};
    a -= Serial_t{kOne};
    EXPECT_EQ(a.v, kMax) << "-=";

    // &=
    a = {};
    a &= kMax;
    EXPECT_EQ(a.v, 0) << "&=";

    a = {};
    a &= Serial_t{kMax};
    EXPECT_EQ(a.v, 0) << "&=";

    a = {kOne};
    a &= kMax;
    EXPECT_EQ(a.v, kOne) << "&=";

    a = {kOne};
    a &= Serial_t{kMax};
    EXPECT_EQ(a.v, kOne) << "&=";

    // |=
    a = {};
    a |= kMax;
    EXPECT_EQ(a.v, kMax) << "|=";

    a = {};
    a |= Serial_t{kMax};
    EXPECT_EQ(a.v, kMax) << "|=";

    a = {};
    a |= kOne;
    EXPECT_EQ(a.v, kOne) << "|=";

    a = {};
    a |= Serial_t{kOne};
    EXPECT_EQ(a.v, kOne) << "|=";

    // ^=
    a = {};
    a ^= kMax;
    EXPECT_EQ(a.v, kMax) << "^=";

    a = {};
    a ^= Serial_t{kMax};
    EXPECT_EQ(a.v, kMax) << "^=";

    a = {kMax};
    a ^= kMax;
    EXPECT_EQ(a.v, 0) << "^=";

    a = {kMax};
    a ^= Serial_t{kMax};
    EXPECT_EQ(a.v, 0) << "^=";

    // <<=
    a = {kOne};
    a <<= kOne;
    EXPECT_EQ(a.v, kOne << kOne) << "<<=";

    a = {kOne};
    a <<= kMax;
    EXPECT_EQ(a.v, 0) << "<<=";

    // >>=
    a = {kOne << kOne};
    a >>= kOne;
    EXPECT_EQ(a.v, kOne) << ">>=";

    a = {kOne};
    a >>= kOne;
    EXPECT_EQ(a.v, 0) << ">>=";
  }

  void
  IncrementDecrementOperators() const
  {
    Serial_t a;

    // ++ (prefix)
    a = {};
    EXPECT_EQ((++a).v, kOne) << "prefix ++";
    EXPECT_EQ(a.v, kOne) << "prefix ++ (value after)";

    a = {kMax};
    EXPECT_EQ((++a).v, 0) << "prefix ++ (wrap around)";
    EXPECT_EQ(a.v, 0) << "prefix ++ (wrap around, value after)";

    // ++ (postfix)
    a = {};
    EXPECT_EQ((a++).v, 0) << "postfix ++ (returned value)";
    EXPECT_EQ(a.v, kOne) << "postfix ++ (value after)";

    a = {kMax};
    EXPECT_EQ((a++).v, kMax) << "postfix ++ (returned value, wrap around)";
    EXPECT_EQ(a.v, 0) << "postfix ++ (value after, wrap around)";

    // -- (prefix)
    a = {kOne};
    EXPECT_EQ((--a).v, 0) << "prefix --";
    EXPECT_EQ(a.v, 0) << "prefix -- (value after)";

    a = {};
    EXPECT_EQ((--a).v, kMax) << "prefix -- (wrap around)";
    EXPECT_EQ(a.v, kMax) << "prefix -- (wrap around, value after)";

    // -- (postfix)
    a = {kOne};
    EXPECT_EQ((a--).v, kOne) << "postfix -- (returned value)";
    EXPECT_EQ(a.v, 0) << "postfix -- (value after)";

    a = {};
    EXPECT_EQ((a--).v, 0) << "postfix -- (returned value, wrap around)";
    EXPECT_EQ(a.v, kMax) << "postfix -- (value after, wrap around)";
  }

  void
  ArithmeticOperators() const
  {
    // +
    EXPECT_EQ((Serial_t{} + kOne).v, kOne) << "+";
    EXPECT_EQ((Serial_t{} + Serial_t{kOne}).v, kOne) << "+";

    EXPECT_EQ((Serial_t{kMax} + kOne).v, 0) << "+";
    EXPECT_EQ((Serial_t{kMax} + Serial_t{kOne}).v, 0) << "+";

    // -
    EXPECT_EQ((Serial_t{kOne} - kOne).v, 0) << "-";
    EXPECT_EQ((Serial_t{kOne} - Serial_t{kOne}).v, 0) << "-";

    EXPECT_EQ((Serial_t{} - kOne).v, kMax) << "-";
    EXPECT_EQ((Serial_t{} - Serial_t{kOne}).v, kMax) << "-";

    // ~
    EXPECT_EQ((~Serial_t{}).v, kMax) << "~";

    EXPECT_EQ((~Serial_t{kMax}).v, 0) << "~";

    // &
    EXPECT_EQ((Serial_t{} & kMax).v, 0) << "&";
    EXPECT_EQ((Serial_t{} & Serial_t{kMax}).v, 0) << "&";

    EXPECT_EQ((Serial_t{kOne} & kMax).v, kOne) << "&";
    EXPECT_EQ((Serial_t{kOne} & Serial_t{kMax}).v, kOne) << "&";

    // |
    EXPECT_EQ((Serial_t{} | kMax).v, kMax) << "|";
    EXPECT_EQ((Serial_t{} | Serial_t{kMax}).v, kMax) << "|";

    EXPECT_EQ((Serial_t{} | kOne).v, kOne) << "|";
    EXPECT_EQ((Serial_t{} | Serial_t{kOne}).v, kOne) << "|";

    // ^
    EXPECT_EQ((Serial_t{} ^ kMax).v, kMax) << "^";
    EXPECT_EQ((Serial_t{} ^ Serial_t{kMax}).v, kMax) << "^";

    EXPECT_EQ((Serial_t{kMax} ^ kMax).v, 0) << "^";
    EXPECT_EQ((Serial_t{kMax} ^ Serial_t{kMax}).v, 0) << "^";

    // <<
    EXPECT_EQ((Serial_t{kOne} << kOne).v, kOne << kOne) << "<<";

    EXPECT_EQ((Serial_t{kOne} << kMax).v, 0) << "<<";

    // >>
    EXPECT_EQ((Serial_t{kOne << kOne} >> kOne).v, kOne) << ">>";

    EXPECT_EQ((Serial_t{kOne} >> kOne).v, 0) << ">>";
  }

  void
  ComparisonOperators() const
  {
    Serial_t a;
    Serial_t b;

    // ==
    a = {};
    b = {};
    EXPECT_TRUE(a == b) << "== (equal zeros)";

    a = {kOne};
    b = {kOne};
    EXPECT_TRUE(a == b) << "== (equal non-zero)";

    a = {};
    b = {kOne};
    EXPECT_FALSE(a == b) << "== (not equal)";

    // !=
    a = {};
    b = {kOne};
    EXPECT_TRUE(a != b) << "!= (different)";
    a = {kOne};
    b = {kOne};
    EXPECT_FALSE(a != b) << "!= (equal)";

    // <
    a = {kOne};
    b = {kOne + kOne};
    EXPECT_TRUE(a < b) << "< (normal ascending)";

    a = {kMax};
    b = {};
    EXPECT_TRUE(a < b) << "< (wrap-around: kMax < 0)";

    a = {};
    b = {kMax};
    EXPECT_FALSE(a < b) << "< (wrap-around reversed)";

    // >
    a = {kOne + kOne};
    b = {kOne};
    EXPECT_TRUE(a > b) << "> (normal descending)";

    a = {};
    b = {kMax};
    EXPECT_TRUE(a > b) << "> (wrap-around: 0 > kMax)";

    a = {kMax};
    b = {};
    EXPECT_FALSE(a > b) << "> (wrap-around reversed)";

    // <=
    a = {kOne};
    b = {kOne};
    EXPECT_TRUE(a <= b) << "<= (equal)";

    a = {kOne};
    b = {kOne + kOne};
    EXPECT_TRUE(a <= b) << "<= (less)";

    a = {kMax};
    b = {};
    EXPECT_TRUE(a <= b) << "<= (wrap-around less)";

    a = {};
    b = {kMax};
    EXPECT_FALSE(a <= b) << "<= (wrap-around reversed)";

    // >=
    a = {kOne};
    b = {kOne};
    EXPECT_TRUE(a >= b) << ">= (equal)";

    a = {kOne + kOne};
    b = {kOne};
    EXPECT_TRUE(a >= b) << ">= (greater)";

    a = {};
    b = {kMax};
    EXPECT_TRUE(a >= b) << ">= (wrap-around greater)";

    a = {kMax};
    b = {};
    EXPECT_FALSE(a >= b) << ">= (wrap-around reversed)";

    // boundary tests
    a = {};
    b = {kHalf};
    EXPECT_TRUE(a < b) << "< (boundary: 0 vs kHalf)";
    EXPECT_TRUE(a <= b) << "<= (boundary: 0 vs kHalf)";
    EXPECT_FALSE(a > b) << "> (boundary: 0 vs kHalf)";
    EXPECT_FALSE(a >= b) << ">= (boundary: 0 vs kHalf)";

    a = {};
    b = {kHalf + kOne};
    EXPECT_FALSE(a < b) << "< (just beyond midpoint: 0 vs kHalf)";
    EXPECT_FALSE(a <= b) << "< (just beyond midpoint: 0 vs kHalf)";
    EXPECT_TRUE(a > b) << "> (just beyond midpoint: 0 vs kHalf)";
    EXPECT_TRUE(a >= b) << "> (just beyond midpoint: 0 vs kHalf)";

    a = {kHalf};
    b = {};
    EXPECT_TRUE(a < b) << "< (boundary: kHalf vs 0)";
    EXPECT_TRUE(a <= b) << "<= (boundary: kHalf vs 0)";
    EXPECT_FALSE(a > b) << "> (boundary: kHalf vs 0)";
    EXPECT_FALSE(a >= b) << ">= (boundary: kHalf vs 0)";

    a = {kHalf};
    b = {kOne};
    EXPECT_FALSE(a < b) << "< (just beyond midpoint: kHalf vs 0)";
    EXPECT_FALSE(a <= b) << "<= (just beyond midpoint: kHalf vs 0)";
    EXPECT_TRUE(a > b) << "> (just beyond midpoint: kHalf vs 0)";
    EXPECT_TRUE(a >= b) << ">= (just beyond midpoint: kHalf vs 0)";
  }
};

/*############################################################################*
 * Preparation for typed testing
 *############################################################################*/

using TestTypes = ::testing::Types<  //
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t>;
TYPED_TEST_SUITE(SerialFixture, TestTypes);

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

/*----------------------------------------------------------------------------*
 * Constructor tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    SerialFixture,
    AssignmentOperators)
{
  TestFixture::AssignmentOperators();
}

TYPED_TEST(  //
    SerialFixture,
    IncrementDecrementOperators)
{
  TestFixture::IncrementDecrementOperators();
}

TYPED_TEST(  //
    SerialFixture,
    ArithmeticOperators)
{
  TestFixture::ArithmeticOperators();
}

TYPED_TEST(  //
    SerialFixture,
    ComparisonOperators)
{
  TestFixture::ComparisonOperators();
}

}  // namespace dbgroup::test
