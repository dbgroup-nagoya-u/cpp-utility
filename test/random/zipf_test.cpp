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

// the corresponding header
#include "dbgroup/random/zipf.hpp"

// C++ standard libraries
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <random>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

namespace dbgroup::random::test
{
/*############################################################################*
 * Global constants
 *############################################################################*/

/// @brief Skew parameters for testing.
constexpr double kAlphas[] = {0.0, 0.5, 1.0, 1.5, 2.0, 3.0};

/// @brief The number of samples for statistical tests.
constexpr size_t kSampleNum = 100'000;

/// @brief The number of samples for comparing two sequences.
constexpr size_t kCompareNum = 10'000;

/// @brief The number of buckets for chi-squared tests.
constexpr size_t kBucketNum = 50;

/// @brief The maximum number of bins for exact Zipf distributions.
constexpr uint64_t kExactBinNum = 1'000;

/// @brief The allowable error for exact CDF values.
constexpr double kExactCDFError = 1e-12;

/// @brief The allowable error for approximate CDF values.
constexpr double kApproxCDFError = 1e-6;

/*############################################################################*
 * Global utilities
 *############################################################################*/

/**
 * @param n The number of bins.
 * @param alpha A skew parameter.
 * @return Reference CDF values computed with extended precision.
 */
auto
ComputeReferenceCDF(  //
    const uint64_t n,
    const double alpha)  //
    -> std::vector<double>
{
  std::vector<long double> sums(n);
  long double sum = 0;
  for (uint64_t k = 0; k < n; ++k) {
    sum += std::pow(static_cast<long double>(k + 1), -static_cast<long double>(alpha));
    sums[k] = sum;
  }
  std::vector<double> cdf(n);
  for (uint64_t k = 0; k < n; ++k) {
    cdf[k] = static_cast<double>(sums[k] / sum);
  }
  return cdf;
}

/*############################################################################*
 * Fixture definitions
 *############################################################################*/

template <class IntType>
class ZipfDistributionFixture : public ::testing::Test
{
 protected:
  /*##########################################################################*
   * Type aliases and constants
   *##########################################################################*/

  using Zipf_t = ZipfDistribution<IntType>;
  using ApproxZipf_t = ApproxZipfDistribution<IntType>;
  using UInt = std::make_unsigned_t<IntType>;

  static constexpr IntType kLowest = std::numeric_limits<IntType>::lowest();
  static constexpr IntType kHighest = std::numeric_limits<IntType>::max();

  /*##########################################################################*
   * Internal utilities
   *##########################################################################*/

  /**
   * @return The ID of a value in [`min`, `min + n`).
   */
  static auto
  ToID(  //
      const IntType val,
      const IntType min)  //
      -> uint64_t
  {
    return static_cast<UInt>(static_cast<UInt>(val) - static_cast<UInt>(min));
  }

  /**
   * @return The value of an ID in [`min`, `min + n`).
   */
  static auto
  ToValue(  //
      const uint64_t id,
      const IntType min)  //
      -> IntType
  {
    return static_cast<IntType>(static_cast<UInt>(min) + static_cast<UInt>(id));
  }

  /**
   * @return The first bin ID in [`lo`, `hi`] whose CDF is not less than `p`.
   */
  template <class Dist>
  static auto
  FindBin(  //
      const Dist& dist,
      uint64_t lo,
      uint64_t hi,
      const double p)  //
      -> uint64_t
  {
    while (lo < hi) {
      const auto mid = lo + ((hi - lo) >> 1UL);
      if (dist.GetCDF(mid) < p) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  /**
   * @brief Check generated values obey the CDF by using a chi-squared test.
   *
   * IDs are grouped into buckets with almost the same probability, and the
   * test fails if the statistic exceeds its mean by six standard deviations.
   */
  template <class Dist>
  static void
  CheckSamplesFollowCDF(  //
      const Dist& dist,
      const IntType min,
      const uint64_t n)
  {
    // split IDs into buckets by using quantiles of the CDF
    std::vector<uint64_t> bounds{};  // the last IDs of buckets
    for (size_t j = 1; j < kBucketNum; ++j) {
      const auto q = static_cast<double>(j) / kBucketNum;
      const auto bin = FindBin(dist, bounds.empty() ? 0 : bounds.back(), n - 1, q);
      if (bounds.empty() || bounds.back() < bin) {
        bounds.emplace_back(bin);
      }
    }
    if (bounds.empty() || bounds.back() < n - 1) {
      bounds.emplace_back(n - 1);
    }

    // count samples in each bucket
    std::mt19937_64 rand_engine{kRandomSeed};  // NOLINT
    std::vector<size_t> counts(bounds.size(), 0);
    size_t out_of_range = 0;
    for (size_t i = 0; i < kSampleNum; ++i) {
      const auto id = ToID(dist(rand_engine), min);
      if (id >= n) {
        ++out_of_range;
        continue;
      }
      ++counts[std::lower_bound(bounds.begin(), bounds.end(), id) - bounds.begin()];
    }
    ASSERT_EQ(out_of_range, 0);
    if (bounds.size() <= 1) return;  // all the samples are in one bucket

    // compute the chi-squared statistic
    auto stat = 0.0;
    auto prev_cdf = 0.0;
    for (size_t j = 0; j < bounds.size(); ++j) {
      const auto cdf = dist.GetCDF(bounds[j]);
      const auto expected = (cdf - prev_cdf) * kSampleNum;
      const auto diff = static_cast<double>(counts[j]) - expected;
      stat += diff * diff / expected;
      prev_cdf = cdf;
    }
    const auto df = static_cast<double>(bounds.size() - 1);
    EXPECT_LT(stat, df + 6.0 * std::sqrt(2.0 * df));
  }

  /**
   * @brief Check each generated value is the first bin whose CDF is not less
   * than the corresponding uniform random value.
   *
   * This assumes that a distribution consumes one uniform real value in [0, 1)
   * for each generation.
   */
  template <class Dist>
  static void
  CheckGenerationInvertsCDF(  //
      const Dist& dist,
      const IntType min,
      const uint64_t n)
  {
    std::mt19937_64 dist_engine{kRandomSeed};  // NOLINT
    std::mt19937_64 ref_engine{kRandomSeed};   // NOLINT
    std::uniform_real_distribution<double> uniform{0.0, 1.0};
    size_t mismatches = 0;
    for (size_t i = 0; i < kCompareNum; ++i) {
      const auto expected = FindBin(dist, 0, n - 1, uniform(ref_engine));
      mismatches += ToID(dist(dist_engine), min) != expected;
    }
    EXPECT_EQ(mismatches, 0) << "n=" << n;
  }

  /**
   * @brief Check two distributions generate the same sequence.
   */
  template <class Dist>
  static void
  CheckSameSequences(  //
      const Dist& expected,
      const Dist& actual)
  {
    std::mt19937_64 e_engine{kRandomSeed};  // NOLINT
    std::mt19937_64 a_engine{kRandomSeed};  // NOLINT
    size_t mismatches = 0;
    for (size_t i = 0; i < kCompareNum; ++i) {
      mismatches += expected(e_engine) != actual(a_engine);
    }
    EXPECT_EQ(mismatches, 0);
  }

  /*##########################################################################*
   * Functions for verification
   *##########################################################################*/

  void
  VerifyDefaultConstructors()
  {
    std::mt19937_64 rand_engine{kRandomSeed};  // NOLINT
    const Zipf_t zipf{};
    const ApproxZipf_t approx{};
    size_t non_zero = 0;
    for (size_t i = 0; i < kCompareNum; ++i) {
      non_zero += zipf(rand_engine) != 0;
      non_zero += approx(rand_engine) != 0;
    }
    EXPECT_EQ(non_zero, 0);
  }

  void
  VerifyInvalidRanges()
  {
    EXPECT_THROW((Zipf_t{1, 0, 1.0}), std::runtime_error);
    EXPECT_THROW((ApproxZipf_t{1, 0, 1.0}), std::runtime_error);

    // bin IDs must be less than the number of bins
    const Zipf_t zipf{0, 9, 1.0};
    EXPECT_THROW(std::ignore = zipf.GetCDF(10), std::out_of_range);
    const ApproxZipf_t approx{0, 999, 1.0};
    EXPECT_THROW(std::ignore = approx.GetCDF(1'000), std::out_of_range);

    if constexpr (sizeof(IntType) == sizeof(uint64_t)) {
      EXPECT_THROW((Zipf_t{kLowest, kHighest, 1.0}), std::runtime_error);
      EXPECT_THROW((ApproxZipf_t{kLowest, kHighest, 1.0}), std::runtime_error);
    } else {  // approximate distributions can cover all the 32-bit integers
      constexpr uint64_t kBinNum = uint64_t{1} << 32UL;
      const ApproxZipf_t full{kLowest, kHighest, 1.0};
      EXPECT_EQ(full.GetCDF(kBinNum - 1), 1.0);
      CheckSamplesFollowCDF(full, kLowest, kBinNum);
      CheckGenerationInvertsCDF(full, kLowest, kBinNum);
    }
  }

  void
  VerifyExactCDF()
  {
    for (const auto alpha : kAlphas) {
      for (const uint64_t n : std::initializer_list<uint64_t>{1, 2, 10, kExactBinNum}) {
        const Zipf_t zipf{0, static_cast<IntType>(n - 1), alpha};
        const auto ref = ComputeReferenceCDF(n, alpha);
        auto max_err = 0.0;
        for (uint64_t k = 0; k < n; ++k) {
          max_err = std::max(max_err, std::abs(zipf.GetCDF(k) - ref[k]));
        }
        EXPECT_LT(max_err, kExactCDFError) << "alpha=" << alpha << ", n=" << n;
        EXPECT_EQ(zipf.GetCDF(n - 1), 1.0);
      }
    }
  }

  void
  VerifyApproxCDF()
  {
    for (const auto alpha : kAlphas) {
      for (const uint64_t n : {1UL, 50UL, 100UL, 101UL, 1'000UL, 100'000UL}) {
        const ApproxZipf_t approx{0, static_cast<IntType>(n - 1), alpha};
        const auto ref = ComputeReferenceCDF(n, alpha);
        auto max_err = 0.0;
        auto prev = 0.0;
        size_t non_monotonic = 0;
        for (uint64_t k = 0; k < n; ++k) {
          const auto cdf = approx.GetCDF(k);
          max_err = std::max(max_err, std::abs(cdf - ref[k]));
          non_monotonic += cdf < prev;
          prev = cdf;
        }
        EXPECT_LT(max_err, kApproxCDFError) << "alpha=" << alpha << ", n=" << n;
        EXPECT_EQ(non_monotonic, 0) << "alpha=" << alpha << ", n=" << n;
        EXPECT_EQ(approx.GetCDF(n - 1), 1.0);
      }
    }
  }

  void
  VerifyExactSamples()
  {
    constexpr auto kN = kExactBinNum;
    for (const auto alpha : kAlphas) {
      for (const auto min : {kLowest, static_cast<IntType>(kHighest - (kN - 1))}) {
        const Zipf_t zipf{min, ToValue(kN - 1, min), alpha};
        CheckSamplesFollowCDF(zipf, min, kN);
      }
    }
  }

  void
  VerifyApproxSamples()
  {
    constexpr uint64_t kN = 1'000'000;
    for (const auto alpha : kAlphas) {
      for (const auto min : {kLowest, static_cast<IntType>(kHighest - (kN - 1))}) {
        const ApproxZipf_t approx{min, ToValue(kN - 1, min), alpha};
        CheckSamplesFollowCDF(approx, min, kN);
      }
    }
  }

  void
  VerifyExactGenerationInvertsCDF()
  {
    for (const auto alpha : kAlphas) {
      for (const uint64_t n : std::initializer_list<uint64_t>{1, 10, kExactBinNum}) {
        const Zipf_t zipf{kLowest, ToValue(n - 1, kLowest), alpha};
        CheckGenerationInvertsCDF(zipf, kLowest, n);
      }
    }
  }

  void
  VerifyApproxGenerationInvertsCDF()
  {
    for (const auto alpha : kAlphas) {
      for (const uint64_t n : {1UL, 100UL, 101UL, 1'000UL, 1'000'000UL}) {
        const ApproxZipf_t approx{kLowest, ToValue(n - 1, kLowest), alpha};
        CheckGenerationInvertsCDF(approx, kLowest, n);
      }
    }
  }

  void
  VerifyCopyAndMove()
  {
    const Zipf_t zipf{kLowest, ToValue(kExactBinNum - 1, kLowest), 1.0};
    const ApproxZipf_t approx{kLowest, ToValue(1'000'000 - 1, kLowest), 1.0};

    // copy constructors and assignment operators
    Zipf_t zipf_copy{zipf};
    CheckSameSequences(zipf, zipf_copy);
    zipf_copy = Zipf_t{};
    zipf_copy = zipf;
    CheckSameSequences(zipf, zipf_copy);
    ApproxZipf_t approx_copy{approx};
    CheckSameSequences(approx, approx_copy);
    approx_copy = ApproxZipf_t{};
    approx_copy = approx;
    CheckSameSequences(approx, approx_copy);

    // move constructors and assignment operators
    Zipf_t zipf_move{std::move(zipf_copy)};
    CheckSameSequences(zipf, zipf_move);
    zipf_move = Zipf_t{};
    zipf_move = Zipf_t{zipf};
    CheckSameSequences(zipf, zipf_move);
    ApproxZipf_t approx_move{std::move(approx_copy)};
    CheckSameSequences(approx, approx_move);
    approx_move = ApproxZipf_t{};
    approx_move = ApproxZipf_t{approx};
    CheckSameSequences(approx, approx_move);
  }
};

/*############################################################################*
 * Preparation for typed testing
 *############################################################################*/

using IntegralTypes = ::testing::Types<int32_t, int64_t, uint32_t, uint64_t>;
TYPED_TEST_SUITE(ZipfDistributionFixture, IntegralTypes);

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TYPED_TEST(ZipfDistributionFixture, DefaultConstructorsAlwaysGenerateZero)
{
  TestFixture::VerifyDefaultConstructors();
}

TYPED_TEST(ZipfDistributionFixture, InvalidRangesThrowExceptions)
{
  TestFixture::VerifyInvalidRanges();
}

TYPED_TEST(ZipfDistributionFixture, ExactCDFMatchesDefinition) { TestFixture::VerifyExactCDF(); }

TYPED_TEST(ZipfDistributionFixture, ApproxCDFIsCloseToExactOne) { TestFixture::VerifyApproxCDF(); }

TYPED_TEST(ZipfDistributionFixture, ExactSamplesFollowCDF) { TestFixture::VerifyExactSamples(); }

TYPED_TEST(ZipfDistributionFixture, ApproxSamplesFollowCDF) { TestFixture::VerifyApproxSamples(); }

TYPED_TEST(ZipfDistributionFixture, ExactGenerationInvertsCDF)
{
  TestFixture::VerifyExactGenerationInvertsCDF();
}

TYPED_TEST(ZipfDistributionFixture, ApproxGenerationInvertsCDF)
{
  TestFixture::VerifyApproxGenerationInvertsCDF();
}

TYPED_TEST(ZipfDistributionFixture, CopiedAndMovedInstancesGenerateSameValues)
{
  TestFixture::VerifyCopyAndMove();
}

}  // namespace dbgroup::random::test
