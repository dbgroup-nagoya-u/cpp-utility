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

#ifndef CPP_UTILITY_DBGROUP_RANDOM_ZIPF_HPP_
#define CPP_UTILITY_DBGROUP_RANDOM_ZIPF_HPP_

// C++ standard libraries
#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <random>
#include <type_traits>
#include <vector>

namespace dbgroup::random
{
/**
 * @brief A class to generate random values according to Zipf's law.
 *
 * @tparam IntType A class of generated random values.
 */
template <class IntType = size_t>
class ZipfDistribution
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct an empty distribution.
   *
   * This always returns zero.
   */
  ZipfDistribution();

  /**
   * @brief Construct a new Zipf distribution with given parameters.
   *
   * This distribution will generate random values within [`min`, `max`]
   * according to Zipf's law with a skew parameter `alpha`.
   *
   * @param min The minimum value to be generated.
   * @param max The maximum value to be generated.
   * @param alpha A skew parameter (zero means uniform distribution).
   * @throw std::runtime_error if `max` is less than `min` or [`min`, `max`]
   * covers the whole range of `IntType` for 64-bit integers.
   */
  ZipfDistribution(  //
      IntType min,
      IntType max,
      double alpha);

  ZipfDistribution(const ZipfDistribution&) = default;
  ZipfDistribution(ZipfDistribution&&) noexcept = default;

  auto operator=(const ZipfDistribution& obj) -> ZipfDistribution& = default;
  auto operator=(ZipfDistribution&&) noexcept -> ZipfDistribution& = default;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~ZipfDistribution() = default;

  /*##########################################################################*
   * Public getters
   *##########################################################################*/

  /**
   * @param id A target ID in [0, `bin_num`).
   * @return A CDF value of the given ID.
   */
  [[nodiscard]]
  constexpr auto
  GetCDF(                      //
      const IntType id) const  //
      -> double
  {
    return zipf_cdf_.at(static_cast<size_t>(id));
  }

  /*##########################################################################*
   * Public utility operators
   *##########################################################################*/

  /**
   * @param g A random value generator.
   * @return A random value according to Zipf's law.
   */
  template <class RandEngine>
  [[nodiscard]]
  auto
  operator()(               //
      RandEngine& g) const  //
      -> IntType
  {
    std::uniform_real_distribution<double> uniform{0.0, 1.0};
    const auto p = uniform(g);

    // find the first bin whose CDF is not less than the probability
    const auto it = std::lower_bound(zipf_cdf_.begin(), zipf_cdf_.end(), p);
    const auto id = std::min<uint64_t>(it - zipf_cdf_.begin(), zipf_cdf_.size() - 1);
    return static_cast<IntType>(static_cast<UInt>(min_) + static_cast<UInt>(id));
  }

 private:
  /*##########################################################################*
   * Internal type aliases
   *##########################################################################*/

  /// @brief An unsigned type for computing values without overflow.
  using UInt = std::make_unsigned_t<IntType>;

  /*##########################################################################*
   * Internal utility functions
   *##########################################################################*/

  /**
   * @brief Compute CDF values for this Zipf distribution.
   */
  void UpdateCDF();

  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  // Assume the use of integer types.
  static_assert(std::is_same_v<IntType, uint32_t>     //
                || std::is_same_v<IntType, uint64_t>  //
                || std::is_same_v<IntType, int32_t>   //
                || std::is_same_v<IntType, int64_t>);

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The minimum value to be generated.
  IntType min_{0};

  /// @brief The maximum value to be generated.
  IntType max_{0};

  /// @brief A skew parameter (zero means uniform distribution).
  double alpha_{0.0};

  /// @brief A cumulative distribution function according to Zipf's law.
  std::vector<double> zipf_cdf_{};
};

/**
 * @brief A class to generate random values according to Zipf's law approximately.
 *
 * @tparam IntType A class of generated random values.
 */
template <class IntType = size_t>
class ApproxZipfDistribution
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct an empty distribution.
   *
   * This always returns zero.
   */
  ApproxZipfDistribution();

  /**
   * @brief Construct a new Zipf distribution with given parameters.
   *
   * This distribution will generate random values within [`min`, `max`]
   * according to Zipf's law with a skew parameter `alpha`.
   *
   * @param min The minimum value to be generated.
   * @param max The maximum value to be generated.
   * @param alpha A skew parameter (zero means uniform distribution).
   * @throw std::runtime_error if `max` is less than `min` or [`min`, `max`]
   * covers the whole range of `IntType` for 64-bit integers.
   */
  ApproxZipfDistribution(  //
      IntType min,
      IntType max,
      double alpha);

  constexpr ApproxZipfDistribution(const ApproxZipfDistribution&) = default;
  constexpr ApproxZipfDistribution(ApproxZipfDistribution&&) noexcept = default;

  constexpr auto operator=(const ApproxZipfDistribution& obj)  //
      -> ApproxZipfDistribution& = default;
  constexpr auto operator=(ApproxZipfDistribution&&) noexcept  //
      -> ApproxZipfDistribution& = default;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~ApproxZipfDistribution() = default;

  /*##########################################################################*
   * Public getters
   *##########################################################################*/

  /**
   * @param id A target ID in [0, `bin_num`).
   * @return A CDF value of the given ID.
   */
  [[nodiscard]]
  auto
  GetCDF(                      //
      const IntType id) const  //
      -> double
  {
    const auto pos = static_cast<uint64_t>(id);
    if (pos < kExactBinNum) return zipf_cdf_[pos];
    return GetHarmonicNum(static_cast<double>(pos) + 1.0) / (c_ / 2.0);  // NOLINT
  }

  /*##########################################################################*
   * Public utility operators
   *##########################################################################*/

  /**
   * @param g A random value generator.
   * @return A random value according to Zipf's law.
   */
  template <class RandEngine>
  [[nodiscard]]
  auto
  operator()(               //
      RandEngine& g) const  //
      -> IntType
  {
    std::uniform_real_distribution<double> uniform{0.0, kMaxP};
    const auto p = uniform(g);

    // NOLINTBEGIN
    const auto bin =
        pow_ == 0 ? (-3.0 + std::sqrt(9.0 - 4.0 * (2.0 - std::exp(c_ * p - 1.0)))) / 2.0 + kBase
                  : std::pow((c_ * pow_ * p - pow_ + 2.0) / 2.0, 1.0 / pow_) - 1.0 + base_;
    // NOLINTEND
    return ToValue(static_cast<uint64_t>(bin));
  }

  /**
   * @param g A random value generator.
   * @return A random value according to Zipf's law.
   */
  template <class RandEngine>
  [[nodiscard]]
  auto
  GetIDUsingBinarySearch(   //
      RandEngine& g) const  //
      -> IntType
  {
    std::uniform_real_distribution<double> uniform{0.0, kMaxP};
    const auto p = uniform(g);

    // find a target bin by using a binary search
    int64_t begin_pos = 0;
    auto end_pos = static_cast<int64_t>(n_ - 1);
    while (begin_pos < end_pos) {
      auto pos = (begin_pos + end_pos) >> 1UL;  // NOLINT
      const auto cdf_val = GetCDF(static_cast<IntType>(pos));
      if (p < cdf_val) {
        end_pos = pos - 1;
      } else if (p > cdf_val) {
        begin_pos = pos + 1;
      } else {  // target_prob == cdf_val
        begin_pos = pos;
        break;
      }
    }
    if (p > GetCDF(static_cast<IntType>(begin_pos))) {
      ++begin_pos;
    }

    return ToValue(static_cast<uint64_t>(begin_pos));
  }

 private:
  /*##########################################################################*
   * Internal type aliases
   *##########################################################################*/

  /// @brief An unsigned type for computing values without overflow.
  using UInt = std::make_unsigned_t<IntType>;

  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  /// @brief The number of bins for approximation.
  static constexpr size_t kExactBinNum = 100;

  /// @brief The maximum value of random probabilities.
  static constexpr double kMaxP = 0.9999999999999999;

  /// @brief An offset for ensuring positive IDs.
  static constexpr double kBase = 0.71394692216654844;

  /*##########################################################################*
   * Internal utility functions
   *##########################################################################*/

  /**
   * @brief Compute CDF values for this Zipf distribution.
   */
  void UpdateCDF();

  /**
   * @param n The number of partial elements in the p-series.
   * @return An approximate partial sum of the p-series.
   */
  [[nodiscard]]
  auto
  GetHarmonicNum(            //
      const double n) const  //
      -> double
  {
    // NOLINTBEGIN
    return pow_ == 0 ? (1 + std::log(n) + std::log(n + 1)) * 0.5
                     : (std::pow(n + 1, pow_) + std::pow(n, pow_) - 2) / (2 * pow_) + 0.5;
    // NOLINTEND
  }

  /**
   * @param id A target ID in [0, `n_`).
   * @return The corresponding value in [`min_`, `max_`].
   */
  [[nodiscard]]
  constexpr auto
  ToValue(                      //
      const uint64_t id) const  //
      -> IntType
  {
    return static_cast<IntType>(static_cast<UInt>(min_) + static_cast<UInt>(id));
  }

  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  // Assume the use of integer types.
  static_assert(std::is_same_v<IntType, uint32_t>     //
                || std::is_same_v<IntType, uint64_t>  //
                || std::is_same_v<IntType, int32_t>   //
                || std::is_same_v<IntType, int64_t>);

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The minimum value to be generated.
  IntType min_{0};

  /// @brief The maximum value to be generated.
  IntType max_{0};

  /// @brief A skew parameter (zero means uniform distribution).
  double alpha_{0.0};

  /// @brief The number of bins in this Zipf distribution.
  uint64_t n_{1};

  /// @brief Equal to `1 - alpha_`.
  double pow_{1.0};

  /// @brief Equal to `2 * GetHarmonicNum(n_)`.
  double c_{};

  /// @brief An offset for ensuring positive IDs.
  double base_{};

  /// @brief A cumulative distribution function according to Zipf's law.
  std::array<double, kExactBinNum> zipf_cdf_{};
};

}  // namespace dbgroup::random

#endif  // CPP_UTILITY_DBGROUP_RANDOM_ZIPF_HPP_
