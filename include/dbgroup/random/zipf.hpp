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
  /*############################################################################
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
   * according to Zipf's law with a skew paramter `alpha`.
   *
   * @param min The minimum value to be generated.
   * @param max The maximum value to be generated.
   * @param alpha A skew parameter (zero means uniform distribution).
   */
  ZipfDistribution(  //
      const IntType min,
      const IntType max,
      const double alpha);

  ZipfDistribution(const ZipfDistribution &) = default;
  ZipfDistribution(ZipfDistribution &&) noexcept = default;

  auto operator=(const ZipfDistribution &obj) -> ZipfDistribution & = default;
  auto operator=(ZipfDistribution &&) noexcept -> ZipfDistribution & = default;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~ZipfDistribution() = default;

  /*############################################################################
   * Public getters
   *##########################################################################*/

  /**
   * @param id A target ID in [0, `bin_num`).
   * @return A CDF value of the given ID.
   */
  [[nodiscard]] constexpr auto
  GetCDF(                      //
      const IntType id) const  //
      -> double
  {
    return zipf_cdf_.at(id);
  }

  /*############################################################################
   * Public utility operators
   *##########################################################################*/

  /**
   * @param g A random value generator.
   * @return A random value according to Zipf's law.
   */
  template <class RandEngine>
  [[nodiscard]] auto
  operator()(               //
      RandEngine &g) const  //
      -> IntType
  {
    thread_local std::uniform_real_distribution<double> uniform_dist{0.0, 1.0};
    const auto target_prob = uniform_dist(g);

    // find a target bin by using a binary search
    int64_t begin_pos = 0;
    int64_t end_pos = zipf_cdf_.size() - 1;
    while (begin_pos < end_pos) {
      auto pos = (begin_pos + end_pos) >> 1UL;  // NOLINT
      const auto cdf_val = zipf_cdf_.at(pos);
      if (target_prob < cdf_val) {
        end_pos = pos - 1;
      } else if (target_prob > cdf_val) {
        begin_pos = pos + 1;
      } else {  // target_prob == cdf_val
        begin_pos = pos;
        break;
      }
    }
    if (target_prob > zipf_cdf_.at(begin_pos)) {
      ++begin_pos;
    }

    return min_ + static_cast<IntType>(begin_pos);
  }

 private:
  /*############################################################################
   * Internal utility functions
   *##########################################################################*/

  /**
   * @brief Compute CDF values for this Zipf distribution.
   */
  void UpdateCDF();

  /*############################################################################
   * Static assertions
   *##########################################################################*/

  // Assume the use of integer types.
  static_assert(std::is_same_v<IntType, uint32_t>     //
                || std::is_same_v<IntType, uint64_t>  //
                || std::is_same_v<IntType, int32_t>   //
                || std::is_same_v<IntType, int64_t>);

  /*############################################################################
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
  /*############################################################################
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
   * according to Zipf's law with a skew paramter `alpha`.
   *
   * @param min The minimum value to be generated.
   * @param max The maximum value to be generated.
   * @param alpha A skew parameter (zero means uniform distribution).
   */
  ApproxZipfDistribution(  //
      const IntType min,
      const IntType max,
      const double alpha);

  constexpr ApproxZipfDistribution(const ApproxZipfDistribution &) = default;
  constexpr ApproxZipfDistribution(ApproxZipfDistribution &&) noexcept = default;

  constexpr auto operator=(const ApproxZipfDistribution &obj)  //
      -> ApproxZipfDistribution & = default;
  constexpr auto operator=(ApproxZipfDistribution &&) noexcept  //
      -> ApproxZipfDistribution & = default;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~ApproxZipfDistribution() = default;

  /*############################################################################
   * Public getters
   *##########################################################################*/

  /**
   * @param id A target ID in [0, `bin_num`).
   * @return A CDF value of the given ID.
   */
  [[nodiscard]] constexpr auto
  GetCDF(                      //
      const IntType id) const  //
      -> double
  {
    if (id < static_cast<IntType>(kExactBinNum)) return zipf_cdf_.at(id);
    return GetHarmonicNum(id + 1) / denom_;
  }

  /*############################################################################
   * Public utility operators
   *##########################################################################*/

  /**
   * @param g A random value generator.
   * @return A random value according to Zipf's law.
   */
  template <class RandEngine>
  [[nodiscard]] auto
  operator()(               //
      RandEngine &g) const  //
      -> IntType
  {
    thread_local std::uniform_real_distribution<double> uniform_dist{0.0, 1.0};
    const auto target_prob = uniform_dist(g);

    // find a target bin by using a binary search
    int64_t begin_pos = 0;
    int64_t end_pos = n_ - 1;
    while (begin_pos < end_pos) {
      auto pos = (begin_pos + end_pos) >> 1UL;  // NOLINT
      const auto cdf_val = GetCDF(pos);
      if (target_prob < cdf_val) {
        end_pos = pos - 1;
      } else if (target_prob > cdf_val) {
        begin_pos = pos + 1;
      } else {  // target_prob == cdf_val
        begin_pos = pos;
        break;
      }
    }
    if (target_prob > GetCDF(begin_pos)) {
      ++begin_pos;
    }

    return min_ + static_cast<IntType>(begin_pos);
  }

 private:
  /*############################################################################
   * Internal constants
   *##########################################################################*/

  /// @brief The number of bins for approximation.
  static constexpr size_t kExactBinNum = 100;

  /*############################################################################
   * Internal utility functions
   *##########################################################################*/

  /**
   * @brief Compute CDF values for this Zipf distribution.
   */
  void UpdateCDF();

  /**
   * @param n The number of partial elements in the p-serires.
   * @return An approximate partial sum of the p-series.
   */
  [[nodiscard]] constexpr auto
  GetHarmonicNum(             //
      const IntType n) const  //
      -> double
  {
    if (pow_ == 0.0) return (1 + log(n) + log(n + 1)) * 0.5;          // NOLINT
    return (pow(n + 1, pow_) + pow(n, pow_) - 2) / (2 * pow_) + 0.5;  // NOLINT
  }

  /*############################################################################
   * Static assertions
   *##########################################################################*/

  // Assume the use of integer types.
  static_assert(std::is_same_v<IntType, uint32_t>     //
                || std::is_same_v<IntType, uint64_t>  //
                || std::is_same_v<IntType, int32_t>   //
                || std::is_same_v<IntType, int64_t>);

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief The minimum value to be generated.
  IntType min_{0};

  /// @brief The maximum value to be generated.
  IntType max_{0};

  /// @brief A skew parameter (zero means uniform distribution).
  double alpha_{0.0};

  /// @brief The number of bins in this Zipf distribution.
  IntType n_{1};

  /// @brief Equal to `1 - alpha_`.
  double pow_{1.0};

  /// @brief Equal to `GetHarmonicNum(n_)`.
  double denom_{1.0};

  /// @brief A cumulative distribution function according to Zipf's law.
  std::array<double, kExactBinNum> zipf_cdf_{};
};

}  // namespace dbgroup::random

#endif  // CPP_UTILITY_DBGROUP_RANDOM_ZIPF_HPP_
