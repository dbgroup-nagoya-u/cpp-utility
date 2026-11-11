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
#include <stdexcept>
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
   * @param bin_id A target bin ID in [0, the number of bins).
   * @return A CDF value of the given bin.
   * @throw std::out_of_range if `bin_id` is not less than the number of bins.
   */
  [[nodiscard]]
  constexpr auto
  GetCDF(                           //
      const uint64_t bin_id) const  //
      -> double
  {
    return zipf_cdf_.at(bin_id);
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
 * This class computes the first `kExactBinNum` bins exactly and approximates
 * the remaining ones by integrating `x^(-alpha)` with the midpoint rule.
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
   * @param bin_id A target bin ID in [0, the number of bins).
   * @return A CDF value of the given bin.
   * @throw std::out_of_range if `bin_id` is not less than the number of bins.
   */
  [[nodiscard]]
  auto
  GetCDF(                           //
      const uint64_t bin_id) const  //
      -> double
  {
    if (bin_id >= n_) {
      throw std::out_of_range{"The bin ID must be less than the number of bins."};
    }
    if (bin_id < std::min<uint64_t>(n_, kExactBinNum)) return zipf_cdf_[bin_id];
    if (bin_id == n_ - 1) return 1.0;
    return (exact_sum_ + Integral(static_cast<double>(bin_id) + 1.5)) / total_;  // NOLINT
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
    const auto exact_num = std::min<uint64_t>(n_, kExactBinNum);
    if (p <= zipf_cdf_[exact_num - 1]) {  // use the exact CDF
      uint64_t id = 0;
      while (zipf_cdf_[id] < p) {
        ++id;  // the last entry is not less than p
      }
      return ToValue(id);
    }

    // solve `kLowerBound <= x` s.t. `Integral(x) = p * total_ - exact_sum_`
    const auto y = p * total_ - exact_sum_;
    const auto x = pow_ == 0.0 ? kLowerBound * std::exp(y)
                               : kLowerBound * std::exp(std::log1p(pow_ * y / lower_pow_) / pow_);
    const auto id = std::ceil(x - 1.5);  // CDF(id) = Integral(id + 1.5) / total_
    if (!(id < static_cast<double>(n_ - 1))) return ToValue(n_ - 1);
    return ToValue(std::max<uint64_t>(static_cast<uint64_t>(id), kExactBinNum));
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

  /// @brief The number of bins to compute exactly.
  static constexpr size_t kExactBinNum = 100;

  /// @brief The lower bound of the integral for approximate bins.
  static constexpr double kLowerBound = kExactBinNum + 0.5;

  /*##########################################################################*
   * Internal utility functions
   *##########################################################################*/

  /**
   * @brief Compute CDF values for this Zipf distribution.
   */
  void UpdateCDF();

  /**
   * @param x The upper bound of the integral.
   * @return The integral of `t^(-alpha)` over [`kLowerBound`, `x`].
   */
  [[nodiscard]]
  auto
  Integral(                  //
      const double x) const  //
      -> double
  {
    if (pow_ == 0.0) return std::log(x / kLowerBound);
    return lower_pow_ * std::expm1(pow_ * std::log(x / kLowerBound)) / pow_;
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

  /// @brief Equal to `kLowerBound^pow_`.
  double lower_pow_{kLowerBound};

  /// @brief The sum of `i^(-alpha)` for exactly computed bins.
  double exact_sum_{1.0};

  /// @brief The (approximate) sum of `i^(-alpha)` for all the bins.
  double total_{1.0};

  /// @brief CDF values of exactly computed bins.
  std::array<double, kExactBinNum> zipf_cdf_{};
};

}  // namespace dbgroup::random

#endif  // CPP_UTILITY_DBGROUP_RANDOM_ZIPF_HPP_
