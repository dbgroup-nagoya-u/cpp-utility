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

#ifndef CPP_UTILITY_RANDOM_ZIPF_HPP
#define CPP_UTILITY_RANDOM_ZIPF_HPP

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <random>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace dbgroup::random
{
/**
 * @brief A class to generate random values according to Zipf's law.
 *
 */
template <class IntType>
class ZipfDistribution
{
  static_assert(std::is_integral_v<IntType>);

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct an empty distribution.
   *
   * This always returns zero.
   */
  ZipfDistribution() { UpdateCDF(); };

  /**
   * @brief Construct a new Zipf distribution with given parameters.
   *
   * This distribution will generate random values within [`min`, `max`) according to
   * Zipf's law with a skew paramter `alpha`.
   *
   * @param min the minimum value to be generated.
   * @param max the maximum value to be generated.
   * @param alpha a skew parameter (zero means uniform distribution).
   */
  ZipfDistribution(  //
      const IntType min,
      const IntType max,
      const double alpha)
      : min_{min}, max_{max}, alpha_{alpha}
  {
    if (max <= min) {
      std::string err_msg = "ERROR: the maximum value must be greater than the minimum one.";
      throw std::runtime_error{err_msg};
    }

    UpdateCDF();
  }

  ZipfDistribution(const ZipfDistribution &) = default;
  auto operator=(const ZipfDistribution &obj) -> ZipfDistribution & = default;
  ZipfDistribution(ZipfDistribution &&) noexcept = default;
  auto operator=(ZipfDistribution &&) noexcept -> ZipfDistribution & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~ZipfDistribution() = default;

  /*####################################################################################
   * Public utility operators
   *##################################################################################*/

  /**
   * @return a random value according to Zipf's law.
   */
  template <class RandEngine>
  auto
  operator()(RandEngine &g) const  //
      -> IntType
  {
    thread_local std::uniform_real_distribution<double> uniform_dist{0.0, 1.0};
    const auto target_prob = uniform_dist(g);

    // find a target bin by using a binary search
    int64_t begin_pos = 0;
    int64_t end_pos = zipf_cdf_.size() - 1;
    while (begin_pos < end_pos) {
      auto pos = (begin_pos + end_pos) >> 1UL;  // NOLINT
      if (target_prob < zipf_cdf_[pos]) {
        end_pos = pos - 1;
      } else if (target_prob > zipf_cdf_[pos]) {
        begin_pos = pos + 1;
      } else {  // target_prob == zipf_cdf_[pos]
        begin_pos = pos;
        break;
      }
    }
    if (target_prob > zipf_cdf_[begin_pos]) {
      ++begin_pos;
    }

    return min_ + static_cast<IntType>(begin_pos);
  }

 private:
  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Set new parameters for Zipf's law.
   *
   * This function recreates a CDF according to Zipf's law by using new paramters, and
   * it may take some time.
   */
  void
  UpdateCDF()
  {
    const auto bin_num = max_ - min_;
    if (bin_num == 1) {
      zipf_cdf_ = {1.0};
      return;
    }

    // compute a base probability
    auto base_prob = 0.0;
    for (IntType i = 1; i < bin_num + 1; ++i) {
      base_prob += 1.0 / pow(i, alpha_);
    }
    base_prob = 1.0 / base_prob;

    // create a CDF according to Zipf's law
    zipf_cdf_.reserve(bin_num);
    zipf_cdf_.emplace_back(base_prob);
    for (IntType i = 1; i < bin_num; ++i) {
      const auto ith_prob = zipf_cdf_[i - 1] + base_prob / pow(i + 1, alpha_);  // NOLINT
      zipf_cdf_.emplace_back(ith_prob);
    }
    zipf_cdf_[bin_num - 1] = 1.0;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// the minimum value to be generated.
  IntType min_{0};

  /// the maximum value to be generated.
  IntType max_{1};

  /// a skew parameter (zero means uniform distribution).
  double alpha_{0.0};

  /// a cumulative distribution function according to Zipf's law.
  std::vector<double> zipf_cdf_{};
};

}  // namespace dbgroup::random

#endif  // CPP_UTILITY_RANDOM_ZIPF_HPP
