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

#pragma once

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

namespace dbgroup::random::zipf
{
/**
 * @brief A class to generate random values according to Zipf's law.
 *
 */
class ZipfGenerator
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a cumulative distribution function according to Zipf's law
  std::vector<double> zipf_cdf_;

  /// the number of bins
  size_t bin_num_;

  /// a probability generator with range [0, 1.0]
  std::uniform_real_distribution<double> prob_generator_{0, 1};

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  ZipfGenerator() : bin_num_{0} { zipf_cdf_.emplace_back(1); }

  /**
   * @brief Construct a new ZipfGenerator with given parameters.
   *
   * The generator will generate random values within [0, `bin_num`) according to Zipf's
   * law with a skew paramter `alpha`.
   *
   * @param bin_num the total number of bins
   * @param alpha a skew parameter (zero means uniform distribution)
   * @param seed a random seed for reproducibility
   */
  ZipfGenerator(  //
      const size_t bin_num,
      const double alpha)
  {
    SetZipfParameters(bin_num, alpha);
  }

  ~ZipfGenerator() = default;

  ZipfGenerator(const ZipfGenerator &) = default;
  ZipfGenerator &operator=(const ZipfGenerator &obj) = default;
  ZipfGenerator(ZipfGenerator &&) = default;
  ZipfGenerator &operator=(ZipfGenerator &&) = default;

  /*################################################################################################
   * Public utility operators
   *##############################################################################################*/

  /**
   * @return size_t a random value according to Zipf's law.
   */
  template <class RandEngine>
  size_t
  operator()(RandEngine &g)
  {
    const auto target_prob = prob_generator_(g);

    // find a target bin by using a binary search
    int64_t begin_index = 0, end_index = bin_num_ - 1, index = end_index / 2;
    while (begin_index < end_index) {
      if (target_prob < zipf_cdf_[index]) {
        end_index = index - 1;
      } else if (target_prob > zipf_cdf_[index]) {
        begin_index = index + 1;
      } else {  // target_prob == zipf_cdf_[index]
        return index;
      }
      index = (begin_index + end_index) / 2;
    }

    return (target_prob <= zipf_cdf_[index]) ? index : index + 1;
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @brief Set new parameters for Zipf's law.
   *
   * This function recreates a CDF according to Zipf's law by using new paramters, and
   * it may take some time.
   *
   * @param bin_num the total number of bins
   * @param alpha skew parameter (zero means uniform distribution)
   */
  void
  SetZipfParameters(  //
      const size_t bin_num,
      const double alpha)
  {
    assert(bin_num > 0);
    assert(alpha >= 0);

    // update parameters
    bin_num_ = bin_num;

    // compute a base probability
    double base_prob = 0;
    for (size_t i = 1; i < bin_num_ + 1; ++i) {
      base_prob += 1.0 / pow(i, alpha);
    }
    base_prob = 1.0 / base_prob;

    // create a CDF according to Zipf's law
    zipf_cdf_.clear();
    zipf_cdf_.reserve(bin_num_);
    zipf_cdf_.emplace_back(base_prob);
    for (size_t i = 1; i < bin_num_; ++i) {
      const auto ith_prob = zipf_cdf_[i - 1] + base_prob / pow(i + 1, alpha);
      zipf_cdf_.emplace_back(ith_prob);
    }
    zipf_cdf_[bin_num_ - 1] = 1.0;
  }
};

}  // namespace dbgroup::random::zipf
