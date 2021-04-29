// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

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

  /// a random generator
  std::mt19937_64 random_engine_;

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
      const double alpha,
      const size_t seed = std::random_device{}())
  {
    SetZipfParameters(bin_num, alpha);
    SetRandomSeed(seed);
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
  size_t
  operator()()
  {
    const auto target_prob = prob_generator_(random_engine_);

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

  /**
   * @brief Set a new random seed.
   *
   * @param seed a random seed
   */
  void
  SetRandomSeed(const size_t seed)
  {
    random_engine_ = std::mt19937_64{seed};
  }
};

}  // namespace dbgroup::random::zipf
