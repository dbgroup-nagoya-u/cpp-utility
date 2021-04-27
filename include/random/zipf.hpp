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
class ZipfGenerator
{
 private:
  /// a cumulative distribution function according to Zipf's law
  std::vector<double> zipf_cdf_;

  /// the number of bins
  size_t bin_num_;

  /// a skew parameter (zero means uniform)
  double alpha_;

  /// a random seed
  size_t seed_;

  /// a random generator
  std::mt19937_64 random_engine_;

  std::uniform_real_distribution<double> prob_generator_{0, 1};

 public:
  constexpr ZipfGenerator() : bin_num_{0}, alpha_{0}, seed_{0} {}

  ~ZipfGenerator() = default;

  void
  Update(  //
      const size_t bin_num,
      const double alpha)
  {
    assert(bin_num > 0);
    assert(alpha >= 0);

    // update parameters
    bin_num_ = bin_num;
    alpha_ = alpha;

    // compute a base probability
    double base_prob = 0;
    for (size_t i = 1; i < bin_num_ + 1; ++i) {
      base_prob += 1.0 / pow(i, alpha_);
    }
    base_prob = 1.0 / base_prob;

    // create a CDF according to Zipf's law
    zipf_cdf_.reserve(bin_num_);
    zipf_cdf_.emplace_back(base_prob);
    for (size_t i = 1; i < bin_num_ - 1; ++i) {
      zipf_cdf_.emplace_back(zipf_cdf_[i - 1] + base_prob / pow(i + 1, alpha_));
    }
    zipf_cdf_.emplace_back(1);
  }

  void
  SetRandomSeed(const size_t seed)
  {
    seed_ = seed;
    random_engine_ = std::mt19937_64{seed_};
  }

  size_t
  Zipf()
  {
    assert(bin_num_ > 0);

    const auto target_prob = prob_generator_(random_engine_);

    // find a target bin by using a binary search
    int64_t begin_index = 0, end_index = bin_num_, index = end_index / 2;
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
};

}  // namespace dbgroup::random::zipf
