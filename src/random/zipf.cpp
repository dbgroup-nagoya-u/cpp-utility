/*
 * Copyright 2024 Database Group, Nagoya University
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

// corresponding header
#include "dbgroup/random/zipf.hpp"

// C++ standard libraries
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace dbgroup::random
{
namespace
{
/*############################################################################*
 * Local utilities
 *############################################################################*/

/**
 * @tparam IntType A class of generated random values.
 * @param min The minimum value to be generated.
 * @param max The maximum value to be generated.
 * @return The number of bins in [`min`, `max`].
 * @throw std::runtime_error if the number of bins cannot be represented.
 */
template <class IntType>
auto
GetBinNum(  //
    const IntType min,
    const IntType max)  //
    -> uint64_t
{
  using UInt = std::make_unsigned_t<IntType>;

  if (max < min) {
    throw std::runtime_error{"The maximum value must be greater than the minimum one."};
  }
  const auto range = static_cast<uint64_t>(static_cast<UInt>(max) - static_cast<UInt>(min));
  if (range == std::numeric_limits<uint64_t>::max()) {
    throw std::runtime_error{"The number of bins exceeds the range of uint64_t."};
  }
  return range + 1;
}

}  // namespace

/*############################################################################*
 * class ZipfDistribution
 *############################################################################*/

template <class IntType>
ZipfDistribution<IntType>::ZipfDistribution()
{
  UpdateCDF();
}

template <class IntType>
ZipfDistribution<IntType>::ZipfDistribution(  //
    const IntType min,
    const IntType max,
    const double alpha)
    : min_{min}
    , max_{max}
    , alpha_{alpha}
{
  UpdateCDF();
}

template <class IntType>
void
ZipfDistribution<IntType>::UpdateCDF()
{
  const auto bin_num = GetBinNum(min_, max_);
  if (bin_num <= 1) {
    zipf_cdf_ = {1.0};
    return;
  }

  // compute a base probability
  auto base_prob = 0.0;
  for (uint64_t i = 1; i <= bin_num; ++i) {
    base_prob += 1.0 / std::pow(i, alpha_);
  }
  base_prob = 1.0 / base_prob;

  // create a CDF according to Zipf's law
  zipf_cdf_.reserve(bin_num);
  zipf_cdf_.emplace_back(base_prob);
  for (uint64_t i = 1; i < bin_num; ++i) {
    zipf_cdf_.emplace_back(zipf_cdf_[i - 1] + base_prob / std::pow(i + 1, alpha_));
  }
  zipf_cdf_[bin_num - 1] = 1.0;
}

/*############################################################################*
 * class ApproxZipfDistribution
 *############################################################################*/

template <class IntType>
ApproxZipfDistribution<IntType>::ApproxZipfDistribution()
{
  UpdateCDF();
}

template <class IntType>
ApproxZipfDistribution<IntType>::ApproxZipfDistribution(  //
    const IntType min,
    const IntType max,
    const double alpha)
    : min_{min}
    , max_{max}
    , alpha_{alpha}
{
  UpdateCDF();
}

template <class IntType>
void
ApproxZipfDistribution<IntType>::UpdateCDF()
{
  n_ = GetBinNum(min_, max_);
  pow_ = 1.0 - alpha_;
  lower_pow_ = std::pow(kLowerBound, pow_);

  // compute the first bins exactly
  const auto exact_num = std::min<uint64_t>(n_, kExactBinNum);
  auto sum = 0.0;
  for (uint64_t i = 0; i < exact_num; ++i) {
    sum += 1.0 / std::pow(i + 1, alpha_);
    zipf_cdf_[i] = sum;
  }
  exact_sum_ = sum;

  // approximate the remaining bins by the midpoint rule
  total_ = sum;
  if (n_ > kExactBinNum) {
    total_ += Integral(static_cast<double>(n_) + 0.5);  // NOLINT
  }

  // normalize the exact CDF
  for (uint64_t i = 0; i < exact_num; ++i) {
    zipf_cdf_[i] /= total_;
  }
  if (n_ <= kExactBinNum) {
    zipf_cdf_[exact_num - 1] = 1.0;
  }
}

/*############################################################################*
 * Explicit instantiation definitions
 *############################################################################*/

template class ZipfDistribution<uint32_t>;
template class ZipfDistribution<uint64_t>;
template class ZipfDistribution<int32_t>;
template class ZipfDistribution<int64_t>;
template class ApproxZipfDistribution<uint32_t>;
template class ApproxZipfDistribution<uint64_t>;
template class ApproxZipfDistribution<int32_t>;
template class ApproxZipfDistribution<int64_t>;

}  // namespace dbgroup::random
