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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace dbgroup::random
{
/*##############################################################################
 * class ZipfDistribution
 *############################################################################*/

template <class IntType>
ZipfDistribution<IntType>::ZipfDistribution()
{
  UpdateCDF();
};

template <class IntType>
ZipfDistribution<IntType>::ZipfDistribution(  //
    const IntType min,
    const IntType max,
    const double alpha)
    : min_{min}, max_{max}, alpha_{alpha}
{
  if (max < min) {
    throw std::runtime_error{"The maximum value must be greater than the minimum one."};
  }
  UpdateCDF();
}

template <class IntType>
void
ZipfDistribution<IntType>::UpdateCDF()
{
  const auto bin_num = max_ - min_ + 1;
  if (bin_num <= 1) {
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
    const auto ith_prob = zipf_cdf_.at(i - 1) + base_prob / pow(i + 1, alpha_);
    zipf_cdf_.emplace_back(ith_prob);
  }
  zipf_cdf_.at(bin_num - 1) = 1.0;
}

/*##############################################################################
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
    : min_{min},
      max_{max},
      alpha_{alpha},
      n_{max_ - min_ + static_cast<IntType>(1)},
      pow_{1.0 - alpha_},
      denom_{GetHarmonicNum(n_)}
{
  if (max < min) {
    throw std::runtime_error{"The maximum value must be greater than the minimum one."};
  }
  UpdateCDF();
}

template <class IntType>
void
ApproxZipfDistribution<IntType>::UpdateCDF()
{
  if (n_ <= 1) {
    zipf_cdf_ = {1.0};
    return;
  }

  if (n_ <= static_cast<IntType>(kExactBinNum)) {
    // compute a base probability exactly
    auto base_prob = 0.0;
    for (IntType i = 1; i < n_ + 1; ++i) {
      base_prob += 1.0 / pow(i, alpha_);
    }
    base_prob = 1.0 / base_prob;

    // create an exact CDF according to Zipf's law
    zipf_cdf_.at(0) = base_prob;
    for (IntType i = 1; i < static_cast<IntType>(kExactBinNum); ++i) {
      const auto ith_prob = zipf_cdf_.at(i - 1) + base_prob / pow(i + 1, alpha_);
      zipf_cdf_.at(i) = ith_prob;
    }
    zipf_cdf_.at(n_ - 1) = 1.0;
  } else {
    // compute a base probability approximately
    constexpr size_t kSkipSize = 100;
    auto base_prob = 0.0;
    IntType i = 1;
    while (i < static_cast<IntType>(kExactBinNum) + 1) {  // compute exact values
      base_prob += 1.0 / pow(i++, alpha_);
    }
    while (i < n_ + 1) {  // compute approximate values
      const auto low = 1.0 / pow(i, alpha_);
      i += kSkipSize;
      const auto high = 1.0 / pow(i, alpha_);
      base_prob += (low + high) * kSkipSize / 2;
    }
    base_prob = 1.0 / base_prob;

    // create a CDF according to Zipf's law
    zipf_cdf_.at(0) = base_prob;
    for (IntType j = 1; j < static_cast<IntType>(kExactBinNum); ++j) {
      const auto ith_prob = zipf_cdf_.at(j - 1) + base_prob / pow(j + 1, alpha_);
      zipf_cdf_.at(j) = ith_prob;
    }
  }
}

/*##############################################################################
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
