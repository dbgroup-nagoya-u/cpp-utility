/*
 * Copyright 2026 Database Group, Nagoya University
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

#include "dbgroup/benchmark/stop_watch.hpp"

// C++ standard libraries
#include <chrono>
#include <cstddef>
#include <thread>

// external libraries
#include "gtest/gtest.h"

namespace dbgroup::benchmark::test
{
/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TEST(  //
    StopWatchTest,
    QuantileQueryReturnReasonableOnes)
{
  constexpr size_t kLoopNum = 1E6;
  StopWatch sw{};
  for (size_t i = 0; i < kLoopNum; ++i) {
    sw.Start();
    std::this_thread::yield();
    sw.Stop();
  }

  // NOLINTBEGIN
  EXPECT_EQ(sw.GetExecNum(), kLoopNum);
  auto prev = sw.Quantile(0);
  for (size_t i = 1; i <= 100; ++i) {
    const auto p = static_cast<double>(i) / 100.0;
    const auto cur = sw.Quantile(p);
    EXPECT_GE(cur, prev);
    prev = cur;
  }
  EXPECT_GE(sw.GetExecTime(), prev);
  // NOLINTEND
}

TEST(  //
    StopWatchTest,
    QuantileQueryReturnLatencyPerItem)
{
  constexpr size_t kLoopNum = 100;
  constexpr size_t kItemNum = 100;
  constexpr auto kSleepTime = std::chrono::milliseconds{1};
  StopWatch sw{};
  for (size_t i = 0; i < kLoopNum; ++i) {
    sw.Start();
    std::this_thread::sleep_for(kSleepTime);
    sw.Stop(kItemNum);
  }

  // NOLINTBEGIN
  constexpr size_t kSleepNS = std::chrono::nanoseconds{kSleepTime}.count();
  EXPECT_EQ(sw.GetExecNum(), kLoopNum * kItemNum);
  const auto median = sw.Quantile(0.5);
  EXPECT_GE(median, kSleepNS / kItemNum * 0.99);  // allow the relative error of bins
  EXPECT_LT(median, kSleepNS);
  EXPECT_LE(sw.Quantile(0), median * 1.01);
  EXPECT_GE(sw.Quantile(1.0), median * 0.99);
  // NOLINTEND
}

TEST(  //
    StopWatchTest,
    ZeroItemsAreRegardedAsOne)
{
  StopWatch sw{};
  sw.Start();
  std::this_thread::sleep_for(std::chrono::microseconds{10});  // NOLINT
  sw.Stop(0);
  EXPECT_EQ(sw.GetExecNum(), 1);
  EXPECT_GT(sw.Quantile(0.5), 0);  // NOLINT
}

TEST(  //
    StopWatchTest,
    QuantileQueryReturnZeroWithoutMeasurements)
{
  const StopWatch sw{};
  EXPECT_FALSE(sw);
  EXPECT_EQ(sw.Quantile(0), 0);
  EXPECT_EQ(sw.Quantile(0.5), 0);  // NOLINT
  EXPECT_EQ(sw.Quantile(1.0), 0);
}

}  // namespace dbgroup::benchmark::test
