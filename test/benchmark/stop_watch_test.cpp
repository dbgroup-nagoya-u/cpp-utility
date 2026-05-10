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

}  // namespace dbgroup::benchmark::test
