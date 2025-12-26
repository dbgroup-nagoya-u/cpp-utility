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

// the corresponding header
#include "dbgroup/thread/epoch_manager.hpp"

// C++ standard libraries
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

// external libraries
#include "gtest/gtest.h"

namespace dbgroup::thread::test
{
/*############################################################################*
 * Global constants
 *############################################################################*/

constexpr size_t kEpochInterval = 1;
constexpr std::chrono::milliseconds kSleepTime{kEpochInterval};
constexpr size_t kLoopNum = 100;

/*############################################################################*
 * Fixture definitions
 *############################################################################*/

class EpochManagerFixture : public ::testing::Test
{
 protected:
  /*##########################################################################*
   * Test setup/teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    epoch_manager_ = std::make_unique<EpochManager>(kEpochInterval);
  }

  void
  TearDown() override
  {
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  std::unique_ptr<EpochManager> epoch_manager_{};
};

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TEST_F(                   //
    EpochManagerFixture,  //
    GlobalEpochWithoutGuardProgressOverTime)
{
  auto prev = epoch_manager_->GetCurrentEpoch();
  for (size_t i = 0; i < kLoopNum; ++i) {
    Serial32_t cur;
    while (true) {
      cur = epoch_manager_->GetCurrentEpoch();
      if (cur != prev) break;
      std::this_thread::sleep_for(kSleepTime);
    }
    EXPECT_GT(cur, prev);
    prev = cur;
  }
}

TEST_F(                   //
    EpochManagerFixture,  //
    GlobalEpochWithGuardProgressOverTime)
{
  [[maybe_unused]] const auto &grd = epoch_manager_->CreateEpochGuard();
  auto prev = epoch_manager_->GetCurrentEpoch();
  for (size_t i = 0; i < kLoopNum; ++i) {
    Serial32_t cur;
    while (true) {
      cur = epoch_manager_->GetCurrentEpoch();
      if (cur != prev) break;
      std::this_thread::sleep_for(kSleepTime);
    }
    EXPECT_GT(cur, prev);
    prev = cur;
  }
}

TEST_F(                   //
    EpochManagerFixture,  //
    MinEpochWithoutGuardProgressOverTime)
{
  auto prev = epoch_manager_->GetMinEpoch();
  for (size_t i = 0; i < kLoopNum; ++i) {
    Serial32_t cur;
    while (true) {
      cur = epoch_manager_->GetMinEpoch();
      if (cur != prev) break;
      std::this_thread::sleep_for(kSleepTime);
    }
    EXPECT_GT(cur, prev);
    prev = cur;
  }
}

TEST_F(                   //
    EpochManagerFixture,  //
    MinEpochWithGuardRemainUnchangedOverTime)
{
  [[maybe_unused]] const auto &grd = epoch_manager_->CreateEpochGuard();
  auto prev = epoch_manager_->GetMinEpoch();
  for (size_t i = 0; i < kLoopNum; ++i) {
    const auto cur = epoch_manager_->GetMinEpoch();
    EXPECT_EQ(cur, prev);
    std::this_thread::sleep_for(kSleepTime);
  }
}

}  // namespace dbgroup::thread::test
