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

// the correspnding header
#include "thread/epoch_manager.hpp"

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
class EpochManagerFixture : public ::testing::Test
{
 protected:
  /*############################################################################
   * Test setup/teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    epoch_manager_ = std::make_unique<EpochManager>();
  }

  void
  TearDown() override
  {
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  std::unique_ptr<EpochManager> epoch_manager_{};

  std::mutex mtx_{};
};

/*##############################################################################
 * Unit test definitions
 *############################################################################*/

TEST_F(EpochManagerFixture, ConstructorInitializeGlobalEpoch)
{
  EXPECT_EQ(EpochManager::kInitialEpoch, epoch_manager_->GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, ForwardGlobalEpochAfterConstructGetIncrementedEpoch)
{
  epoch_manager_->ForwardGlobalEpoch();

  EXPECT_EQ(EpochManager::kInitialEpoch + 1, epoch_manager_->GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, GetProtectedEpochWithoutEpochsGetCurrentEpoch)
{
  EXPECT_EQ(EpochManager::kInitialEpoch, epoch_manager_->GetMinEpoch());

  const auto &[ep_guard, protected_epochs] = epoch_manager_->GetProtectedEpochs();
  EXPECT_EQ(protected_epochs.size(), 1);
  EXPECT_EQ(protected_epochs.front(), EpochManager::kInitialEpoch);
}

TEST_F(EpochManagerFixture, GetProtectedEpochWithEnteredEpochGetEnteredEpoch)
{
  constexpr size_t kLoopNum = 1000;
  constexpr size_t kRepeatNum = 10;

  // forward global epoch
  std::thread forwarder{[&]() {
    for (size_t i = 0; i < kLoopNum; ++i) {
      std::this_thread::sleep_for(std::chrono::microseconds{1});
      epoch_manager_->ForwardGlobalEpoch();
    }
  }};

  // create entered epochs
  std::vector<std::thread> threads;
  {
    const std::unique_lock guard{mtx_};
    for (size_t i = 0; i < kRepeatNum; ++i) {
      threads.emplace_back([&]() {
        [[maybe_unused]] const auto &guard = epoch_manager_->CreateEpochGuard();
        const std::unique_lock lock{mtx_};
      });
    }

    forwarder.join();

    const auto &[ep_guard, protected_epochs] = epoch_manager_->GetProtectedEpochs();
    for (size_t i = 0; i < protected_epochs.size() - 1; ++i) {
      EXPECT_GT(protected_epochs.at(i), protected_epochs.at(i + 1));
    }
    EXPECT_EQ(protected_epochs.front(), epoch_manager_->GetCurrentEpoch());
    EXPECT_EQ(protected_epochs.back(), epoch_manager_->GetMinEpoch());
  }
  for (auto &&t : threads) {
    t.join();
  }
}

TEST_F(EpochManagerFixture, GetProtectedEpochWithLeavedEpochGetCurrentEpoch)
{
  constexpr size_t kLoopNum = 1000;
  constexpr size_t kRepeatNum = 10;

  // forward global epoch
  std::thread forwarder{[&]() {
    for (size_t i = 0; i < kLoopNum; ++i) {
      std::this_thread::sleep_for(std::chrono::microseconds{1});
      epoch_manager_->ForwardGlobalEpoch();
    }
  }};

  // create entered epochs
  std::vector<std::thread> threads;
  {
    const std::unique_lock guard{mtx_};
    for (size_t i = 0; i < kRepeatNum; ++i) {
      threads.emplace_back([&]() {
        {
          [[maybe_unused]] const auto &guard = epoch_manager_->CreateEpochGuard();
        }
        const std::unique_lock lock{mtx_};
      });
    }

    forwarder.join();

    const auto &[ep_guard, protected_epochs] = epoch_manager_->GetProtectedEpochs();
    EXPECT_EQ(protected_epochs.size(), 2);
    EXPECT_EQ(protected_epochs.front(), epoch_manager_->GetCurrentEpoch());
  }
  for (auto &&t : threads) {
    t.join();
  }
}

TEST_F(EpochManagerFixture, EpochGuardProtectProtectedEpochLists)
{
  constexpr size_t kLoopNum = 1000 * 100;
  constexpr size_t kRepeatNum = 10;

  // a flag for controling worker threads
  std::atomic_bool is_running{true};

  // forward global epoch
  std::thread forwarder{[&]() {
    for (size_t i = 0; i < kLoopNum; ++i) {
      std::this_thread::sleep_for(std::chrono::microseconds{1});
      epoch_manager_->ForwardGlobalEpoch();
    }
  }};

  // create entered epochs
  std::vector<std::thread> threads;
  {
    const std::unique_lock guard{mtx_};
    for (size_t i = 0; i < kRepeatNum; ++i) {
      threads.emplace_back([&]() {
        while (is_running) {
          const auto &[ep_guard, protected_epochs] = epoch_manager_->GetProtectedEpochs();
          for (size_t i = 0; i < protected_epochs.size() - 1; ++i) {
            EXPECT_GT(protected_epochs.at(i), protected_epochs.at(i + 1));
          }
        }

        const std::unique_lock lock{mtx_};
      });
    }

    forwarder.join();
    is_running = false;

    const auto &[ep_guard, protected_epochs] = epoch_manager_->GetProtectedEpochs();
    for (size_t i = 0; i < protected_epochs.size() - 1; ++i) {
      EXPECT_GT(protected_epochs.at(i), protected_epochs.at(i + 1));
    }
    EXPECT_EQ(protected_epochs.front(), epoch_manager_->GetCurrentEpoch());
    EXPECT_EQ(protected_epochs.back(), epoch_manager_->GetMinEpoch());
  }
  for (auto &&t : threads) {
    t.join();
  }
}

}  // namespace dbgroup::thread::test
