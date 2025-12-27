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
#include "dbgroup/thread/id_manager.hpp"

// C++ standard libraries
#include <algorithm>
#include <array>
#include <chrono>
#include <mutex>
#include <thread>

// external sources
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

namespace dbgroup::thread::test
{
/*############################################################################*
 * Global constants
 *############################################################################*/

/*############################################################################*
 * Fixture declaration
 *############################################################################*/

class IDManagerFixture : public ::testing::Test
{
 protected:
  /*##########################################################################*
   * Constants
   *##########################################################################*/

  /*##########################################################################*
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    IDManager::SetMaxThreadNum(kMaxThreadCapacity);
  }

  void
  TearDown() override
  {
  }

  /*##########################################################################*
   * Utilities for verification
   *##########################################################################*/

  static void
  ReserveAllThreadIDs()
  {
    std::mutex mtx{};
    std::atomic_size_t cnt{0};
    std::array<bool, kMaxThreadCapacity> reserved{};
    reserved.fill(false);
    std::vector<std::thread> threads{};
    threads.reserve(kMaxThreadCapacity);

    auto f = [&] {
      const auto id = IDManager::GetThreadID();
      reserved.at(id) = true;
      cnt.fetch_add(1, std::memory_order_release);
      std::lock_guard guard{mtx};
    };

    {
      std::lock_guard guard{mtx};
      for (size_t i = 0; i < kMaxThreadCapacity; ++i) {
        threads.emplace_back(f);
      }
      while (cnt.load(std::memory_order_acquire) < kMaxThreadCapacity) {
        std::this_thread::sleep_for(std::chrono::microseconds{1});
      }
    }
    for (auto &&t : threads) {
      t.join();
    }

    for (auto &&flag : reserved) {
      EXPECT_TRUE(flag);
    }
  }
};

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TEST_F(IDManagerFixture, GetThreadIDReturnsUniqueIDs)
{  //
  ReserveAllThreadIDs();
}

TEST_F(IDManagerFixture, ExitedThreadsReleaseTheirIDs)
{  //
  ReserveAllThreadIDs();
  ReserveAllThreadIDs();
}

TEST_F(IDManagerFixture, ThreadHeartBeatsShowProperBoolValues)
{
  std::mutex mtx{};
  std::atomic_size_t cnt{0};
  std::array<std::weak_ptr<size_t>, kMaxThreadCapacity> hbs{};
  std::vector<std::thread> threads{};
  threads.reserve(kMaxThreadCapacity);

  auto f = [&] {
    const auto id = IDManager::GetThreadID();
    hbs.at(id) = IDManager::GetHeartBeat();
    cnt.fetch_add(1, std::memory_order_release);
    std::lock_guard guard{mtx};
  };

  {
    std::lock_guard guard{mtx};
    for (size_t i = 0; i < kMaxThreadCapacity; ++i) {
      threads.emplace_back(f);
    }
    while (cnt.load(std::memory_order_acquire) < kMaxThreadCapacity) {
      std::this_thread::sleep_for(std::chrono::microseconds{1});
    }
    for (auto &&flag : hbs) {
      EXPECT_FALSE(flag.expired());
    }
  }
  for (auto &&t : threads) {
    t.join();
  }

  for (auto &&flag : hbs) {
    EXPECT_TRUE(flag.expired());
  }
}

}  // namespace dbgroup::thread::test
