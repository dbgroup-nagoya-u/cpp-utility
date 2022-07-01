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

#include "lock/pessimistic_lock.hpp"

#include <thread>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::lock::test
{
class PessimisticLockFixture : public ::testing::Test
{
 protected:
  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifySharedLockWithMultiThread()
  {
    EXPECT_EQ(lock_.GetLock(), 0);

    LockSharedWithMultiThread();
    EXPECT_EQ(lock_.GetLock(), 2 * kThreadNum);

    UnlockSharedWithMultiThread();
    EXPECT_EQ(lock_.GetLock(), 0);
  }

  void
  VerifyLockWithSingleThread()
  {
    EXPECT_EQ(lock_.GetLock(), 0);

    lock_.Lock();
    EXPECT_EQ(lock_.GetLock(), 1);

    lock_.Unlock();
    EXPECT_EQ(lock_.GetLock(), 0);
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/
  void
  LockSharedWithMultiThread()
  {
    auto f = [&]() { lock_.LockShared(); };

    std::vector<std::thread> threads{};
    threads.reserve(kThreadNum);

    for (size_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(f);
    }

    for (auto &&t : threads) {
      t.join();
    }
  }

  void
  UnlockSharedWithMultiThread()
  {
    auto f = [&]() { lock_.UnlockShared(); };

    std::vector<std::thread> threads{};
    threads.reserve(kThreadNum);

    for (size_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(f);
    }

    for (auto &&t : threads) {
      t.join();
    }
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  PessimisticLock lock_{};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------*/

TEST_F(PessimisticLockFixture, SharedLockWithMultiThread) { VerifySharedLockWithMultiThread(); }

TEST_F(PessimisticLockFixture, LockWithSingleThread) { VerifyLockWithSingleThread(); }

}  // namespace dbgroup::lock::test
