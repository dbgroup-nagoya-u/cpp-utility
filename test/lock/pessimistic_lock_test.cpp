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

#include "dbgroup/lock/pessimistic_lock.hpp"

// C++ standard libraries
#include <chrono>
#include <future>
#include <thread>
#include <variant>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

namespace dbgroup::lock::test
{
/*##############################################################################
 * Global constants
 *############################################################################*/

constexpr bool kExpectSucceed = true;
constexpr bool kExpectFail = false;
constexpr size_t kThreadNumForLockS = 1E2;
constexpr size_t kWriteNumPerThread = 1E5;
constexpr std::chrono::milliseconds kWaitTimeMill{100};

/*##############################################################################
 * Fixture definition
 *############################################################################*/

class PessimisticLockFixture : public ::testing::Test
{
 protected:
  /*############################################################################
   * Types
   *##########################################################################*/

  using Guard = std::
      variant<int, PessimisticLock::SGuard, PessimisticLock::SIXGuard, PessimisticLock::XGuard>;

  /*############################################################################
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*############################################################################
   * Functions for verification
   *##########################################################################*/

  void
  VerifyLockSWith(  //
      const LockType lock_type,
      const bool expected_rc)
  {
    {
      [[maybe_unused]] const auto &guard = GetLock(lock_type);
      TryLock(kSLock, expected_rc);
    }
    t_.join();
  }

  void
  VerifyLockSIXWith(  //
      const LockType lock_type,
      const bool expected_rc)
  {
    {
      [[maybe_unused]] const auto &guard = GetLock(lock_type);
      TryLock(kSIXLock, expected_rc);
    }
    t_.join();
  }

  void
  VerifyLockXWith(  //
      const LockType lock_type,
      const bool expected_rc)
  {
    {
      [[maybe_unused]] const auto &guard = GetLock(lock_type);
      TryLock(kXLock, expected_rc);
    }
    t_.join();
  }

  void
  VerifyDowngradeToSIX(  //
      const LockType lock_type,
      const bool expected_rc)
  {
    {
      [[maybe_unused]] const auto &six_guard = lock_.LockX().DowngradeToSIX();
      TryLock(lock_type, expected_rc);
    }
    t_.join();
  }

  void
  VerifyUpgradeToXWith(  //
      const LockType lock_type,
      const bool expected_rc)
  {
    {
      [[maybe_unused]] const auto &guard = GetLock(lock_type);
      TryUpgrade(lock_.LockSIX(), expected_rc);
    }
    t_.join();
  }

  void
  VerifyLockSWithMultiThread()
  {
    // create threads to get/release a shared lock
    std::vector<std::thread> threads{};
    threads.reserve(kThreadNumForLockS);
    for (size_t i = 0; i < kThreadNumForLockS; ++i) {
      threads.emplace_back([this]() { auto &&s_guard = lock_.LockS(); });
    }

    // check the counter of shared locks is correctly managed
    for (auto &&t : threads) {
      t.join();
    }
    TryLock(kXLock, kExpectSucceed);

    t_.join();
  }

  void
  VerifyLockXWithMultiThread()
  {
    std::vector<std::thread> threads{};
    threads.reserve(kThreadNum);

    {  // create a shared lock to prevent a counter from modifying
      auto &&s_guard = lock_.LockS();

      // create incrementor threads
      for (size_t i = 0; i < kThreadNum; ++i) {
        threads.emplace_back([this]() {
          for (size_t i = 0; i < kWriteNumPerThread; i++) {
            auto &&x_guard = lock_.LockX();
            ++counter_;
          }
        });
      }

      // after short sleep, check that the counter has not incremented
      std::this_thread::sleep_for(kWaitTimeMill);
      ASSERT_EQ(counter_, 0);
    }

    // release the shared lock, and then wait for the increment workers
    for (auto &&t : threads) {
      t.join();
    }

    // check the counter
    auto &&s_guard = lock_.LockS();
    ASSERT_EQ(counter_, kThreadNum * kWriteNumPerThread);
  }

  /*############################################################################
   * Public utility functions
   *##########################################################################*/

  auto
  GetLock(                       //
      const LockType lock_type)  //
      -> Guard
  {
    switch (lock_type) {
      case kSLock: {
        auto &&guard = lock_.LockS();
        EXPECT_TRUE(guard);
        return Guard{std::move(guard)};
      }
      case kSIXLock: {
        auto &&guard = lock_.LockSIX();
        EXPECT_TRUE(guard);
        return Guard{std::move(guard)};
      }
      case kXLock: {
        auto &&guard = lock_.LockX();
        EXPECT_TRUE(guard);
        return Guard{std::move(guard)};
      }
      case kFree:
      default:
        break;
    }
    return Guard{};
  }

  void
  LockWorker(  //
      const LockType lock_type,
      std::promise<void> p)
  {
    [[maybe_unused]] const auto &guard = GetLock(lock_type);
    p.set_value();
  }

  void
  TryLock(  //
      const LockType lock_type,
      const bool expect_success)
  {
    // try to get an exclusive lock by another thread
    std::promise<void> p{};
    auto &&f = p.get_future();
    t_ = std::thread{&PessimisticLockFixture::LockWorker, this, lock_type, std::move(p)};

    // after short sleep, give up on acquiring the lock
    const auto rc = f.wait_for(kWaitTimeMill);

    // verify status to check locking is succeeded
    if (expect_success) {
      ASSERT_EQ(rc, std::future_status::ready);
    } else {
      ASSERT_EQ(rc, std::future_status::timeout);
    }
  }

  void
  TryUpgrade(  //
      PessimisticLock::SIXGuard six_guard,
      const bool expect_success)
  {
    auto upgrade_worker = [](PessimisticLock::SIXGuard six_guard, std::promise<void> p) -> void {
      [[maybe_unused]] const auto &x_guard = six_guard.UpgradeToX();
      p.set_value();
    };

    // try to get an exclusive lock by another thread
    std::promise<void> p{};
    auto &&f = p.get_future();
    t_ = std::thread{upgrade_worker, std::move(six_guard), std::move(p)};

    // after short sleep, give up on acquiring the lock
    const auto rc = f.wait_for(kWaitTimeMill);

    // verify status to check locking is succeeded
    if (expect_success) {
      ASSERT_EQ(rc, std::future_status::ready);
    } else {
      ASSERT_EQ(rc, std::future_status::timeout);
    }
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  PessimisticLock lock_{};

  size_t counter_{0};

  std::thread t_{};
};

/*##############################################################################
 * Unit test definitions
 *############################################################################*/

/*----------------------------------------------------------------------------*
 * Shared lock tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    PessimisticLockFixture,
    LockSWithoutLocksSucceed)
{
  VerifyLockSWith(kFree, kExpectSucceed);
}

TEST_F(  //
    PessimisticLockFixture,
    LockSAfterSLockSucceed)
{
  VerifyLockSWith(kSLock, kExpectSucceed);
}

TEST_F(  //
    PessimisticLockFixture,
    LockSAfterSIXLockSucceed)
{
  VerifyLockSWith(kSIXLock, kExpectSucceed);
}

TEST_F(  //
    PessimisticLockFixture,
    LockSAfterXLockNeedWait)
{
  VerifyLockSWith(kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Exclusive lock tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    PessimisticLockFixture,
    LockXWithoutLocksSucceed)
{
  VerifyLockXWith(kFree, kExpectSucceed);
}

TEST_F(  //
    PessimisticLockFixture,
    LockXAfterSLockNeedWait)
{
  VerifyLockXWith(kSLock, kExpectFail);
}

TEST_F(  //
    PessimisticLockFixture,
    LockXAfterSIXLockNeedWait)
{
  VerifyLockXWith(kSIXLock, kExpectFail);
}

TEST_F(  //
    PessimisticLockFixture,
    LockXAfterXLockNeedWait)
{
  VerifyLockXWith(kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Shared-with-intent-exclusive lock tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    PessimisticLockFixture,
    LockSIXWithoutLocksSucceed)
{
  VerifyLockSIXWith(kFree, kExpectSucceed);
}

TEST_F(  //
    PessimisticLockFixture,
    LockSIXAfterSLockSucceed)
{
  VerifyLockSIXWith(kSLock, kExpectSucceed);
}

TEST_F(  //
    PessimisticLockFixture,
    LockSIXAfterSIXLockNeedWait)
{
  VerifyLockSIXWith(kSIXLock, kExpectFail);
}

TEST_F(  //
    PessimisticLockFixture,
    LockSIXAfterXLockNeedWait)
{
  VerifyLockSIXWith(kXLock, kExpectFail);
}

TEST_F(  //
    PessimisticLockFixture,
    LockSAfterDowngradeToSIXSucceed)
{
  VerifyDowngradeToSIX(kSLock, kExpectSucceed);
}

TEST_F(  //
    PessimisticLockFixture,
    LockSIXAfterDowngradeToSIXNeedWait)
{
  VerifyDowngradeToSIX(kSIXLock, kExpectFail);
}

TEST_F(  //
    PessimisticLockFixture,
    LockXAfterDowngradeToSIXNeedWait)
{
  VerifyDowngradeToSIX(kXLock, kExpectFail);
}

TEST_F(  //
    PessimisticLockFixture,
    UpgradeToXWithoutLocksSucceed)
{
  VerifyUpgradeToXWith(kFree, kExpectSucceed);
}

TEST_F(  //
    PessimisticLockFixture,
    UpgradeToXAfterSLockNeedWait)
{
  VerifyUpgradeToXWith(kSLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Multi-thread tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    PessimisticLockFixture,
    SharedLockCounterIsCorrectlyManaged)
{
  VerifyLockSWithMultiThread();
}

TEST_F(  //
    PessimisticLockFixture,
    IncrementWithLockXKeepConsistentCounter)
{
  VerifyLockXWithMultiThread();
}

}  // namespace dbgroup::lock::test
