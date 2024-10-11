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

#include "dbgroup/lock/optimistic_lock.hpp"

// C++ standard libraries
#include <chrono>
#include <future>
#include <thread>
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
constexpr size_t kVerUnit = 0b010UL << 17UL;
constexpr std::chrono::milliseconds kWaitTimeMill{100};

class OptimisticLockFixture : public ::testing::Test
{
 protected:
  /*############################################################################
   * Types
   *##########################################################################*/

  using Guard =
      std::variant<int, OptimisticLock::SGuard, OptimisticLock::SIXGuard, OptimisticLock::XGuard>;

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
    const auto version = lock_.GetVersion();

    {
      [[maybe_unused]] const auto &guard = GetLock(lock_type);
      TryLock(kSLock, expected_rc);
    }
    t_.join();

    if (lock_type != kXLock) {
      ASSERT_TRUE(lock_.HasSameVersion(version));
      ASSERT_TRUE(lock_.TryLockS(version));
    } else {
      ASSERT_FALSE(lock_.HasSameVersion(version));
      ASSERT_FALSE(lock_.TryLockS(version));
    }
  }

  void
  VerifyLockSIXWith(  //
      const LockType lock_type,
      const bool expected_rc)
  {
    const auto version = lock_.GetVersion();

    {
      [[maybe_unused]] const auto &guard = GetLock(lock_type);
      TryLock(kSIXLock, expected_rc);
    }
    t_.join();

    if (lock_type != kXLock) {
      ASSERT_TRUE(lock_.HasSameVersion(version));
      ASSERT_TRUE(lock_.TryLockSIX(version));
    } else {
      ASSERT_FALSE(lock_.HasSameVersion(version));
      ASSERT_FALSE(lock_.TryLockSIX(version));
    }
  }

  void
  VerifyLockXWith(  //
      const LockType lock_type,
      const bool expected_rc)
  {
    auto version = lock_.GetVersion();

    {
      [[maybe_unused]] const auto &guard = GetLock(lock_type);
      TryLock(kXLock, expected_rc);
    }
    t_.join();

    ASSERT_FALSE(lock_.HasSameVersion(version));
    ASSERT_FALSE(lock_.TryLockX(version));

    version = lock_.GetVersion();
    ASSERT_TRUE(lock_.TryLockX(version));
  }

  void
  VerifyDowngradeToSIX(  //
      const LockType lock_type,
      const bool expected_rc)
  {
    const auto version = lock_.GetVersion();

    {
      [[maybe_unused]] const auto &six_guard = lock_.LockX().DowngradeToSIX();
      TryLock(lock_type, expected_rc);
    }
    t_.join();

    ASSERT_FALSE(lock_.HasSameVersion(version));
  }

  void
  VerifyUpgradeToXWith(  //
      const LockType lock_type,
      const bool expected_rc)
  {
    const auto version = lock_.GetVersion();

    {
      [[maybe_unused]] const auto &guard = GetLock(lock_type);
      TryUpgrade(lock_.LockSIX(), expected_rc);
    }
    t_.join();

    ASSERT_FALSE(lock_.HasSameVersion(version));
  }

  void
  VerifyLockSWithMultiThread()
  {
    const auto version = lock_.GetVersion();

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
    ASSERT_TRUE(lock_.HasSameVersion(version));

    TryLock(kXLock, kExpectSucceed);
    t_.join();
  }

  void
  VerifyLockXWithMultiThread()
  {
    const auto version = lock_.GetVersion();

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

    // release the shared lock, and then wait for the incrementors
    for (auto &&t : threads) {
      t.join();
    }
    ASSERT_FALSE(lock_.HasSameVersion(version));

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
    t_ = std::thread{&OptimisticLockFixture::LockWorker, this, lock_type, std::move(p)};

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
      OptimisticLock::SIXGuard six_guard,
      const bool expect_success)
  {
    auto upgrade_worker = [](OptimisticLock::SIXGuard six_guard, std::promise<void> p) -> void {
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

  OptimisticLock lock_{};

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
    OptimisticLockFixture,
    LockSWithoutLocksSucceed)
{
  VerifyLockSWith(kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSWithSLockSucceed)
{
  VerifyLockSWith(kSLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSWithSIXLockSucceed)
{
  VerifyLockSWith(kSIXLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSWithXLockNeedWait)
{
  VerifyLockSWith(kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Exclusive lock tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    OptimisticLockFixture,
    LockXWithoutLocksSucceed)
{
  VerifyLockXWith(kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockXWithSLockNeedWait)
{
  VerifyLockXWith(kSLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    LockXWithSIXLockNeedWait)
{
  VerifyLockXWith(kSIXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    LockXWithXLockNeedWait)
{
  VerifyLockXWith(kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Shared-with-intent-exclusive lock tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    OptimisticLockFixture,
    LockSIXWithoutLocksSucceed)
{
  VerifyLockSIXWith(kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSIXWithSLockSucceed)
{
  VerifyLockSIXWith(kSLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSIXWithSIXLockNeedWait)
{
  VerifyLockSIXWith(kSIXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSIXWithXLockNeedWait)
{
  VerifyLockSIXWith(kXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSAfterDowngradeToSIXSucceed)
{
  VerifyDowngradeToSIX(kSLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSIXAfterDowngradeToSIXNeedWait)
{
  VerifyDowngradeToSIX(kSIXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    LockXAfterDowngradeToSIXNeedWait)
{
  VerifyDowngradeToSIX(kXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    UpgradeToXWithoutLocksSucceed)
{
  VerifyUpgradeToXWith(kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    UpgradeToXWithSLockNeedWait)
{
  VerifyUpgradeToXWith(kSLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Multi-thread tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    OptimisticLockFixture,
    SharedLockCounterIsCorrectlyManaged)
{
  VerifyLockSWithMultiThread();
}

TEST_F(  //
    OptimisticLockFixture,
    IncrementWithLockXKeepConsistentCounter)
{
  VerifyLockXWithMultiThread();
}

}  // namespace dbgroup::lock::test
