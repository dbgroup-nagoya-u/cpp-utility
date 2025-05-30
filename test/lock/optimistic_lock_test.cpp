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
#include <tuple>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

namespace dbgroup::lock::test
{
/*############################################################################*
 * Global constants
 *############################################################################*/

constexpr bool kExpectSucceed = true;
constexpr bool kExpectFail = false;
constexpr size_t kThreadNumForLockS = 1E2;
constexpr size_t kWriteNumPerThread = 1E5;
constexpr std::chrono::milliseconds kWaitTimeMill{100};

class OptimisticLockFixture : public ::testing::Test
{
 protected:
  /*##########################################################################*
   * Types
   *##########################################################################*/

  using Guard =
      std::variant<int, OptimisticLock::SGuard, OptimisticLock::SIXGuard, OptimisticLock::XGuard>;

  /*##########################################################################*
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

  /*##########################################################################*
   * Functions for verification
   *##########################################################################*/

  void
  VerifyLock(  //
      const LockType lock_type,
      const LockType with_lock_type,
      const bool expected_rc)
  {
    {
      [[maybe_unused]] const auto &guard = GetLock(with_lock_type);
      TryLock(lock_type, expected_rc);
    }
    t_.join();
  }

  void
  VerifyTryLock(  //
      const LockType lock_type,
      const LockType with_lock_type,
      const bool expected_rc)
  {
    auto &&opt_guard = lock_.GetVersion();
    {
      [[maybe_unused]] const auto &guard = GetLock(with_lock_type);
      TryTryLock(lock_type, with_lock_type, std::move(opt_guard), expected_rc);
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
      const LockType with_lock_type,
      const bool expected_rc)
  {
    auto &&opt_guard = lock_.GetVersion();
    {
      [[maybe_unused]] const auto &guard = GetLock(with_lock_type);
      TryUpgrade(lock_.LockSIX(), expected_rc);
    }
    t_.join();

    ASSERT_FALSE(opt_guard.VerifyVersion());
  }

  void
  VerifyLockSWithMultiThread()
  {
    auto &&opt_guard = lock_.GetVersion();

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
    ASSERT_TRUE(opt_guard.VerifyVersion());

    TryLock(kXLock, kExpectSucceed);
    t_.join();
  }

  void
  VerifyLockXWithMultiThread()
  {
    constexpr auto kWriterNum = kThreadNum / 2;
    std::atomic_size_t end_num{};
    std::vector<std::thread> threads{};
    threads.reserve(kThreadNum);

    {  // create a shared lock to prevent a counter from modifying
      auto &&s_guard = lock_.LockS();

      // create incrementor threads
      for (size_t i = 0; i < kThreadNum; ++i) {
        if (i % 2 == 0) {
          threads.emplace_back([&]() {
            size_t cnt = 0;
            while (end_num < kWriterNum) {
              size_t cur;
              auto &&opt_guard = lock_.GetVersion();
              while (true) {
                cur = counter_;
                if (opt_guard.VerifyVersion()) break;
              }

              ASSERT_LE(cnt, cur);
              ASSERT_EQ(cur, opt_guard.GetVersion());
              cnt = cur;
            }
          });
        } else {
          threads.emplace_back([&]() {
            for (size_t j = 0; j < kWriteNumPerThread; j++) {
              auto &&x_guard = lock_.LockX();
              ASSERT_EQ(counter_, x_guard.GetVersion());
              ++counter_;
            }
            ++end_num;
          });
        }
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
    ASSERT_EQ(counter_, kWriterNum * kWriteNumPerThread);
    ASSERT_EQ(counter_, s_guard.GetVersion());
  }

  /*##########################################################################*
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
  TryLock(  //
      const LockType lock_type,
      const bool expect_success)
  {
    // try to get an exclusive lock by another thread
    std::promise<void> p{};
    auto &&f = p.get_future();
    t_ = std::thread{[this](const LockType lock_type, std::promise<void> p) {
                       [[maybe_unused]] const auto &guard = GetLock(lock_type);
                       p.set_value();
                     },
                     lock_type, std::move(p)};

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
    // try to get an exclusive lock by another thread
    std::promise<void> p{};
    auto &&f = p.get_future();
    t_ = std::thread{[](OptimisticLock::SIXGuard six_guard, std::promise<void> p) -> void {
                       [[maybe_unused]] const auto &x_guard = six_guard.UpgradeToX();
                       p.set_value();
                     },
                     std::move(six_guard), std::move(p)};

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
  TryTryLock(  //
      const LockType lock_type,
      const LockType conflict_type,
      OptimisticLock::VerGuard opt_guard,
      const bool expect_success)
  {
    auto try_lock = [](LockType lock_type, LockType conflict_type,
                       OptimisticLock::VerGuard opt_guard, std::promise<void> p) {
      switch (lock_type) {
        case kSLock: {
          const auto &guard = opt_guard.TryLockS();
          if (conflict_type != kXLock) {
            ASSERT_TRUE(guard);
          } else {
            ASSERT_FALSE(guard);
          }
          break;
        }
        case kSIXLock: {
          const auto &guard = opt_guard.TryLockSIX();
          if (conflict_type != kXLock) {
            ASSERT_TRUE(guard);
          } else {
            ASSERT_FALSE(guard);
          }
          break;
        }
        case kXLock: {
          const auto &guard = opt_guard.TryLockX();
          if (conflict_type != kXLock) {
            ASSERT_TRUE(guard);
          } else {
            ASSERT_FALSE(guard);
          }
          break;
        }
        case kFree:
        default:
          break;
      }
      p.set_value();
    };

    // try to get an exclusive lock by another thread
    std::promise<void> p{};
    auto &&f = p.get_future();
    t_ = std::thread{try_lock, lock_type, conflict_type, std::move(opt_guard), std::move(p)};

    // after short sleep, give up on acquiring the lock
    const auto rc = f.wait_for(kWaitTimeMill);

    // verify status to check locking is succeeded
    if (expect_success) {
      ASSERT_EQ(rc, std::future_status::ready);
    } else {
      ASSERT_EQ(rc, std::future_status::timeout);
    }
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  OptimisticLock lock_{};

  size_t counter_{0};

  std::thread t_{};
};

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

/*----------------------------------------------------------------------------*
 * Shared lock tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    OptimisticLockFixture,
    LockSWithoutLocksSucceed)
{
  VerifyLock(kSLock, kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSWithSLockSucceed)
{
  VerifyLock(kSLock, kSLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSWithSIXLockSucceed)
{
  VerifyLock(kSLock, kSIXLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSWithXLockNeedWait)
{
  VerifyLock(kSLock, kXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockSWithoutLocksSucceed)
{
  VerifyTryLock(kSLock, kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockSWithSLockSucceed)
{
  VerifyTryLock(kSLock, kSLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockSWithSIXLockSucceed)
{
  VerifyTryLock(kSLock, kSIXLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockSWithXLockNeedWait)
{
  VerifyTryLock(kSLock, kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Exclusive lock tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    OptimisticLockFixture,
    LockXWithoutLocksSucceed)
{
  VerifyLock(kXLock, kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockXWithSLockNeedWait)
{
  VerifyLock(kXLock, kSLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    LockXWithSIXLockNeedWait)
{
  VerifyLock(kXLock, kSIXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    LockXWithXLockNeedWait)
{
  VerifyLock(kXLock, kXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockXWithoutLocksSucceed)
{
  VerifyTryLock(kXLock, kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockXWithSLockNeedWait)
{
  VerifyTryLock(kXLock, kSLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockXWithSIXLockNeedWait)
{
  VerifyTryLock(kXLock, kSIXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockXWithXLockNeedWait)
{
  VerifyTryLock(kXLock, kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Shared-with-intent-exclusive lock tests
 *----------------------------------------------------------------------------*/

TEST_F(  //
    OptimisticLockFixture,
    LockSIXWithoutLocksSucceed)
{
  VerifyLock(kSIXLock, kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSIXWithSLockSucceed)
{
  VerifyLock(kSIXLock, kSLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSIXWithSIXLockNeedWait)
{
  VerifyLock(kSIXLock, kSIXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    LockSIXWithXLockNeedWait)
{
  VerifyLock(kSIXLock, kXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockSIXWithoutLocksSucceed)
{
  VerifyTryLock(kSIXLock, kFree, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockSIXWithSLockSucceed)
{
  VerifyTryLock(kSIXLock, kSLock, kExpectSucceed);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockSIXWithSIXLockNeedWait)
{
  VerifyTryLock(kSIXLock, kSIXLock, kExpectFail);
}

TEST_F(  //
    OptimisticLockFixture,
    TryLockSIXWithXLockNeedWait)
{
  VerifyTryLock(kSIXLock, kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Downgrade/Upgrade tests
 *----------------------------------------------------------------------------*/

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
