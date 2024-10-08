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
constexpr size_t kWaitTimeMill = 100;
constexpr size_t kThreadNumForLockS = 1E2;
constexpr size_t kWriteNumPerThread = 1E5;
constexpr size_t kVerUnit = 0b010UL << 17UL;

class OptimisticLockFixture : public ::testing::Test
{
 protected:
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
  VerifyLockSWith(const LockType lock_type)
  {
    const auto expected_rc = (lock_type == kXLock) ? kExpectFail : kExpectSucceed;
    const auto version = lock_.GetVersion();

    GetLock(lock_type);
    TryLock(kSLock, expected_rc);
    if (lock_type == kXLock) {
      ASSERT_FALSE(lock_.TryLockS(version));
    }
    ReleaseLock(lock_type);

    ASSERT_EQ(lock_.HasSameVersion(version), lock_type != kXLock);

    t_.join();

    const size_t inc = static_cast<size_t>(lock_type == kXLock);
    ASSERT_EQ(lock_.HasSameVersion(version), lock_type != kXLock);
    ASSERT_EQ(version + kVerUnit * inc, lock_.GetVersion());
  }

  void
  VerifyLockXWith(const LockType lock_type)
  {
    const auto expected_rc = (lock_type != kFree) ? kExpectFail : kExpectSucceed;
    const auto version = lock_.GetVersion();

    GetLock(lock_type);
    TryLock(kXLock, expected_rc);
    if (((lock_type == kSLock) || (lock_type == kSIXLock))) {
      ASSERT_TRUE(lock_.HasSameVersion(version));
    }
    if (lock_type == kXLock) {
      ASSERT_FALSE(lock_.TryLockX(version));
    }
    ReleaseLock(lock_type);

    t_.join();

    const size_t inc = 1 + static_cast<size_t>(lock_type == kXLock);
    ASSERT_FALSE(lock_.HasSameVersion(version));
    ASSERT_EQ(version + kVerUnit * inc, lock_.GetVersion());
  }

  void
  VerifyLockSIXWith(const LockType lock_type)
  {
    const auto expected_rc =
        (lock_type == kXLock || lock_type == kSIXLock) ? kExpectFail : kExpectSucceed;
    const auto version = lock_.GetVersion();

    GetLock(lock_type);
    TryLock(kSIXLock, expected_rc);
    if (lock_type == kXLock) {
      ASSERT_FALSE(lock_.TryLockSIX(version));
    }
    ReleaseLock(lock_type);

    ASSERT_EQ(lock_.HasSameVersion(version), lock_type != kXLock);

    t_.join();

    const size_t inc = static_cast<size_t>(lock_type == kXLock);
    ASSERT_EQ(lock_.HasSameVersion(version), lock_type != kXLock);
    ASSERT_EQ(version + kVerUnit * inc, lock_.GetVersion());
  }

  void
  VerifyDowngradeToSIX(const LockType lock_type)
  {
    const auto version = lock_.GetVersion();

    lock_.LockX();
    lock_.DowngradeToSIX();

    ASSERT_FALSE(lock_.HasSameVersion(version));

    switch (lock_type) {
      case kSLock:
        TryLock(kSLock, kExpectSucceed);
        break;

      case kXLock:
        TryLock(kXLock, kExpectFail);
        break;

      case kSIXLock:
        TryLock(kSIXLock, kExpectFail);
        break;

      default:
        break;
    }

    lock_.UnlockSIX();

    t_.join();

    const size_t inc = 1 + static_cast<size_t>(lock_type == kXLock);
    ASSERT_EQ(version + kVerUnit * inc, lock_.GetVersion());
  }

  void
  VerifyUpgradeToXWith(const LockType lock_type)
  {
    const auto expected_rc = (lock_type == kSLock) ? kExpectFail : kExpectSucceed;
    const auto version = lock_.GetVersion();

    lock_.LockSIX();  // this lock is going to be released in TryUpgrade
    GetLock(lock_type);
    TryUpgrade(expected_rc);
    if (lock_type == kSLock) {
      ASSERT_TRUE(lock_.HasSameVersion(version));
    }
    ReleaseLock(lock_type);

    t_.join();

    ASSERT_FALSE(lock_.HasSameVersion(version));
    ASSERT_EQ(version + kVerUnit, lock_.GetVersion());
  }

  void
  VerifyLockSWithMultiThread()
  {
    const auto version = lock_.GetVersion();

    // create threads to get/release a shared lock
    auto lock_unlock_s = [this]() {
      lock_.LockS();
      lock_.UnlockS();
    };
    std::vector<std::thread> threads{};
    threads.reserve(kThreadNumForLockS);
    for (size_t i = 0; i < kThreadNumForLockS; ++i) {
      threads.emplace_back(lock_unlock_s);
    }

    // check the counter of shared locks is correctly managed
    for (auto &&t : threads) {
      t.join();
    }

    ASSERT_TRUE(lock_.HasSameVersion(version));

    TryLock(kXLock, kExpectSucceed);

    t_.join();

    ASSERT_FALSE(lock_.HasSameVersion(version));
  }

  void
  VerifyLockXWithMultiThread()
  {
    const auto version = lock_.GetVersion();

    // create a shared lock to prevent a counter from modifying
    lock_.LockS();

    // create incrementor threads
    auto increment_with_lock = [&]() {
      for (size_t i = 0; i < kWriteNumPerThread; i++) {
        lock_.LockX();
        ++counter_;
        lock_.UnlockX();
      }
    };
    std::vector<std::thread> threads{};
    threads.reserve(kThreadNum);
    for (size_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(increment_with_lock);
    }

    // after short sleep, check that the counter has not incremented
    std::this_thread::sleep_for(std::chrono::milliseconds(kWaitTimeMill));
    ASSERT_EQ(counter_, 0);

    // release the shared lock, and then wait for the incrementors
    lock_.UnlockS();
    for (auto &&t : threads) {
      t.join();
    }

    // check the counter
    lock_.LockS();
    ASSERT_EQ(counter_, kThreadNum * kWriteNumPerThread);
    lock_.UnlockS();
    ASSERT_FALSE(lock_.HasSameVersion(version));
    ASSERT_EQ(version + kVerUnit * counter_, lock_.GetVersion());
  }

  /*############################################################################
   * Public utility functions
   *##########################################################################*/

  void
  GetLock(const LockType lock_type)
  {
    switch (lock_type) {
      case kSLock:
        lock_.LockS();
        break;

      case kXLock:
        lock_.LockX();
        break;

      case kSIXLock:
        lock_.LockSIX();
        break;

      case kFree:
      default:
        break;
    }
  }

  void
  ReleaseLock(const LockType lock_type)
  {
    switch (lock_type) {
      case kSLock:
        lock_.UnlockS();
        break;

      case kXLock:
        lock_.UnlockX();
        break;

      case kSIXLock:
        lock_.UnlockSIX();
        break;

      case kFree:
      default:
        break;
    }
  }

  void
  LockWorker(  //
      const LockType lock_type,
      std::promise<void> p)
  {
    GetLock(lock_type);
    p.set_value();
    ReleaseLock(lock_type);
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
    const auto rc = f.wait_for(std::chrono::milliseconds{kWaitTimeMill});

    // verify status to check locking is succeeded
    if (expect_success) {
      ASSERT_EQ(rc, std::future_status::ready);
    } else {
      ASSERT_EQ(rc, std::future_status::timeout);
    }
  }

  void
  TryUpgrade(const bool expect_success)
  {
    auto upgrade_worker = [this](std::promise<void> p) -> void {
      lock_.UpgradeToX();
      p.set_value();
      lock_.UnlockX();
    };

    // try to get an exclusive lock by another thread
    std::promise<void> p{};
    auto &&f = p.get_future();
    t_ = std::thread{upgrade_worker, std::move(p)};

    // after short sleep, give up on acquiring the lock
    const auto rc = f.wait_for(std::chrono::milliseconds{kWaitTimeMill});

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

TEST_F(OptimisticLockFixture, LockSWithoutLocksSucceed)
{  //
  VerifyLockSWith(kFree);
}

TEST_F(OptimisticLockFixture, LockSWithSLockSucceed)
{  //
  VerifyLockSWith(kSLock);
}

TEST_F(OptimisticLockFixture, LockSWithXLockFail)
{  //
  VerifyLockSWith(kXLock);
}

TEST_F(OptimisticLockFixture, LockSWithSIXLockSucceed)
{  //
  VerifyLockSWith(kSIXLock);
}

TEST_F(OptimisticLockFixture, LockXWithoutLocksSucceed)
{  //
  VerifyLockXWith(kFree);
}

TEST_F(OptimisticLockFixture, LockXWithSLockFail)
{  //
  VerifyLockXWith(kSLock);
}

TEST_F(OptimisticLockFixture, LockXWithXLockFail)
{  //
  VerifyLockXWith(kXLock);
}

TEST_F(OptimisticLockFixture, LockXWithSIXLockFail)
{  //
  VerifyLockXWith(kSIXLock);
}

TEST_F(OptimisticLockFixture, LockSIXWithoutLocksSucceed)
{  //
  VerifyLockSIXWith(kFree);
}

TEST_F(OptimisticLockFixture, LockSIXWithSLockSucceed)
{  //
  VerifyLockSIXWith(kSLock);
}

TEST_F(OptimisticLockFixture, LockSIXWithXLockFail)
{  //
  VerifyLockSIXWith(kXLock);
}

TEST_F(OptimisticLockFixture, LockSIXWithSIXLockFail)
{  //
  VerifyLockSIXWith(kSIXLock);
}

TEST_F(OptimisticLockFixture, LockSAfterDowngradeToSIXSucceed)
{  //
  VerifyDowngradeToSIX(kSLock);
}

TEST_F(OptimisticLockFixture, LockXAfterDowngradeToSIXFail)
{  //
  VerifyDowngradeToSIX(kXLock);
}

TEST_F(OptimisticLockFixture, LockSIXAfterDowngradeToSIXFail)
{  //
  VerifyDowngradeToSIX(kSIXLock);
}

TEST_F(OptimisticLockFixture, UpgradeToXWithoutLocksSucceed)
{  //
  VerifyUpgradeToXWith(kFree);
}

TEST_F(OptimisticLockFixture, UpgradeToXWithSLockFail)
{  //
  VerifyUpgradeToXWith(kSLock);
}

TEST_F(OptimisticLockFixture, SharedLockCounterIsCorrectlyManaged)
{  //
  VerifyLockSWithMultiThread();
}

TEST_F(OptimisticLockFixture, IncrementWithLockXKeepConsistentCounter)
{  //
  VerifyLockXWithMultiThread();
}

}  // namespace dbgroup::lock::test
