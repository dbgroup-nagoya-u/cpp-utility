/*
 * Copyright 2024 Database Group, Nagoya University
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

#include "dbgroup/lock/mcs_lock.hpp"

// C++ standard libraries
#include <chrono>
#include <future>
#include <stdexcept>
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
constexpr size_t kWriteNumPerThread = 1E4;

/*##############################################################################
 * Fixture definition
 *############################################################################*/

class MCSLockFixture : public ::testing::Test
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

    auto *guard = GetLock(lock_type);
    TryLock(kSLock, expected_rc);
    ReleaseLock(lock_type, guard);

    t_.join();
  }

  void
  VerifyLockXWith(  //
      const LockType lock_type)
  {
    const auto expected_rc = (lock_type != kFree) ? kExpectFail : kExpectSucceed;

    auto *guard = GetLock(lock_type);
    TryLock(kXLock, expected_rc);
    ReleaseLock(lock_type, guard);

    t_.join();
  }

  void
  VerifyLockSWithMultiThread()
  {
    // create threads to get/release a shared lock
    auto lock_unlock_s = [this]() {
      auto *s_guard = lock_.LockS();
      lock_.UnlockS(s_guard);
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
    TryLock(kXLock, kExpectSucceed);

    t_.join();
  }

  void
  VerifyLockXWithMultiThread()
  {
    // create a shared lock to prevent a counter from modifying
    auto *s_guard = lock_.LockS();

    // create incrementor threads
    auto increment_with_lock = [&]() {
      for (size_t i = 0; i < kWriteNumPerThread; i++) {
        auto *x_guard = lock_.LockX();
        ++counter_;
        lock_.UnlockX(x_guard);
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
    lock_.UnlockS(s_guard);
    for (auto &&t : threads) {
      t.join();
    }

    // check the counter
    s_guard = lock_.LockS();
    ASSERT_EQ(counter_, kThreadNum * kWriteNumPerThread);
    lock_.UnlockS(s_guard);
  }

  /*############################################################################
   * Public utility functions
   *##########################################################################*/

  auto
  GetLock(                       //
      const LockType lock_type)  //
      -> MCSLock *
  {
    switch (lock_type) {
      case kSLock:
        return lock_.LockS();

      case kXLock:
        return lock_.LockX();

      case kSIXLock:
        throw std::runtime_error{"This class has no SIX locks."};

      case kFree:
      default:
        break;
    }
    return nullptr;
  }

  void
  ReleaseLock(  //
      const LockType lock_type,
      MCSLock *lock_guard)
  {
    switch (lock_type) {
      case kSLock:
        lock_.UnlockS(lock_guard);
        break;

      case kXLock:
        lock_.UnlockX(lock_guard);
        break;

      case kSIXLock:
        throw std::runtime_error{"This class has no SIX locks."};

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
    auto *guard = GetLock(lock_type);
    p.set_value();
    ReleaseLock(lock_type, guard);
  }

  void
  TryLock(  //
      const LockType lock_type,
      const bool expect_success)
  {
    // try to get an exclusive lock by another thread
    std::promise<void> p{};
    auto &&f = p.get_future();
    t_ = std::thread{&MCSLockFixture::LockWorker, this, lock_type, std::move(p)};

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

  MCSLock lock_{};

  size_t counter_{0};

  std::thread t_{};
};

/*##############################################################################
 * Unit test definitions
 *############################################################################*/

TEST_F(  //
    MCSLockFixture,
    LockSWithoutLocksSucceed)
{
  VerifyLockSWith(kFree);
}

TEST_F(  //
    MCSLockFixture,
    LockSWithSLockSucceed)
{
  VerifyLockSWith(kSLock);
}

TEST_F(  //
    MCSLockFixture,
    LockSWithXLockFail)
{
  VerifyLockSWith(kXLock);
}

TEST_F(  //
    MCSLockFixture,
    LockXWithoutLocksSucceed)
{
  VerifyLockXWith(kFree);
}

TEST_F(  //
    MCSLockFixture,
    LockXWithSLockFail)
{
  VerifyLockXWith(kSLock);
}

TEST_F(  //
    MCSLockFixture,
    LockXWithXLockFail)
{
  VerifyLockXWith(kXLock);
}

TEST_F(  //
    MCSLockFixture,
    SharedLockCounterIsCorrectlyManaged)
{
  VerifyLockSWithMultiThread();
}

TEST_F(  //
    MCSLockFixture,
    IncrementWithLockXKeepConsistentCounter)
{
  VerifyLockXWithMultiThread();
}

}  // namespace dbgroup::lock::test
