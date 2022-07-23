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

#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::lock::test
{
/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr bool kExpectSuccess = true;
constexpr bool kExpectFailed = false;
constexpr bool kWithSLock = true;
constexpr bool kWithXLock = true;
constexpr size_t kWaitTimeMill = 10;

class PessimisticLockFixture : public ::testing::Test
{
 protected:
  /*####################################################################################
   * Constants
   *##################################################################################*/

  static constexpr auto kWriteNum = 100000;

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
  VerifySLockWithSingleThread(const bool with_x_lock)
  {
    const auto expected_rc = with_x_lock ? kExpectFailed : kExpectSuccess;
    if (with_x_lock) lock_.LockX();
    VerifySLock(expected_rc);
  }

  void
  VerifyXLockWithSingleThread(const bool with_s_lock)
  {
    const auto expected_rc = with_s_lock ? kExpectFailed : kExpectSuccess;
    if (with_s_lock) {
      lock_.LockS();
      s_lock_count_.fetch_add(1);
    }
    VerifyXLock(expected_rc);
  }

  void
  VerifySLockWithMultiThread()
  {
    lock_.LockS();
    s_lock_count_.fetch_add(1);

    // try to get a shared lock by another thread
    auto lock_shared = [this](std::promise<void> p) {
      lock_.LockS();
      s_lock_count_.fetch_add(1);
      p.set_value();
    };
    std::promise<void> p{};
    auto&& f = p.get_future();
    std::thread t{lock_shared, std::move(p)};

    // after one millisecond has passed, give up on acquiring the lock
    const auto rc = f.wait_for(std::chrono::milliseconds{1});

    // verify status to check locking is succeeded
    ASSERT_EQ(rc, std::future_status::ready);
    while (s_lock_count_.load(std::memory_order_acquire) > 0) {
      lock_.UnlockS();
      s_lock_count_.fetch_sub(1);
    }

    t.join();
  }

  void
  VerifyXLockWithMultiThread()
  {
    lock_.LockS();

    auto increment_with_lock = [&]() {
      for (size_t i = 0; i < kWriteNum; i++) {
        lock_.LockX();
        counter_++;
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

    lock_.UnlockS();

    for (auto&& t : threads) {
      t.join();
    }

    lock_.LockS();
    ASSERT_EQ(counter_, kThreadNum * kWriteNum);
    lock_.UnlockS();
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  VerifySLock(const bool expect_success)
  {
    // try to get a shared lock by another thread
    auto lock_shared = [this](std::promise<void> p) {
      lock_.LockS();
      s_lock_count_.fetch_add(1);
      p.set_value();
    };
    std::promise<void> p{};
    auto&& f = p.get_future();
    std::thread t{lock_shared, std::move(p)};

    // after one millisecond has passed, give up on acquiring the lock
    const auto rc = f.wait_for(std::chrono::milliseconds{1});

    // verify status to check locking is succeeded
    if (expect_success) {
      ASSERT_EQ(rc, std::future_status::ready);
      while (s_lock_count_.load(std::memory_order_acquire) > 0) {
        lock_.UnlockS();
        s_lock_count_.fetch_sub(1);
      }
    } else {
      ASSERT_EQ(rc, std::future_status::timeout);
      lock_.UnlockX();  // unlock to join the thread
    }
    t.join();
  }

  void
  VerifyXLock(const bool expect_success)
  {
    // try to get a lock by another thread
    auto lock = [this](std::promise<void> p) {
      lock_.LockX();
      p.set_value();
    };
    std::promise<void> p{};
    auto&& f = p.get_future();
    std::thread t{lock, std::move(p)};

    // after one millisecond has passed, give up on acquiring the lock
    const auto rc = f.wait_for(std::chrono::milliseconds{1});

    // verify status to check locking is succeeded
    if (expect_success) {
      ASSERT_EQ(rc, std::future_status::ready);
    } else {
      ASSERT_EQ(rc, std::future_status::timeout);
      while (s_lock_count_.load(std::memory_order_acquire) > 0) {
        lock_.UnlockS();  // unlock to join the thread
        s_lock_count_.fetch_sub(1);
      }
    }
    t.join();
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  PessimisticLock lock_{};
  std::atomic_size_t s_lock_count_{0};
  size_t counter_{0};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------*/

TEST_F(PessimisticLockFixture, SLockWithSingleThreadSuccess)
{
  VerifySLockWithSingleThread(!kWithXLock);
}

TEST_F(PessimisticLockFixture, SLockAfterXLockWithSingleThreadFailed)
{
  VerifySLockWithSingleThread(kWithXLock);
}

TEST_F(PessimisticLockFixture, XLockWithSingleThreadSuccess)
{
  VerifyXLockWithSingleThread(!kWithSLock);
}

TEST_F(PessimisticLockFixture, XLockAfterSLockWithSingleThreadFailed)
{
  VerifyXLockWithSingleThread(kWithSLock);
}

TEST_F(PessimisticLockFixture, SLockWithMultiThreadSuccess)
{  //
  VerifySLockWithMultiThread();
}

TEST_F(PessimisticLockFixture, IncrementCounterWithMultiThreadSuccess)
{
  VerifyXLockWithMultiThread();
}

}  // namespace dbgroup::lock::test
