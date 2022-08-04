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

#include "lock/optimistic_lock.hpp"

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

class OptimisticLockFixture : public ::testing::Test
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

  //  void
  //  VerifyLockSharedWithSingleThread(const bool with_lock)
  // {
  //    const auto expected_rc = with_lock ? kExpectFailed : kExpectSuccess;
  //    if (with_lock) lock_.Lock();
  //    VerifyLockShared(expected_rc);
  //  }
  //
  //  void
  //  VerifyLockWithSingleThread(const bool with_lock_shared)
  // {
  //   const auto expected_rc = with_lock_shared ? kExpectFailed : kExpectSuccess;
  //   if (with_lock_shared) {
  //     lock_.LockShared();
  //     s_lock_count_.fetch_add(1);
  //   }
  //   VerifyLock(expected_rc);
  // }
  //
  //  void
  //  VerifyLockSharedWithMultiThread()
  // {
  //   lock_.LockShared();
  //   s_lock_count_.fetch_add(1);
  //
  //   // try to get a shared lock by another thread
  //   auto lock_shared = [this](std::promise<void> p) {
  //     lock_.LockShared();
  //     s_lock_count_.fetch_add(1);
  //     p.set_value();
  //   };
  //   std::promise<void> p{};
  //   auto&& f = p.get_future();
  //   std::thread t{lock_shared, std::move(p)};
  //
  //   // after one millisecond has passed, give up on acquiring the lock
  //   const auto rc = f.wait_for(std::chrono::milliseconds{1});
  //
  //   // verify status to check locking is succeeded
  //   ASSERT_EQ(rc, std::future_status::ready);
  //   while (s_lock_count_.load(std::memory_order_acquire) > 0) {
  //     lock_.UnlockShared();
  //     s_lock_count_.fetch_sub(1);
  //   }
  //
  //   t.join();
  // }

  // void
  // VerifyLockWithMultiThread()
  // {
  //   lock_.LockShared();
  //
  //   auto increment_with_lock = [&]() {
  //     for (size_t i = 0; i < kWriteNum; i++) {
  //       lock_.Lock();
  //       counter_++;
  //       lock_.Unlock();
  //     }
  //   };
  //
  //   std::vector<std::thread> threads{};
  //   threads.reserve(kThreadNum);
  //
  //   for (size_t i = 0; i < kThreadNum; ++i) {
  //     threads.emplace_back(increment_with_lock);
  //   }
  //
  //   // after 10 milliseconds has passed, check that the counter has not incremented
  //   std::this_thread::sleep_for(std::chrono::milliseconds(10));
  //   ASSERT_EQ(counter_, 0);
  //
  //   lock_.UnlockShared();
  //
  //   for (auto&& t : threads) {
  //     t.join();
  //   }
  //
  //   lock_.LockShared();
  //   ASSERT_EQ(counter_, kThreadNum * kWriteNum);
  //   lock_.UnlockShared();
  // }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  // void
  // VerifyLockShared(const bool expect_success)
  // {
  //   // try to get a shared lock by another thread
  //   auto lock_shared = [this](std::promise<void> p) {
  //     lock_.LockShared();
  //     s_lock_count_.fetch_add(1);
  //     p.set_value();
  //   };
  //   std::promise<void> p{};
  //   auto&& f = p.get_future();
  //   std::thread t{lock_shared, std::move(p)};
  //
  //   // after one millisecond has passed, give up on acquiring the lock
  //   const auto rc = f.wait_for(std::chrono::milliseconds{1});
  //
  //   // verify status to check locking is succeeded
  //   if (expect_success) {
  //     ASSERT_EQ(rc, std::future_status::ready);
  //     while (s_lock_count_.load(std::memory_order_acquire) > 0) {
  //       lock_.UnlockShared();
  //       s_lock_count_.fetch_sub(1);
  //     }
  //   } else {
  //     ASSERT_EQ(rc, std::future_status::timeout);
  //     lock_.Unlock();  // unlock to join the thread
  //   }
  //   t.join();
  // }

  void
  VerifyGetVersion(const bool expect_success)
  {
    // try to get a version of target node
    auto version = [this](std::promise<void> p) {
      version_.GetVersion();
      p.set_value();
    };
    std::promise<void> p{};
    auto&& f = p.get_future();
    std::thread t{version, std::move(p)};

    // after one millisecond has passed, give up on acquiring the lock
    const auto rc = f.wait_for(std::chrono::milliseconds{1});

    // verify status to check locking is succeeded
    if (expect_success) {
      ASSERT_EQ(rc, std::future_status::ready);
    } else {
      ASSERT_EQ(rc, std::future_status::timeout);
      while (s_lock_count_.load(std::memory_order_acquire) > 0) {
        lock_.UnlockShared();  // unlock to join the thread
        s_lock_count_.fetch_sub(1);
      }
    }
    t.join();
  }

  void
  VerifyCheckVersion(const bool expect_success)
  {
  }

  void
  VerifyLock(const bool expect_success)
  {
    // try to get a lock by another thread
    auto lock = [this](std::promise<void> p) {
      lock_.Lock();
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
        lock_.UnlockShared();  // unlock to join the thread
        s_lock_count_.fetch_sub(1);
      }
    }
    t.join();

    /*################################################################################################
     * Internal member variables
     *##############################################################################################*/

    OptimisticLock lock_{};
    std::atomic_size_t s_lock_count_{0};
    size_t counter_{0};
  };

  /*######################################################################################
   * Unit test definitions
   *####################################################################################*/

  /*--------------------------------------------------------------------------------------
   * Constructor tests
   *------------------------------------------------------------------------------------*/

  TEST_F(PessimisticLockFixture, LockSharedWithSingleThreadSuccess)
  {
    const bool with_lock = false;
    VerifyLockSharedWithSingleThread(with_lock);
  }

  TEST_F(PessimisticLockFixture, LockSharedAfterLockWithSingleThreadFailed)
  {
    const bool with_lock = true;
    VerifyLockSharedWithSingleThread(with_lock);
  }

  TEST_F(PessimisticLockFixture, LockWithSingleThreadSuccess)
  {
    const bool with_lock_shared = false;
    VerifyLockWithSingleThread(with_lock_shared);
  }

  TEST_F(PessimisticLockFixture, LockAfterLockSharedWithSingleThreadFailed)
  {
    const bool with_lock_shared = true;
    VerifyLockWithSingleThread(with_lock_shared);
  }

  TEST_F(PessimisticLockFixture, LockSharedWithMultiThreadSuccess)
  {
    VerifyLockSharedWithMultiThread();
  }

  TEST_F(PessimisticLockFixture, IncrementCounterWithMultiThreadSuccess)
  {
    VerifyLockWithMultiThread();
  }

}  // namespace dbgroup::lock::test
