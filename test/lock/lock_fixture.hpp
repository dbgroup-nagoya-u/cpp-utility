/*
 * Copyright 2026 Database Group, Nagoya University
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

#ifndef CPP_UTILITY_TEST_LOCK_LOCK_FIXTURE_HPP_
#define CPP_UTILITY_TEST_LOCK_LOCK_FIXTURE_HPP_

// C++ standard libraries
#include <atomic>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <future>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

// external libraries
#include <gtest/gtest.h>

// local sources
#include "common.hpp"
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/mcs_lock.hpp"
#include "dbgroup/lock/omcs_lock.hpp"
#include "dbgroup/lock/optimistic_lock.hpp"
#include "dbgroup/lock/optiql.hpp"
#include "dbgroup/lock/pessimistic_lock.hpp"
#include "dbgroup/lock/utility.hpp"
#include "dbgroup/thread/id_manager.hpp"

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

/// @brief A version bit used for verifying bitmasks.
constexpr uint32_t kMaskedBit = 0b10U;

/*############################################################################*
 * Lock-specific properties
 *############################################################################*/

/**
 * @brief Lock-specific properties that change expected results.
 *
 * @tparam Lock A target lock class.
 */
template <class Lock>
struct LockTraits {
  /// @brief Shared locks wait for preceding SIX locks (e.g., FIFO queue locks).
  static constexpr bool kSIXBlocksS = false;
};

template <>
struct LockTraits<MCSLock> {
  static constexpr bool kSIXBlocksS = true;
};

template <>
struct LockTraits<OMCSLock> {
  static constexpr bool kSIXBlocksS = true;
};

/**
 * @brief A concept for representing locks with an optional `ImmediateVerify` API.
 *
 * @tparam Lock A target lock class.
 */
template <class Lock>
concept ImmediatelyVerifiable = requires(typename Lock::OptGuard& x, uint32_t mask) {
  { x.ImmediateVerify(mask) } noexcept -> std::convertible_to<bool>;
};

/**
 * @brief A class for generating readable names of typed tests.
 *
 */
struct LockNameGenerator {
  template <class Lock>
  static auto
  GetName(  //
      [[maybe_unused]] const int i) -> std::string
  {
    if constexpr (std::is_same_v<Lock, PessimisticLock>) return "PessimisticLock";
    if constexpr (std::is_same_v<Lock, OptimisticLock>) return "OptimisticLock";
    if constexpr (std::is_same_v<Lock, MCSLock>) return "MCSLock";
    if constexpr (std::is_same_v<Lock, OMCSLock>) return "OMCSLock";
    if constexpr (std::is_same_v<Lock, OptiQL>) return "OptiQL";
    return std::to_string(i);
  }
};

/*############################################################################*
 * Fixture definitions
 *############################################################################*/

/**
 * @brief A common fixture for testing lock classes.
 *
 * @tparam Lock A target lock class.
 * @note APIs depending on lock categories are constrained by lock concepts.
 */
template <Lockable Lock>
class LockFixture : public ::testing::Test
{
 protected:
  /*##########################################################################*
   * Types
   *##########################################################################*/

  using XGuard = typename Lock::XGuard;

  /*##########################################################################*
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    static_assert(kThreadNum < kMaxThreadCapacity);
    dbgroup::thread::IDManager::SetMaxThreadNum(kMaxThreadCapacity);
  }

  void
  TearDown() override
  {
    if (t_.joinable()) {
      t_.join();
    }
  }

  /*##########################################################################*
   * Utilities for multi-threading
   *##########################################################################*/

  /**
   * @brief Run a given procedure in another thread and check it finishes in time.
   *
   * @param func A procedure to be run.
   * @param expect_success true if the procedure must finish without waiting.
   * @note If `func` returns a value (e.g., a lock guard), this function keeps
   * it until notifying the completion so that releasing locks does not affect
   * the result.
   * @note The caller must join `t_` after releasing locks held by itself.
   */
  template <class Func>
  void
  RunInAnotherThread(  //
      Func&& func,
      const bool expect_success)
  {
    std::promise<void> p{};
    auto&& f = p.get_future();
    t_ = std::thread{[func = std::forward<Func>(func)](std::promise<void> p) mutable {
                       if constexpr (std::is_void_v<std::invoke_result_t<Func&>>) {
                         func();
                         p.set_value();
                       } else {
                         [[maybe_unused]] auto&& ret = func();
                         p.set_value();
                       }
                     },
                     std::move(p)};

    // after short sleep, give up on waiting the procedure
    const auto rc = f.wait_for(kWaitTimeMill);
    if (expect_success) {
      EXPECT_EQ(rc, std::future_status::ready);
    } else {
      EXPECT_EQ(rc, std::future_status::timeout);
    }
  }

  /*##########################################################################*
   * Utilities for pessimistic locks
   *##########################################################################*/

  /**
   * @param lock_type A lock type to be acquired.
   * @return A guard instance for the acquired lock (or an empty instance).
   */
  auto
  GetLock(                       //
      const LockType lock_type)  //
      -> auto
    requires PessimisticallyLockable<Lock>
  {
    using Guard = std::variant<std::monostate, typename Lock::SGuard, typename Lock::SIXGuard,
                               typename Lock::XGuard>;
    switch (lock_type) {
      case kSLock: {
        auto&& guard = lock_.LockS();
        EXPECT_TRUE(guard);
        return Guard{std::move(guard)};
      }
      case kSIXLock: {
        auto&& guard = lock_.LockSIX();
        EXPECT_TRUE(guard);
        return Guard{std::move(guard)};
      }
      case kXLock: {
        auto&& guard = lock_.LockX();
        EXPECT_TRUE(guard);
        return Guard{std::move(guard)};
      }
      case kFree:
      default:
        break;
    }
    return Guard{};
  }

  /**
   * @brief Try to get a lock while another lock is held.
   *
   * @param lock_type A lock type to be acquired.
   * @param with_lock_type A lock type held beforehand.
   * @param expect_success true if the lock must be acquired without waiting.
   */
  void
  VerifyLock(  //
      const LockType lock_type,
      const LockType with_lock_type,
      const bool expect_success)
    requires PessimisticallyLockable<Lock>
  {
    {
      [[maybe_unused]] const auto& guard = GetLock(with_lock_type);
      RunInAnotherThread([this, lock_type] { return GetLock(lock_type); }, expect_success);
    }
    t_.join();
  }

  /**
   * @brief Try to get a lock while a downgraded SIX lock is held.
   *
   * @param lock_type A lock type to be acquired.
   * @param expect_success true if the lock must be acquired without waiting.
   */
  void
  VerifyLockWithDowngradedSIXLock(  //
      const LockType lock_type,
      const bool expect_success)
    requires PessimisticallyLockable<Lock>
  {
    {
      [[maybe_unused]] const auto& six_guard = lock_.LockX().DowngradeToSIX();
      RunInAnotherThread([this, lock_type] { return GetLock(lock_type); }, expect_success);
    }
    t_.join();
  }

  /**
   * @brief Try to upgrade an SIX lock to an X lock while another lock is held.
   *
   * @param with_lock_type A lock type held beforehand.
   * @param expect_success true if the lock must be upgraded without waiting.
   */
  void
  VerifyUpgradeToX(  //
      const LockType with_lock_type,
      const bool expect_success)
    requires PessimisticallyLockable<Lock>
  {
    auto&& opt_guard = GetOptGuardIfExist();
    {
      [[maybe_unused]] const auto& guard = GetLock(with_lock_type);
      RunInAnotherThread([six_guard = lock_.LockSIX()]() mutable { return six_guard.UpgradeToX(); },
                         expect_success);
    }
    t_.join();

    if constexpr (OptimisticallyLockable<Lock>) {
      EXPECT_FALSE(opt_guard.VerifyVersion());  // X locks modify the version
    }
  }

  /**
   * @brief Verify that many shared locks are acquired and released correctly.
   *
   */
  void
  VerifyLockSWithMultiThread()
    requires PessimisticallyLockable<Lock>
  {
    auto&& opt_guard = GetOptGuardIfExist();

    // create threads to get/release a shared lock
    std::vector<std::thread> threads{};
    threads.reserve(kThreadNumForLockS);
    for (size_t i = 0; i < kThreadNumForLockS; ++i) {
      threads.emplace_back([this]() { [[maybe_unused]] auto&& s_guard = lock_.LockS(); });
    }
    for (auto&& t : threads) {
      t.join();
    }

    if constexpr (OptimisticallyLockable<Lock>) {
      EXPECT_TRUE(opt_guard.VerifyVersion());  // S locks do not modify the version
    }

    // check the counter of shared locks is correctly managed
    RunInAnotherThread([this] { return lock_.LockX(); }, kExpectSucceed);
    t_.join();
  }

  /**
   * @brief Verify that exclusive locks protect a shared counter.
   *
   */
  void
  VerifyLockXWithMultiThread()
    requires PessimisticallyLockable<Lock>
  {
    auto&& opt_guard = GetOptGuardIfExist();

    std::vector<std::thread> threads{};
    threads.reserve(kThreadNum);
    {  // create a shared lock to prevent a counter from modifying
      [[maybe_unused]] auto&& s_guard = lock_.LockS();

      // create incrementor threads
      for (size_t i = 0; i < kThreadNum; ++i) {
        threads.emplace_back([this]() {
          for (size_t i = 0; i < kWriteNumPerThread; i++) {
            [[maybe_unused]] auto&& x_guard = lock_.LockX();
            ++counter_;
          }
        });
      }

      // after short sleep, check that the counter has not incremented
      std::this_thread::sleep_for(kWaitTimeMill);
      EXPECT_EQ(counter_, 0);
    }

    // release the shared lock, and then wait for the increment workers
    for (auto&& t : threads) {
      t.join();
    }

    if constexpr (OptimisticallyLockable<Lock>) {
      EXPECT_FALSE(opt_guard.VerifyVersion());  // X locks modify the version
    }

    // check the counter
    [[maybe_unused]] auto&& s_guard = lock_.LockS();
    EXPECT_EQ(counter_, kThreadNum * kWriteNumPerThread);
  }

  /**
   * @brief Verify that read-modify-write procedures with SIX locks upgraded to X
   * locks protect a shared counter.
   *
   */
  void
  VerifyUpgradeToXWithMultiThread()
    requires PessimisticallyLockable<Lock>
  {
    std::vector<std::thread> threads{};
    threads.reserve(kThreadNum);
    for (size_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back([this]() {
        for (size_t i = 0; i < kWriteNumPerThread; i++) {
          auto&& six_guard = lock_.LockSIX();
          const auto val = counter_;  // SIX locks prevent other writers
          [[maybe_unused]] auto&& x_guard = six_guard.UpgradeToX();
          counter_ = val + 1;
        }
      });
    }
    for (auto&& t : threads) {
      t.join();
    }

    EXPECT_EQ(counter_, kThreadNum * kWriteNumPerThread);
  }

  /**
   * @brief Verify that shared locks never read data being modified.
   *
   */
  void
  VerifyLockSWithMultiThreadReadConsistentData()
    requires PessimisticallyLockable<Lock>
  {
    constexpr size_t kReaderNum = 2;
    size_t first{0};  // `first` and `second` must be equal under the lock
    size_t second{0};
    std::atomic_bool running{true};
    std::atomic_size_t inconsistent{0};

    std::vector<std::thread> readers{};
    for (size_t i = 0; i < kReaderNum; ++i) {
      readers.emplace_back([&] {
        while (running.load(kRelaxed)) {
          [[maybe_unused]] auto&& s_guard = lock_.LockS();
          if (first != second) {
            inconsistent.fetch_add(1, kRelaxed);
          }
        }
      });
    }

    std::vector<std::thread> writers{};
    for (size_t i = 0; i < kThreadNum; ++i) {
      writers.emplace_back([&] {
        for (size_t j = 0; j < kWriteNumPerThread; ++j) {
          [[maybe_unused]] auto&& x_guard = lock_.LockX();
          ++first;
          ++second;
        }
      });
    }
    for (auto&& t : writers) {
      t.join();
    }
    running = false;
    for (auto&& t : readers) {
      t.join();
    }

    EXPECT_EQ(inconsistent.load(), 0);
    EXPECT_EQ(first, kThreadNum * kWriteNumPerThread);
  }

  /*##########################################################################*
   * Utilities for optimistic locks
   *##########################################################################*/

  /**
   * @return An optimistic guard if the lock supports it (or a dummy value).
   */
  auto
  GetOptGuardIfExist()  //
      -> auto
  {
    if constexpr (OptimisticallyLockable<Lock>) {
      return lock_.GetVersion();
    } else {
      return std::monostate{};
    }
  }

  /**
   * @brief Release an X lock after setting a version with a flipped masked bit.
   *
   * @param ver The current version.
   */
  void
  FlipMaskedBit(  //
      const uint32_t ver)
    requires OptimisticallyLockable<Lock>
  {
    auto&& x_guard = lock_.LockX();
    x_guard.SetVersion(ver ^ kMaskedBit);
  }

  /**
   * @brief Try to get an X lock via an optimistic guard while another lock is held.
   *
   * @param lock_type A lock type to be acquired via the optimistic guard.
   * @param with_lock_type A lock type held beforehand.
   * @param expect_success true if the procedure must finish without waiting.
   * @note The optimistic guard is created before acquiring `with_lock_type`, so
   * trying to lock fails if `with_lock_type` is an X lock.
   */
  void
  VerifyTryLock(  //
      const LockType lock_type,
      const LockType with_lock_type,
      const bool expect_success)
    requires OptimisticallyLockable<Lock>
  {
    auto try_lock = [lock_type, with_lock_type](typename Lock::OptGuard opt_guard) {
      const auto expect_locked = with_lock_type != kXLock;
      switch (lock_type) {
        case kSLock:
          if constexpr (HybridLockable<Lock>) {
            EXPECT_EQ(static_cast<bool>(opt_guard.TryLockS()), expect_locked);
          }
          break;
        case kSIXLock:
          if constexpr (HybridLockable<Lock>) {
            EXPECT_EQ(static_cast<bool>(opt_guard.TryLockSIX()), expect_locked);
          }
          break;
        case kXLock:
          EXPECT_EQ(static_cast<bool>(opt_guard.TryLockX()), expect_locked);
          break;
        case kFree:
        default:
          break;
      }
    };

    auto&& opt_guard = lock_.GetVersion();
    {
      [[maybe_unused]] auto&& guard = GetLockForOptimisticTests(with_lock_type);
      RunInAnotherThread([try_lock, opt_guard = std::move(
                                        opt_guard)]() mutable { try_lock(std::move(opt_guard)); },
                         expect_success);
    }
    t_.join();
  }

  /**
   * @param lock_type A lock type to be acquired.
   * @return A guard instance for the acquired lock (or an empty instance).
   * @note Locks without shared locks only support X locks.
   */
  auto
  GetLockForOptimisticTests(     //
      const LockType lock_type)  //
      -> auto
    requires OptimisticallyLockable<Lock>
  {
    if constexpr (PessimisticallyLockable<Lock>) {
      return GetLock(lock_type);
    } else {
      using Guard = std::variant<std::monostate, XGuard>;
      if (lock_type == kXLock) {
        auto&& guard = lock_.LockX();
        EXPECT_TRUE(guard);
        return Guard{std::move(guard)};
      }
      EXPECT_EQ(lock_type, kFree);
      return Guard{};
    }
  }

  /**
   * @brief Verify a version in another thread while another lock is held.
   *
   * @param with_lock_type A lock type held beforehand.
   * @param expect_success true if the verification must finish without waiting.
   * @param expect_verified An expected result of the verification.
   */
  void
  VerifyVersionWith(  //
      const LockType with_lock_type,
      const bool expect_success,
      const bool expect_verified)
    requires OptimisticallyLockable<Lock>
  {
    std::atomic_bool verified{!expect_verified};
    {
      auto&& opt_guard = lock_.GetVersion();
      [[maybe_unused]] auto&& guard = GetLockForOptimisticTests(with_lock_type);
      RunInAnotherThread(
          [&verified, opt_guard = std::move(opt_guard)]() mutable {
            verified = opt_guard.VerifyVersion();
          },
          expect_success);
    }
    t_.join();
    EXPECT_EQ(verified.load(), expect_verified);
  }

  /**
   * @brief Verify concurrent optimistic reads never accept inconsistent data.
   *
   */
  void
  VerifyOptimisticReadsWithMultiThread()
    requires OptimisticallyLockable<Lock>
  {
    constexpr size_t kWriterNum = kThreadNum < 2 ? 2 : kThreadNum;  // queued writers are needed
    constexpr size_t kReaderNum = 2;
    std::atomic_uint64_t first{0};  // `first` and `second` must be equal under the lock
    std::atomic_uint64_t second{0};
    std::atomic_bool running{true};
    std::atomic_size_t inconsistent{0};

    std::vector<std::thread> readers{};
    for (size_t i = 0; i < kReaderNum; ++i) {
      readers.emplace_back([&] {
        while (running.load(kRelaxed)) {
          auto&& opt_guard = lock_.GetVersion();
          const auto lhs = first.load(kRelaxed);
          const auto rhs = second.load(kRelaxed);
          if (opt_guard.VerifyVersion() && lhs != rhs) {
            inconsistent.fetch_add(1, kRelaxed);
          }
        }
      });
    }

    std::vector<std::thread> writers{};
    for (size_t i = 0; i < kWriterNum; ++i) {
      writers.emplace_back([&] {
        for (size_t j = 0; j < kWriteNumPerThread; ++j) {
          [[maybe_unused]] auto&& x_guard = lock_.LockX();
          const auto val = first.load(kRelaxed) + 1;
          first.store(val, kRelaxed);
          second.store(val, kRelaxed);
        }
      });
    }
    for (auto&& t : writers) {
      t.join();
    }
    running = false;
    for (auto&& t : readers) {
      t.join();
    }

    EXPECT_EQ(inconsistent.load(), 0);
    EXPECT_EQ(first.load(), kWriterNum * kWriteNumPerThread);
  }

  /*##########################################################################*
   * Utilities for hybrid locks
   *##########################################################################*/

  /**
   * @brief Verify that an S lock acquired by falling back from optimistic reads
   * is released by the next verification.
   *
   * @tparam kImmediate Use `ImmediateVerify` instead of `VerifyVersion` for the
   * next verification.
   * @note The optimistic guard falls back to an S lock while another thread
   * holds an SIX lock. If the S lock is not released, the final X lock blocks
   * the procedure forever, so a lock instance is leaked on purpose.
   */
  template <bool kImmediate>
  static void
  VerifyFallbackSLockRelease()
    requires HybridLockable<Lock>
  {
    auto* const lock = new Lock{};  // leaked if the following procedure is blocked
    std::promise<bool> p{};
    auto f = p.get_future();
    std::thread{[lock, p = std::move(p)]() mutable {
      auto&& opt_guard = lock->GetVersion();
      {  // change the version
        [[maybe_unused]] auto&& x_guard = lock->LockX();
      }

      std::thread holder{[lock] {
        [[maybe_unused]] auto&& six_guard = lock->LockSIX();
        std::this_thread::sleep_for(kWaitTimeMill);
      }};
      std::this_thread::sleep_for(kWaitTimeMill / 5);
      const auto verified = opt_guard.VerifyVersion(kNoMask, 1);  // fall back to an S lock
      holder.join();

      // the S lock must be released by the next verification
      bool released{};
      if constexpr (kImmediate) {
        released = opt_guard.ImmediateVerify();
      } else {
        released = opt_guard.VerifyVersion();
      }
      {
        [[maybe_unused]] auto&& x_guard = lock->LockX();
      }
      p.set_value(!verified && released);
    }}.detach();

    ASSERT_EQ(f.wait_for(kWaitTimeMill * 10), std::future_status::ready);
    EXPECT_TRUE(f.get());
    delete lock;
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  Lock lock_{};

  size_t counter_{0};

  std::thread t_{};
};

/**
 * @brief A fixture for testing pessimistic locks.
 *
 * @tparam Lock A target lock class.
 */
template <PessimisticallyLockable Lock>
class PessimisticLockFixture : public LockFixture<Lock>
{
};

/**
 * @brief A fixture for testing optimistic locks.
 *
 * @tparam Lock A target lock class.
 */
template <OptimisticallyLockable Lock>
class OptimisticLockFixture : public LockFixture<Lock>
{
};

/**
 * @brief A fixture for testing hybrid (pessimistic and optimistic) locks.
 *
 * @tparam Lock A target lock class.
 */
template <HybridLockable Lock>
class HybridLockFixture : public LockFixture<Lock>
{
};

}  // namespace dbgroup::lock::test

#endif  // CPP_UTILITY_TEST_LOCK_LOCK_FIXTURE_HPP_
