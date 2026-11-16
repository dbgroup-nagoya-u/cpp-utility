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

// external libraries
#include <gtest/gtest.h>

// local sources
#include "common.hpp"
#include "dbgroup/lock/omcs_lock.hpp"
#include "dbgroup/lock/optimistic_lock.hpp"
#include "lock_fixture.hpp"

namespace dbgroup::lock::test
{
/*############################################################################*
 * Preparation for typed testing
 *############################################################################*/

using HybridLocks = ::testing::Types<OptimisticLock, OMCSLock>;
TYPED_TEST_SUITE(HybridLockFixture, HybridLocks, LockNameGenerator);

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

/*----------------------------------------------------------------------------*
 * Shared lock tests via optimistic guards
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSWithoutLocksSucceed)
{
  this->VerifyTryLock(kSLock, kFree, kExpectSucceed);
}

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSWithSLockSucceed)
{
  this->VerifyTryLock(kSLock, kSLock, kExpectSucceed);
}

// some locks (e.g., FIFO queue locks) make shared locks wait for SIX locks
TYPED_TEST(  //
    HybridLockFixture,
    TryLockSWithSIXLock)
{
  this->VerifyTryLock(kSLock, kSIXLock, !LockTraits<TypeParam>::kSIXBlocksS);
}

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSWithXLockNeedWait)
{
  this->VerifyTryLock(kSLock, kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Shared-with-intent-exclusive lock tests via optimistic guards
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSIXWithoutLocksSucceed)
{
  this->VerifyTryLock(kSIXLock, kFree, kExpectSucceed);
}

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSIXWithSLockSucceed)
{
  this->VerifyTryLock(kSIXLock, kSLock, kExpectSucceed);
}

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSIXWithSIXLockNeedWait)
{
  this->VerifyTryLock(kSIXLock, kSIXLock, kExpectFail);
}

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSIXWithXLockNeedWait)
{
  this->VerifyTryLock(kSIXLock, kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Exclusive lock tests via optimistic guards
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    HybridLockFixture,
    TryLockXWithSLockNeedWait)
{
  this->VerifyTryLock(kXLock, kSLock, kExpectFail);
}

TYPED_TEST(  //
    HybridLockFixture,
    TryLockXWithSIXLockNeedWait)
{
  this->VerifyTryLock(kXLock, kSIXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Version verification tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    HybridLockFixture,
    VerifyVersionWithSLockSucceed)
{
  this->VerifyVersionWith(kSLock, kExpectSucceed, true);
}

TYPED_TEST(  //
    HybridLockFixture,
    VerifyVersionWithSIXLockSucceed)
{
  this->VerifyVersionWith(kSIXLock, kExpectSucceed, true);
}

// the optimistic guard falls back to an S lock, and the next verification must
// release it
TYPED_TEST(  //
    HybridLockFixture,
    VerifyVersionWithFallbackSLockReleaseIt)
{
  TestFixture::template VerifyFallbackSLockRelease<false>();
}

TYPED_TEST(  //
    HybridLockFixture,
    ImmediateVerifyWithFallbackSLockReleaseIt)
{
  if constexpr (ImmediatelyVerifiable<TypeParam>) {
    TestFixture::template VerifyFallbackSLockRelease<true>();
  } else {
    GTEST_SKIP() << "ImmediateVerify is not implemented.";
  }
}

/*----------------------------------------------------------------------------*
 * Lock tests via optimistic guards with version changes
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSWithVersionChangesFail)
{
  auto&& opt_guard = this->lock_.GetVersion();
  {
    [[maybe_unused]] auto&& x_guard = this->lock_.LockX();
  }
  const auto cur_ver = this->lock_.GetVersion().GetVersion();

  EXPECT_FALSE(opt_guard.TryLockS());
  EXPECT_EQ(opt_guard.GetVersion(), cur_ver);  // the guard has the current version
  this->RunInAnotherThread([this] { return this->lock_.LockX(); }, kExpectSucceed);
  this->t_.join();
}

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSIXWithVersionChangesFail)
{
  auto&& opt_guard = this->lock_.GetVersion();
  {
    [[maybe_unused]] auto&& x_guard = this->lock_.LockX();
  }
  const auto cur_ver = this->lock_.GetVersion().GetVersion();

  EXPECT_FALSE(opt_guard.TryLockSIX());
  EXPECT_EQ(opt_guard.GetVersion(), cur_ver);  // the guard has the current version
  this->RunInAnotherThread([this] { return this->lock_.LockX(); }, kExpectSucceed);
  this->t_.join();
}

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSWithMaskIgnoreMaskedBits)
{
  auto&& opt_guard = this->lock_.GetVersion();
  this->FlipMaskedBit(opt_guard.GetVersion());
  const auto cur_ver = this->lock_.GetVersion().GetVersion();

  auto&& s_guard = opt_guard.TryLockS(~kMaskedBit);
  ASSERT_TRUE(s_guard);
  EXPECT_EQ(s_guard.GetVersion(), cur_ver);
}

TYPED_TEST(  //
    HybridLockFixture,
    TryLockSIXWithMaskIgnoreMaskedBits)
{
  auto&& opt_guard = this->lock_.GetVersion();
  this->FlipMaskedBit(opt_guard.GetVersion());
  const auto cur_ver = this->lock_.GetVersion().GetVersion();

  auto&& six_guard = opt_guard.TryLockSIX(~kMaskedBit);
  ASSERT_TRUE(six_guard);
  EXPECT_EQ(six_guard.GetVersion(), cur_ver);
}

/*----------------------------------------------------------------------------*
 * Version management tests of pessimistic locks
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    HybridLockFixture,
    LockSWithoutLocksKeepVersion)
{
  const auto ver = this->lock_.GetVersion().GetVersion();
  {
    auto&& s_guard = this->lock_.LockS();
    EXPECT_EQ(s_guard.GetVersion(), ver);
  }
  EXPECT_EQ(this->lock_.GetVersion().GetVersion(), ver);
}

TYPED_TEST(  //
    HybridLockFixture,
    LockSIXWithoutLocksKeepVersion)
{
  const auto ver = this->lock_.GetVersion().GetVersion();
  {
    auto&& six_guard = this->lock_.LockSIX();
    EXPECT_EQ(six_guard.GetVersion(), ver);
  }
  EXPECT_EQ(this->lock_.GetVersion().GetVersion(), ver);
}

TYPED_TEST(  //
    HybridLockFixture,
    UpgradeToXWithoutLocksIncrementVersion)
{
  const auto ver = this->lock_.GetVersion().GetVersion();
  {
    auto&& x_guard = this->lock_.LockSIX().UpgradeToX();
    ASSERT_TRUE(x_guard);
    EXPECT_EQ(x_guard.GetVersion(), ver);
  }
  EXPECT_EQ(this->lock_.GetVersion().GetVersion(), ver + 1);
}

// downgrading publishes a new version while keeping the SIX lock
TYPED_TEST(  //
    HybridLockFixture,
    DowngradeToSIXWithSetVersionPublishVersion)
{
  const auto new_ver = this->lock_.GetVersion().GetVersion() + 10;
  {
    auto&& x_guard = this->lock_.LockX();
    x_guard.SetVersion(new_ver);
    auto&& six_guard = x_guard.DowngradeToSIX();
    ASSERT_TRUE(six_guard);
    EXPECT_EQ(six_guard.GetVersion(), new_ver);

    // optimistic reads do not wait for SIX locks
    this->RunInAnotherThread([this] { return this->lock_.GetVersion(); }, kExpectSucceed);
    this->t_.join();
    EXPECT_EQ(this->lock_.GetVersion().GetVersion(), new_ver);
  }
  EXPECT_EQ(this->lock_.GetVersion().GetVersion(), new_ver);
}

}  // namespace dbgroup::lock::test
