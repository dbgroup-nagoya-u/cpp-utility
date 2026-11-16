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
#include "dbgroup/lock/optiql.hpp"
#include "dbgroup/lock/utility.hpp"
#include "lock_fixture.hpp"

namespace dbgroup::lock::test
{
/*############################################################################*
 * Preparation for typed testing
 *############################################################################*/

using OptimisticLocks = ::testing::Types<OptimisticLock, OMCSLock, OptiQL>;
TYPED_TEST_SUITE(OptimisticLockFixture, OptimisticLocks, LockNameGenerator);

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

/*----------------------------------------------------------------------------*
 * Exclusive lock tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    OptimisticLockFixture,
    LockXWithoutLocksSucceed)
{
  this->RunInAnotherThread([this] { return this->lock_.LockX(); }, kExpectSucceed);
  this->t_.join();
}

TYPED_TEST(  //
    OptimisticLockFixture,
    LockXWithXLockNeedWait)
{
  {
    [[maybe_unused]] auto&& x_guard = this->lock_.LockX();
    this->RunInAnotherThread([this] { return this->lock_.LockX(); }, kExpectFail);
  }
  this->t_.join();
}

TYPED_TEST(  //
    OptimisticLockFixture,
    LockXWithoutLocksIncrementVersion)
{
  const auto ver = this->lock_.GetVersion().GetVersion();
  {
    auto&& x_guard = this->lock_.LockX();
    EXPECT_EQ(x_guard.GetVersion(), ver);
  }
  EXPECT_EQ(this->lock_.GetVersion().GetVersion(), ver + 1);
}

TYPED_TEST(  //
    OptimisticLockFixture,
    SetVersionWithXLockModifyVersion)
{
  const auto new_ver = this->lock_.GetVersion().GetVersion() + 10;
  {
    auto&& x_guard = this->lock_.LockX();
    x_guard.SetVersion(new_ver);
  }
  EXPECT_EQ(this->lock_.GetVersion().GetVersion(), new_ver);
}

/*----------------------------------------------------------------------------*
 * Optimistic guard tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    OptimisticLockFixture,
    GetVersionWithoutLocksReturnValidGuard)
{
  EXPECT_FALSE(typename TypeParam::OptGuard{});
  EXPECT_TRUE(this->lock_.GetVersion());
}

TYPED_TEST(  //
    OptimisticLockFixture,
    GetVersionWithXLockNeedWait)
{
  {
    [[maybe_unused]] auto&& x_guard = this->lock_.LockX();
    this->RunInAnotherThread([this] { return this->lock_.GetVersion(); }, kExpectFail);
  }
  this->t_.join();
}

/*----------------------------------------------------------------------------*
 * Version verification tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    OptimisticLockFixture,
    VerifyVersionWithoutLocksSucceed)
{
  this->VerifyVersionWith(kFree, kExpectSucceed, true);
}

// verification waits for the X lock and fails because the version is modified
TYPED_TEST(  //
    OptimisticLockFixture,
    VerifyVersionWithXLockNeedWait)
{
  this->VerifyVersionWith(kXLock, kExpectFail, false);
}

TYPED_TEST(  //
    OptimisticLockFixture,
    VerifyVersionWithMaskIgnoreMaskedBits)
{
  auto&& masked_guard = this->lock_.GetVersion();
  auto&& unmasked_guard = this->lock_.GetVersion();
  this->FlipMaskedBit(masked_guard.GetVersion());

  EXPECT_TRUE(masked_guard.VerifyVersion(~kMaskedBit));
  EXPECT_FALSE(unmasked_guard.VerifyVersion(kNoMask));
  EXPECT_TRUE(unmasked_guard.VerifyVersion(kNoMask));  // the version was updated
}

/*----------------------------------------------------------------------------*
 * Immediate verification tests (only for locks implementing them)
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    OptimisticLockFixture,
    ImmediateVerifyWithoutLocksSucceed)
{
  if constexpr (ImmediatelyVerifiable<TypeParam>) {
    auto&& opt_guard = this->lock_.GetVersion();
    EXPECT_TRUE(opt_guard.ImmediateVerify());
  } else {
    GTEST_SKIP() << "ImmediateVerify is not implemented.";
  }
}

// immediate verification does not wait for X locks
TYPED_TEST(  //
    OptimisticLockFixture,
    ImmediateVerifyWithXLockFail)
{
  if constexpr (ImmediatelyVerifiable<TypeParam>) {
    auto&& opt_guard = this->lock_.GetVersion();
    [[maybe_unused]] auto&& x_guard = this->lock_.LockX();
    EXPECT_FALSE(opt_guard.ImmediateVerify());
  } else {
    GTEST_SKIP() << "ImmediateVerify is not implemented.";
  }
}

TYPED_TEST(  //
    OptimisticLockFixture,
    ImmediateVerifyWithMaskIgnoreMaskedBits)
{
  if constexpr (ImmediatelyVerifiable<TypeParam>) {
    auto&& masked_guard = this->lock_.GetVersion();
    auto&& unmasked_guard = this->lock_.GetVersion();
    this->FlipMaskedBit(masked_guard.GetVersion());

    EXPECT_TRUE(masked_guard.ImmediateVerify(~kMaskedBit));
    EXPECT_FALSE(unmasked_guard.ImmediateVerify(kNoMask));
  } else {
    GTEST_SKIP() << "ImmediateVerify is not implemented.";
  }
}

/*----------------------------------------------------------------------------*
 * Exclusive lock tests via optimistic guards
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    OptimisticLockFixture,
    TryLockXWithoutLocksSucceed)
{
  this->VerifyTryLock(kXLock, kFree, kExpectSucceed);
}

// the X lock is acquired after waiting, but released because of version changes
TYPED_TEST(  //
    OptimisticLockFixture,
    TryLockXWithXLockNeedWait)
{
  this->VerifyTryLock(kXLock, kXLock, kExpectFail);
}

TYPED_TEST(  //
    OptimisticLockFixture,
    TryLockXWithoutVersionChangesModifyVersion)
{
  auto&& opt_guard = this->lock_.GetVersion();
  const auto ver = opt_guard.GetVersion();
  {
    auto&& x_guard = opt_guard.TryLockX();
    ASSERT_TRUE(x_guard);
    EXPECT_EQ(x_guard.GetVersion(), ver);
  }

  EXPECT_NE(this->lock_.GetVersion().GetVersion(), ver);
}

TYPED_TEST(  //
    OptimisticLockFixture,
    TryLockXWithVersionChangesFail)
{
  auto&& opt_guard = this->lock_.GetVersion();
  {
    [[maybe_unused]] auto&& x_guard = this->lock_.LockX();
  }
  const auto cur_ver = this->lock_.GetVersion().GetVersion();

  EXPECT_FALSE(opt_guard.TryLockX());
  EXPECT_EQ(opt_guard.GetVersion(), cur_ver);                 // the guard has the current version
  EXPECT_EQ(this->lock_.GetVersion().GetVersion(), cur_ver);  // the version is not modified
}

TYPED_TEST(  //
    OptimisticLockFixture,
    TryLockXWithMaskIgnoreMaskedBits)
{
  auto&& opt_guard = this->lock_.GetVersion();
  this->FlipMaskedBit(opt_guard.GetVersion());

  EXPECT_TRUE(opt_guard.TryLockX(~kMaskedBit));
}

/*----------------------------------------------------------------------------*
 * Multi-thread tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    OptimisticLockFixture,
    VerifyVersionWithMultiThreadNeverAcceptInconsistentData)
{
  this->VerifyOptimisticReadsWithMultiThread();
}

}  // namespace dbgroup::lock::test
