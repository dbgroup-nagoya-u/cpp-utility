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

// C++ standard libraries
#include <utility>

// external libraries
#include <gtest/gtest.h>

// local sources
#include "common.hpp"
#include "dbgroup/lock/mcs_lock.hpp"
#include "dbgroup/lock/omcs_lock.hpp"
#include "dbgroup/lock/optimistic_lock.hpp"
#include "dbgroup/lock/pessimistic_lock.hpp"
#include "lock_fixture.hpp"

namespace dbgroup::lock::test
{
/*############################################################################*
 * Preparation for typed testing
 *############################################################################*/

using PessimisticLocks = ::testing::Types<PessimisticLock, MCSLock, OptimisticLock, OMCSLock>;
TYPED_TEST_SUITE(PessimisticLockFixture, PessimisticLocks, LockNameGenerator);

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

/*----------------------------------------------------------------------------*
 * Shared lock tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSWithoutLocksSucceed)
{
  this->VerifyLock(kSLock, kFree, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSWithSLockSucceed)
{
  this->VerifyLock(kSLock, kSLock, kExpectSucceed);
}

// some locks (e.g., FIFO queue locks) make shared locks wait for SIX locks
TYPED_TEST(  //
    PessimisticLockFixture,
    LockSWithSIXLock)
{
  this->VerifyLock(kSLock, kSIXLock, !LockTraits<TypeParam>::kSIXBlocksS);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSWithXLockNeedWait)
{
  this->VerifyLock(kSLock, kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Shared-with-intent-exclusive lock tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSIXWithoutLocksSucceed)
{
  this->VerifyLock(kSIXLock, kFree, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSIXWithSLockSucceed)
{
  this->VerifyLock(kSIXLock, kSLock, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSIXWithSIXLockNeedWait)
{
  this->VerifyLock(kSIXLock, kSIXLock, kExpectFail);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSIXWithXLockNeedWait)
{
  this->VerifyLock(kSIXLock, kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Exclusive lock tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    PessimisticLockFixture,
    LockXWithoutLocksSucceed)
{
  this->VerifyLock(kXLock, kFree, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockXWithSLockNeedWait)
{
  this->VerifyLock(kXLock, kSLock, kExpectFail);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockXWithSIXLockNeedWait)
{
  this->VerifyLock(kXLock, kSIXLock, kExpectFail);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockXWithXLockNeedWait)
{
  this->VerifyLock(kXLock, kXLock, kExpectFail);
}

/*----------------------------------------------------------------------------*
 * Downgrade/Upgrade tests
 *----------------------------------------------------------------------------*/

// some locks (e.g., FIFO queue locks) make shared locks wait for SIX locks
TYPED_TEST(  //
    PessimisticLockFixture,
    LockSWithDowngradedSIXLock)
{
  this->VerifyLockWithDowngradedSIXLock(kSLock, !LockTraits<TypeParam>::kSIXBlocksS);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSIXWithDowngradedSIXLockNeedWait)
{
  this->VerifyLockWithDowngradedSIXLock(kSIXLock, kExpectFail);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockXWithDowngradedSIXLockNeedWait)
{
  this->VerifyLockWithDowngradedSIXLock(kXLock, kExpectFail);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    UpgradeToXWithoutLocksSucceed)
{
  this->VerifyUpgradeToX(kFree, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    UpgradeToXWithSLockNeedWait)
{
  this->VerifyUpgradeToX(kSLock, kExpectFail);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    UpgradeToXWithDowngradedSIXLockHoldXLock)
{
  {
    auto&& x_guard = this->lock_.LockX().DowngradeToSIX().UpgradeToX();
    ASSERT_TRUE(x_guard);
    this->RunInAnotherThread([this] { return this->lock_.LockS(); }, kExpectFail);
  }
  this->t_.join();
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSWithSLockDowngradedFromSIXSucceed)
{
  this->VerifyLockWithDowngradedSLock(kSLock, kSIXLock, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSIXWithSLockDowngradedFromSIXSucceed)
{
  this->VerifyLockWithDowngradedSLock(kSIXLock, kSIXLock, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockXWithSLockDowngradedFromSIXNeedWait)
{
  this->VerifyLockWithDowngradedSLock(kXLock, kSIXLock, kExpectFail);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSWithSLockDowngradedFromXSucceed)
{
  this->VerifyLockWithDowngradedSLock(kSLock, kXLock, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSIXWithSLockDowngradedFromXSucceed)
{
  this->VerifyLockWithDowngradedSLock(kSIXLock, kXLock, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockXWithSLockDowngradedFromXNeedWait)
{
  this->VerifyLockWithDowngradedSLock(kXLock, kXLock, kExpectFail);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    DowngradeToSWithoutLocksSucceed)
{
  this->VerifyDowngradeToS(kFree, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    DowngradeToSWithSLockSucceed)
{
  this->VerifyDowngradeToS(kSLock, kExpectSucceed);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    DowngradeToSWithDowngradedSIXLockHoldSLock)
{
  {
    auto&& s_guard = this->lock_.LockX().DowngradeToSIX().DowngradeToS();
    ASSERT_TRUE(s_guard);
    this->RunInAnotherThread([this] { return this->lock_.LockX(); }, kExpectFail);
  }
  this->t_.join();
}

/*----------------------------------------------------------------------------*
 * Guard tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    PessimisticLockFixture,
    UpgradeToXWithEmptyGuardReturnEmptyGuard)
{
  typename TypeParam::SIXGuard six_guard{};
  EXPECT_FALSE(six_guard);
  EXPECT_FALSE(six_guard.UpgradeToX());
}

TYPED_TEST(  //
    PessimisticLockFixture,
    DowngradeToSIXWithEmptyGuardReturnEmptyGuard)
{
  typename TypeParam::XGuard x_guard{};
  EXPECT_FALSE(x_guard);
  EXPECT_FALSE(x_guard.DowngradeToSIX());
}

TYPED_TEST(  //
    PessimisticLockFixture,
    DowngradeSIXToSWithEmptyGuardReturnEmptyGuard)
{
  typename TypeParam::SIXGuard six_guard{};
  EXPECT_FALSE(six_guard);
  EXPECT_FALSE(six_guard.DowngradeToS());
}

TYPED_TEST(  //
    PessimisticLockFixture,
    DowngradeXToSWithEmptyGuardReturnEmptyGuard)
{
  typename TypeParam::XGuard x_guard{};
  EXPECT_FALSE(x_guard);
  EXPECT_FALSE(x_guard.DowngradeToS());
}

TYPED_TEST(  //
    PessimisticLockFixture,
    MoveConstructionWithXLockTransferOwnership)
{
  {
    auto&& src = this->lock_.LockX();
    auto dst = std::move(src);
    EXPECT_FALSE(src);  // NOLINT(bugprone-use-after-move,hicpp-invalid-access-moved)
    EXPECT_TRUE(dst);
    this->RunInAnotherThread([this] { return this->lock_.LockX(); }, kExpectFail);
  }
  this->t_.join();
}

TYPED_TEST(  //
    PessimisticLockFixture,
    MoveAssignmentWithXLockReleaseOldLock)
{
  auto&& x_guard = this->lock_.LockX();
  this->RunInAnotherThread([this] { return this->lock_.LockX(); }, kExpectFail);
  x_guard = typename TypeParam::XGuard{};  // release the held lock
  this->t_.join();
  EXPECT_FALSE(x_guard);
}

TYPED_TEST(  //
    PessimisticLockFixture,
    MoveAssignmentWithSLockReleaseOldLock)
{
  auto&& s_guard = this->lock_.LockS();
  this->RunInAnotherThread([this] { return this->lock_.LockX(); }, kExpectFail);
  s_guard = typename TypeParam::SGuard{};  // release the held lock
  this->t_.join();
  EXPECT_FALSE(s_guard);
}

/*----------------------------------------------------------------------------*
 * Multi-thread tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSWithMultiThreadManageSharedLockCounter)
{
  this->VerifyLockSWithMultiThread();
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockXWithMultiThreadKeepConsistentCounter)
{
  this->VerifyLockXWithMultiThread();
}

TYPED_TEST(  //
    PessimisticLockFixture,
    UpgradeToXWithMultiThreadKeepConsistentCounter)
{
  this->VerifyUpgradeToXWithMultiThread();
}

TYPED_TEST(  //
    PessimisticLockFixture,
    DowngradeToSWithMultiThreadKeepConsistentCounter)
{
  this->VerifyDowngradeToSWithMultiThread();
}

TYPED_TEST(  //
    PessimisticLockFixture,
    LockSWithMultiThreadNeverReadInconsistentData)
{
  this->VerifyLockSWithMultiThreadReadConsistentData();
}

}  // namespace dbgroup::lock::test
