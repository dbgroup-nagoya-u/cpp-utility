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

// the corresponding header
#include "dbgroup/thread/epoch_guard.hpp"

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>

// external libraries
#include "gtest/gtest.h"

// library headers
#include "dbgroup/thread/component/epoch.hpp"

namespace dbgroup::thread::test
{
class EpochGuardFixture : public ::testing::Test
{
 protected:
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using Epoch = component::Epoch;

  /*############################################################################
   * Constants
   *##########################################################################*/

  static constexpr size_t kULMax = std::numeric_limits<size_t>::max();

  /*############################################################################
   * Test setup/teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    current_epoch_ = 0;
    epoch_ = std::make_unique<Epoch>();
    epoch_->SetGlobalEpoch(&current_epoch_);
  }

  void
  TearDown() override
  {
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  std::atomic_size_t current_epoch_{};

  std::unique_ptr<Epoch> epoch_{nullptr};
};

/*##############################################################################
 * Unit test definitions
 *############################################################################*/

TEST_F(EpochGuardFixture, ConstructorWithCurrentEpochProtectEpoch)
{
  const EpochGuard guard{epoch_.get()};

  EXPECT_EQ(0, epoch_->GetProtectedEpoch());
}

TEST_F(EpochGuardFixture, DestructorWithCurrentEpochUnprotectEpoch)
{
  {
    const EpochGuard guard{epoch_.get()};
  }

  EXPECT_EQ(kULMax, epoch_->GetProtectedEpoch());
}

TEST_F(EpochGuardFixture, MoveConstructorKeepEpochProtected)
{
  EpochGuard guard{epoch_.get()};
  [[maybe_unused]] EpochGuard moved{std::move(guard)};

  EXPECT_EQ(0, epoch_->GetProtectedEpoch());
}

TEST_F(EpochGuardFixture, MoveAssignmentKeepEpochProtected)
{
  EpochGuard guard{epoch_.get()};
  [[maybe_unused]] auto &&moved = std::move(guard);

  EXPECT_EQ(0, epoch_->GetProtectedEpoch());
}

}  // namespace dbgroup::thread::test
