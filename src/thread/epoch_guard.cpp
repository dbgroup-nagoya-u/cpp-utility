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

// the corresponding header
#include "dbgroup/thread/epoch_guard.hpp"

// C++ standard libraries
#include <utility>

namespace dbgroup::thread
{
EpochGuard::EpochGuard(  //
    Epoch *epoch) noexcept
    : epoch_{epoch}
{
  epoch_->EnterEpoch();
}

auto
EpochGuard::operator=(          //
    EpochGuard &&rhs) noexcept  //
    -> EpochGuard &
{
  if (epoch_) {
    epoch_->LeaveEpoch();
  }
  epoch_ = std::exchange(rhs.epoch_, nullptr);
  return *this;
}

EpochGuard::~EpochGuard()
{
  if (epoch_) {
    epoch_->LeaveEpoch();
  }
}

auto
EpochGuard::GetProtectedEpoch() const noexcept  //
    -> size_t
{
  return epoch_->GetProtectedEpoch();
}

}  // namespace dbgroup::thread
