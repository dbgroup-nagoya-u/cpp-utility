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
#include <atomic>
#include <utility>

// local sources
#include "dbgroup/constants.hpp"

namespace dbgroup::thread
{
EpochGuard::EpochGuard(  //
    std::atomic_bool *active) noexcept
    : active_{active}
{
  active_->store(true, kRelease);
}

auto
EpochGuard::operator=(          //
    EpochGuard &&rhs) noexcept  //
    -> EpochGuard &
{
  if (active_) {
    active_->store(false, kRelaxed);
  }
  active_ = std::exchange(rhs.active_, nullptr);
  return *this;
}

EpochGuard::~EpochGuard()
{
  if (active_) {
    active_->store(false, kRelaxed);
  }
}

}  // namespace dbgroup::thread
