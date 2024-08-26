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

// the correspnding header
#include "dbgroup/thread/component/epoch.hpp"

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <limits>

// local sources
#include "dbgroup/thread/common.hpp"

namespace dbgroup::thread::component
{
/*##############################################################################
 * Public getters/setters
 *############################################################################*/

auto
Epoch::GetCurrentEpoch() const  //
    -> size_t
{
  return current_->load(kAcquire);
}

auto
Epoch::GetProtectedEpoch() const  //
    -> size_t
{
  return entered_.load(kRelaxed);
}

void
Epoch::SetGrobalEpoch(  //
    std::atomic_size_t *global_epoch)
{
  current_ = global_epoch;
}

/*##############################################################################
 * Public utility functions
 *############################################################################*/

void
Epoch::EnterEpoch()
{
  entered_.store(GetCurrentEpoch(), kRelaxed);
}

void
Epoch::LeaveEpoch()
{
  entered_.store(std::numeric_limits<size_t>::max(), kRelaxed);
}

}  // namespace dbgroup::thread::component
