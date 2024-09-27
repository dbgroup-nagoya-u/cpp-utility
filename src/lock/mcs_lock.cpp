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

// corresponding header
#include "dbgroup/lock/mcs_lock.hpp"

// C++ standard libraries
#include <atomic>
#include <cstdint>
#include <thread>

// temp
#include <stdexcept>

// local sources
#include "dbgroup/lock/common.hpp"

namespace
{
/*##############################################################################
 * Local constants
 *############################################################################*/

/// @brief The begin bit position of shared locks.
constexpr uint64_t kSLockPos = 47;

/// @brief The bit position of  a shared-with-intent-exclusive lock.
constexpr uint64_t kSIXLockPos = 62;

/// @brief The bet position of an exclusive lock.
constexpr uint64_t kXLockPos = 63;

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << kSLockPos;

/// @brief A lock state representing a shared-with-intent-exclusive lock.
constexpr uint64_t kSIXLock = 1UL << kSIXLockPos;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << kXLockPos;

/// @brief A bit mask for extracting an SIX/X-lock state.
constexpr uint64_t kPtrMask = kSLock - 1UL;

/// @brief A bit mask for extracting an SIX/X-lock state.
constexpr uint64_t kSIXAndXMask = kSIXLock | kXLock;

}  // namespace

namespace dbgroup::lock
{
/*##############################################################################
 * Public APIs
 *############################################################################*/

void
MCSLock::LockS()
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::UnlockS()
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::LockX()
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::DowngradeToSIX()
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::UnlockX()
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::LockSIX()
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::UpgradeToX()
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::UnlockSIX()
{
  throw std::runtime_error{"not implemented yet."};
}

/*##############################################################################
 * Internal APIs
 *############################################################################*/

auto
GetNode()  //
    -> MCSLock*
{
  thread_local MCSLock* node{nullptr};
  if (node == nullptr) {
    node = new MCSLock{};
  }
  return node;
}

}  // namespace dbgroup::lock
