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

/// @brief The `uintptr_t` of nullptr.
constexpr uint64_t kNull = 0;

/// @brief The begin bit position of shared locks.
constexpr uint64_t kSPos = 47;

/// @brief The bit position of  a shared-with-intent-exclusive lock.
constexpr uint64_t kSIXPos = 62;

/// @brief The bet position of an exclusive lock.
constexpr uint64_t kXPos = 63;

/// @brief A lock state representing no locks.
constexpr uint64_t kNoLocks = 0b000;

/// @brief A lock state representing a shared lock.
constexpr uint64_t kSLock = 1UL << kSPos;

/// @brief A lock state representing a shared-with-intent-exclusive lock.
constexpr uint64_t kSIXLock = 1UL << kSIXPos;

/// @brief A lock state representing an exclusive lock.
constexpr uint64_t kXLock = 1UL << kXPos;

/// @brief A bit mask for extracting a node pointer.
constexpr uint64_t kPtrMask = kSLock - 1UL;

/// @brief A bit mask for extracting a lock state.
constexpr uint64_t kLockMask = ~kPtrMask;

/// @brief A bit mask for extracting an SIX/X-lock state.
constexpr uint64_t kSIXAndXMask = kSIXLock | kXLock;

/// @brief A bit mask for extracting a sharedlock state.
constexpr uint64_t kSMask = kLockMask & ~kSIXAndXMask;

}  // namespace

namespace dbgroup::lock
{
/*##############################################################################
 * Public APIs
 *############################################################################*/

auto
MCSLock::LockS()  //
    -> MCSLock *
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::UnlockS(  //
    MCSLock *qnode)
{
  throw std::runtime_error{"not implemented yet."};
}

auto
MCSLock::LockX()  //
    -> MCSLock *
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::DowngradeToSIX(  //
    MCSLock *qnode)
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::UnlockX(  //
    MCSLock *qnode)
{
  throw std::runtime_error{"not implemented yet."};
}

auto
MCSLock::LockSIX()  //
    -> MCSLock *
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::UpgradeToX(  //
    MCSLock *qnode)
{
  throw std::runtime_error{"not implemented yet."};
}

void
MCSLock::UnlockSIX(  //
    MCSLock *qnode)
{
  throw std::runtime_error{"not implemented yet."};
}

}  // namespace dbgroup::lock
