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

#ifndef CPP_UTILITY_DBGROUP_THREAD_COMMON_HPP_
#define CPP_UTILITY_DBGROUP_THREAD_COMMON_HPP_

// C++ standard libraries
#include <atomic>
#include <cstddef>

namespace dbgroup::thread
{
/*##############################################################################
 * Global constants
 *############################################################################*/

/// @brief An alias of the acquire memory order.
constexpr std::memory_order kAcquire = std::memory_order_acquire;

/// @brief An alias of the release memory order.
constexpr std::memory_order kRelease = std::memory_order_release;

/// @brief An alias of the relaxed memory order.
constexpr std::memory_order kRelaxed = std::memory_order_relaxed;

/// @brief The maximum number of threads used in a process.
constexpr size_t kMaxThreadNum = DBGROUP_MAX_THREAD_NUM;  // NOLINT

/// @brief The expected cache-line size.
constexpr size_t kCashLineSize = 64;

}  // namespace dbgroup::thread

#endif  // CPP_UTILITY_DBGROUP_THREAD_COMMON_HPP_
