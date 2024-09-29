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

#ifndef CPP_UTILITY_DBGROUP_LOCK_COMMON_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_COMMON_HPP_

// C++ standard libraries
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <thread>

// define spinlock hints if exist
#ifdef CPP_UTILITY_HAS_SPINLOCK_HINT
#include <x86intrin.h>
#define CPP_UTILITY_SPINLOCK_HINT _mm_pause();
#else
#define CPP_UTILITY_SPINLOCK_HINT /* do nothing */
#endif

namespace dbgroup::lock
{
/*##############################################################################
 * Global constants
 *############################################################################*/

/// @brief An alias of the release memory order.
constexpr std::memory_order kRelease = std::memory_order_release;

/// @brief An alias of the acquire memory order.
constexpr std::memory_order kAcquire = std::memory_order_acquire;

/// @brief An alias of the relaxed memory order.
constexpr std::memory_order kRelaxed = std::memory_order_relaxed;

/// @brief The maximum number of retries for preventing busy loops.
constexpr size_t kRetryNum{CPP_UTILITY_SPINLOCK_RETRY_NUM};

/// @brief A back-off time interval for preventing busy loops.
constexpr std::chrono::microseconds kBackOffTime{CPP_UTILITY_BACKOFF_TIME};

/*##############################################################################
 * Internal utilities
 *############################################################################*/

/**
 * @brief Execute a given procedure with spinning and backoff.
 *
 * @param proc A target procedure.
 * @param args Arguments for executing a given procedure.
 * @tparam Func A function pointer.
 * @tparam Args A parameter pack for calling a given function.
 */
template <class Func, class... Args>
void
SpinWithBackoff(  //
    Func proc,
    Args... args)
{
  while (true) {
    for (size_t i = 0; true; ++i) {
      if (proc(args...)) return;
      if (i >= kRetryNum) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
    std::this_thread::sleep_for(kBackOffTime);
  }
}

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_COMMON_HPP_
