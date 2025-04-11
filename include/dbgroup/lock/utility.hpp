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

#ifndef CPP_UTILITY_DBGROUP_LOCK_UTILITY_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_UTILITY_HPP_

// C++ standard libraries
#include <atomic>
#include <chrono>
#include <concepts>
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
/*############################################################################*
 * Lock concepts
 *############################################################################*/

/**
 * @brief A concept for representing guard classes.
 *
 * @tparam T A target class.
 */
template <class T>
concept GuardClass = requires(T &x) {
  { static_cast<bool>(x) } -> std::convertible_to<bool>;
};

/**
 * @brief A concept for representing pessimistic locks.
 *
 * @tparam T A target class.
 */
template <class T>
concept PessimisticallyLockable = requires(T &x) {
  typename T::SGuard;
  typename T::SIXGuard;
  typename T::XGuard;
  GuardClass<typename T::SGuard>;
  GuardClass<typename T::SIXGuard>;
  GuardClass<typename T::XGuard>;

  // public APIs
  { x.LockS() } -> std::convertible_to<typename T::SGuard>;
  { x.LockSIX() } -> std::convertible_to<typename T::SIXGuard>;
  { x.LockX() } -> std::convertible_to<typename T::XGuard>;
};

/**
 * @brief A concept for representing optimistic locks.
 *
 * @tparam T A target class.
 */
template <class T>
concept OptimisticallyLockable = requires(T &x, uint32_t ver, uint32_t mask) {
  typename T::XGuard;
  typename T::OptGuard;
  GuardClass<typename T::XGuard>;
  GuardClass<typename T::OptGuard>;

  // public APIs
  { x.LockX() } -> std::convertible_to<typename T::XGuard>;
  { x.GetVersion() } -> std::convertible_to<typename T::OptGuard>;

  // XGuard's APIs
  { x.LockX().GetVersion() } -> std::convertible_to<uint32_t>;
  x.LockX().SetVersion(ver);

  // OptGuard's APIs
  { x.GetVersion().GetVersion() } -> std::convertible_to<uint32_t>;
  { x.GetVersion().VerifyVersion(mask) } -> std::convertible_to<bool>;
  { x.GetVersion().TryLockX(mask) } -> std::convertible_to<typename T::XGuard>;
};

/**
 * @brief A concept for representing locks.
 *
 * @tparam T A target class.
 */
template <class T>
concept Lockable = PessimisticallyLockable<T> || OptimisticallyLockable<T>;

/*############################################################################*
 * Global constants
 *############################################################################*/

/// @brief The maximum number of retries for preventing busy loops.
constexpr size_t kRetryNum{CPP_UTILITY_SPINLOCK_RETRY_NUM};

/// @brief A back-off time interval for preventing busy loops.
constexpr std::chrono::microseconds kBackOffTime{CPP_UTILITY_BACKOFF_TIME};

/// @brief A bitmask for extracting all 32 bits.
constexpr uint32_t kNoMask = ~0U;

/*############################################################################*
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

#endif  // CPP_UTILITY_DBGROUP_LOCK_UTILITY_HPP_
