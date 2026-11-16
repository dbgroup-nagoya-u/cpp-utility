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

#ifndef CPP_UTILITY_DBGROUP_THREAD_EPOCH_GUARD_HPP_
#define CPP_UTILITY_DBGROUP_THREAD_EPOCH_GUARD_HPP_

// C++ standard libraries
#include <atomic>
#include <utility>

// local sources
#include "dbgroup/constants.hpp"

namespace dbgroup::thread
{
/**
 * @brief A class to protect epochs based on the scoped locking pattern.
 *
 */
class EpochGuard
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr EpochGuard() noexcept = default;

  /**
   * @brief Construct a new instance and protect a current epoch.
   *
   * @param active An actual address for managing a guard status.
   */
  explicit EpochGuard(  //
      std::atomic_bool* const active) noexcept
      : active_{active}
  {
    active_->store(true, kRelease);
    std::atomic_thread_fence(kSeqCst);  // prevent protected loads from preceding
  }

  constexpr EpochGuard(  //
      EpochGuard&& obj) noexcept
      : active_{std::exchange(obj.active_, nullptr)}
  {
  }

  auto
  operator=(                      //
      EpochGuard&& rhs) noexcept  //
      -> EpochGuard&
  {
    if (active_) {
      active_->store(false, kRelease);
    }
    active_ = std::exchange(rhs.active_, nullptr);
    return *this;
  }

  // delete the copy constructor/assignment
  EpochGuard(const EpochGuard&) = delete;
  auto operator=(const EpochGuard&) -> EpochGuard& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the instance and release a protected epoch.
   *
   */
  ~EpochGuard()
  {
    if (active_) {
      active_->store(false, kRelease);
    }
  }

 private:
  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief A reference to a target epoch.
  std::atomic_bool* active_{};
};

}  // namespace dbgroup::thread

#endif  // CPP_UTILITY_DBGROUP_THREAD_EPOCH_GUARD_HPP_
