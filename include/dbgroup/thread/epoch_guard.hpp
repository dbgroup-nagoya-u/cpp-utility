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

// local sources
#include "dbgroup/thread/component/epoch.hpp"

namespace dbgroup::thread
{
/**
 * @brief A class to protect epochs based on the scoped locking pattern.
 *
 */
class EpochGuard
{
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using Epoch = component::Epoch;

 public:
  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr EpochGuard() = default;

  /**
   * @brief Construct a new instance and protect a current epoch.
   *
   * @param epoch A reference to a target epoch.
   */
  explicit EpochGuard(  //
      Epoch *epoch);

  /**
   * @brief Construct a new instance.
   *
   * @param obj An rvalue reference.
   */
  constexpr EpochGuard(  //
      EpochGuard &&obj) noexcept
      : epoch_{obj.epoch_}
  {
    obj.epoch_ = nullptr;
  }

  /**
   * @brief Construct a new instance.
   *
   * @param rhs An rvalue reference.
   */
  auto operator=(                 //
      EpochGuard &&rhs) noexcept  //
      -> EpochGuard &;

  // delete the copy constructor/assignment
  EpochGuard(const EpochGuard &) = delete;
  auto operator=(const EpochGuard &) -> EpochGuard & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the instace and release a protected epoch.
   *
   */
  ~EpochGuard();

  /*############################################################################
   * Public getters
   *##########################################################################*/

  /**
   * @return The epoch value protected by this object.
   */
  [[nodiscard]] auto GetProtectedEpoch() const  //
      -> size_t;

 private:
  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief A reference to a target epoch.
  Epoch *epoch_{nullptr};
};

}  // namespace dbgroup::thread

#endif  // CPP_UTILITY_DBGROUP_THREAD_EPOCH_GUARD_HPP_
