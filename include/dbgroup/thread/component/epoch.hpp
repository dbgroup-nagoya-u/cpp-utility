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

#ifndef CPP_UTILITY_DBGROUP_THREAD_COMPONENT_EPOCH_HPP_
#define CPP_UTILITY_DBGROUP_THREAD_COMPONENT_EPOCH_HPP_

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <limits>

namespace dbgroup::thread::component
{
/**
 * @brief A class to represent epochs for epoch-based garbage collection.
 *
 */
class Epoch
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr Epoch() noexcept = default;

  Epoch(const Epoch &) = delete;
  Epoch(Epoch &&orig) = delete;

  auto operator=(const Epoch &) -> Epoch & = delete;
  auto operator=(Epoch &&orig) -> Epoch & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the Epoch object.
   *
   */
  ~Epoch() = default;

  /*##########################################################################*
   * Public getters/setters
   *##########################################################################*/

  /**
   * @return size_t A current epoch value.
   */
  [[nodiscard]] auto GetCurrentEpoch() const noexcept  //
      -> size_t;

  /**
   * @return size_t A protected epoch value.
   */
  [[nodiscard]] auto GetProtectedEpoch() const noexcept  //
      -> size_t;

  /**
   * @param global_epoch A pointer to the global epoch.
   */
  constexpr void
  SetGlobalEpoch(  //
      std::atomic_size_t *global_epoch) noexcept
  {
    current_ = global_epoch;
  }

  /*##########################################################################*
   * Public utility functions
   *##########################################################################*/

  /**
   * @brief Keep a current epoch value to protect new garbages.
   *
   */
  void EnterEpoch() noexcept;

  /**
   * @brief Release a protected epoch value to allow GC to delete old garbages.
   *
   */
  void LeaveEpoch() noexcept;

 private:
  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief A current epoch.
  std::atomic_size_t *current_{nullptr};

  /// @brief A snapshot to denote a protected epoch.
  std::atomic_size_t entered_{std::numeric_limits<size_t>::max()};
};

}  // namespace dbgroup::thread::component

#endif  // CPP_UTILITY_DBGROUP_THREAD_COMPONENT_EPOCH_HPP_
