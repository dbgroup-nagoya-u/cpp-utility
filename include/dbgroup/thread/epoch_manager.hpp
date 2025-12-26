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

#ifndef CPP_UTILITY_DBGROUP_THREAD_EPOCH_MANAGER_HPP_
#define CPP_UTILITY_DBGROUP_THREAD_EPOCH_MANAGER_HPP_

// C++ standard libraries
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <thread>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/thread/epoch_guard.hpp"
#include "dbgroup/types.hpp"

namespace dbgroup::thread
{
/**
 * @brief A class to manage epochs for epoch-based garbage collection.
 *
 * @tparam Serial A serial type for representing epoch values.
 */
template <class Serial = Serial64_t>
class EpochManager
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a new object and start epoch management.
   *
   * @param epoch_interval The period duration for each epoch.
   * @param thread_num The maximum number of worker threads.
   */
  explicit EpochManager(  //
      size_t epoch_interval,
      size_t thread_num = kMaxThreadNum);

  /**
   * @brief Construct a new object and start epoch management.
   *
   * @param epoch_interval The interval between epoch updates.
   * @param get_new_epoch Callback function for updating the global epoch value.
   * @param thread_num The maximum number of worker threads.
   */
  EpochManager(  //
      size_t epoch_interval,
      const std::function<Serial(void)> &get_new_epoch,
      size_t thread_num = kMaxThreadNum);

  EpochManager(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = delete;

  auto operator=(const EpochManager &) -> EpochManager & = delete;
  auto operator=(EpochManager &&) -> EpochManager & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the instance.
   *
   */
  ~EpochManager();

  /*##########################################################################*
   * Public APIs
   *##########################################################################*/

  /**
   * @return The current epoch.
   */
  [[nodiscard]]
  auto GetCurrentEpoch() const noexcept  //
      -> Serial;

  /**
   * @return The current minimum (i.e., protected) epoch.
   */
  [[nodiscard]]
  auto GetMinEpoch() const noexcept  //
      -> Serial;

  /**
   * @brief Create a guard instance based on the scoped locking pattern.
   *
   * @return A created epoch guard.
   */
  [[nodiscard]]
  auto CreateEpochGuard()  //
      -> EpochGuard;

 private:
  /*##########################################################################*
   * Internal structs
   *##########################################################################*/

  /**
   * @brief A class for representing thread local epoch storages.
   *
   */
  struct alignas(kCacheLineSize) TLSEpoch {
    /// @brief A flag for indicating wether a thread has entered an epoch.
    std::atomic_bool active{};

    /// @brief The last epoch when a thread has entered.
    Serial entered{};
  };

  /*##########################################################################*
   * Internal utility functions
   *##########################################################################*/

  /**
   * @brief Advance a global epoch while running.
   *
   */
  void AdvanceEpochWorker(  //
      const std::function<Serial(void)> &get_new_epoch);

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief A global epoch counter.
  std::atomic<Serial> global_epoch_{Serial{1}};

  /// @brief The local minimum epoch.
  std::atomic<Serial> min_epoch_{Serial{0}};

  /// @brief The maximum number of worker threads.
  size_t thread_num_{kMaxThreadNum};

  /// @brief The interval between epoch updates.
  std::chrono::milliseconds epoch_interval_{};

  /// @brief The array of epochs to use as thread local storages.
  std::unique_ptr<TLSEpoch[]> tls_fields_{};

  /// @brief A flag for detaching a epoch manager thread.
  std::atomic_bool running_{};

  /// @brief A thread for managing epoch values.
  std::thread manager_{};
};

}  // namespace dbgroup::thread

#endif  // CPP_UTILITY_DBGROUP_THREAD_EPOCH_MANAGER_HPP_
