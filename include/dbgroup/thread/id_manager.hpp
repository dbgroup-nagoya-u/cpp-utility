/*
 * Copyright 2023 Database Group, Nagoya University
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

#ifndef CPP_UTILITY_DBGROUP_THREAD_ID_MANAGER_HPP_
#define CPP_UTILITY_DBGROUP_THREAD_ID_MANAGER_HPP_

// C++ standard libraries
#include <cstddef>
#include <memory>

// local sources
#include "dbgroup/constants.hpp"

namespace dbgroup::thread
{

/**
 * @brief A singleton class for managing IDs for each thread.
 *
 */
class IDManager
{
 public:
  /*############################################################################
   * No constructors and destructor
   *##########################################################################*/

  IDManager() = delete;
  ~IDManager() = delete;

  IDManager(const IDManager &) = delete;
  IDManager(IDManager &&) noexcept = delete;

  auto operator=(const IDManager &) -> IDManager & = delete;
  auto operator=(IDManager &&) noexcept -> IDManager & = delete;

  /*############################################################################
   * Public static utilities
   *##########################################################################*/

  /**
   * @return The unique thread ID in [0, DBGROUP_MAX_THREAD_NUM).
   */
  [[nodiscard]] static auto GetThreadID()  //
      -> size_t;

  /**
   * @return A weak pointer object to check heart beats of the current thread.
   */
  [[nodiscard]] static auto GetHeartBeat()  //
      -> std::weak_ptr<size_t>;

 private:
  /*############################################################################
   * Internal classes
   *##########################################################################*/

  /**
   * @brief A class for managing heart beats of threads.
   *
   * This class is used with thread local storages and stops heart beats of the
   * corresponding thread when it has exited.
   */
  class HeartBeater
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr HeartBeater() = default;

    HeartBeater(const HeartBeater &) = delete;
    HeartBeater(HeartBeater &&) noexcept = delete;

    auto operator=(const HeartBeater &obj) -> HeartBeater & = delete;
    auto operator=(HeartBeater &&) noexcept -> HeartBeater & = delete;

    /*##########################################################################
     * Public destructor
     *########################################################################*/

    /**
     * @brief Destroy the object.
     *
     * This destructor removes the heart beat and thread reservation flags.
     */
    ~HeartBeater();

    /*##########################################################################
     * Public getters
     *########################################################################*/

    /**
     * @retval true if this object has a unique thread ID.
     * @retval false otherwise.
     */
    [[nodiscard]] auto HasID() const  //
        -> bool;

    /**
     * @return The assigned ID for this object.
     */
    [[nodiscard]] auto GetID() const  //
        -> size_t;

    /**
     * @return A weak pointer object to check heart beats of this object.
     */
    [[nodiscard]] auto GetHeartBeat() const  //
        -> std::weak_ptr<size_t>;

    /*##########################################################################
     * Public setters
     *########################################################################*/

    /**
     * @brief Set a unique thread ID to this object.
     *
     * @param id The thread ID to be assigned.
     */
    void SetID(  //
        size_t id);

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The assigned ID for this object.
    std::shared_ptr<size_t> id_{};
  };

  /*############################################################################
   * Internal static utilities
   *##########################################################################*/

  /**
   * @brief Get the reference to a heart beater.
   *
   * When a thread calls this function for the first time, it prepares its
   * unique ID and heart beat manager.
   *
   * @return The reference to a heart beater.
   */
  [[nodiscard]] static auto GetHeartBeater()  //
      -> const HeartBeater &;
};

}  // namespace dbgroup::thread

#endif  // CPP_UTILITY_DBGROUP_THREAD_ID_MANAGER_HPP_
