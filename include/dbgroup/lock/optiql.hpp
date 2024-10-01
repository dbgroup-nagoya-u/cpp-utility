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

#ifndef CPP_UTILITY_DBGROUP_LOCK_OPTIQL_HPP_
#define CPP_UTILITY_DBGROUP_LOCK_OPTIQL_HPP_

// C++ standard libraries
#include <atomic>
#include <cstdint>

namespace dbgroup::lock
{
/**
 * @brief A class for representing the MCS queue lock.
 *
 */
class OptiQL
{
 public:
  /*############################################################################
   * Public constants
   *##########################################################################*/

  /// @brief The maximum number of queue nodes.
  static constexpr uint64_t kQNodeNum = 1UL << 16UL;

  /*############################################################################
   * Public inner classes
   *##########################################################################*/

  /**
   * @brief A class for representing a guard instance for exclusive locks.
   *
   */
  class OptiQLGuard
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr OptiQLGuard() = default;

    /**
     * @param lock The address of a target lock.
     * @param qid The corresponding queue node for unlocking.
     */
    constexpr OptiQLGuard(  //
        OptiQL *lock,
        uint64_t qid)
        : lock_{lock}, qid_{qid}
    {
    }

    OptiQLGuard(const OptiQLGuard &) = delete;

    constexpr OptiQLGuard(  //
        OptiQLGuard &&obj) noexcept
        : lock_{obj.lock_}, qid_{obj.qid_}
    {
      obj.qid_ = kQNodeNum;
    }

    auto operator=(const OptiQLGuard &) -> OptiQLGuard & = delete;

    auto operator=(                  //
        OptiQLGuard &&obj) noexcept  //
        -> OptiQLGuard &;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    ~OptiQLGuard();

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OptiQL *lock_{nullptr};

    /// @brief The corresponding queue node for unlocking.
    uint64_t qid_{};
  };

  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr OptiQL() = default;

  OptiQL(const OptiQL &) = delete;
  OptiQL(OptiQL &&) = delete;

  auto operator=(const OptiQL &) -> OptiQL & = delete;
  auto operator=(OptiQL &&) -> OptiQL & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~OptiQL() = default;

  /*############################################################################
   * Public APIs
   *##########################################################################*/

  /**
   * @return The current version value.
   *
   * @note This function does not give up reading a version value and continues
   * with spinlock and back-off.
   */
  [[nodiscard]] auto GetVersion() const  //
      -> uint64_t;

  /**
   * @param expected An expected version value.
   * @retval true if the given version value is the same as the current one.
   * @retval false otherwise.
   */
  [[nodiscard]] auto HasSameVersion(  //
      const uint64_t expected) const  //
      -> bool;

  /**
   * @brief Get an exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto LockX()  //
      -> OptiQLGuard;

 private:
  /*############################################################################
   * Internal APIs
   *##########################################################################*/

  /**
   * @brief Release an exclusive lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      uint64_t qid);

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{0};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_OPTIQL_HPP_
