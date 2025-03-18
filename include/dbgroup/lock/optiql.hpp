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
  class XGuard
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr XGuard() = default;

    /**
     * @param dest The address of a target lock.
     * @param ver The current version.
     */
    constexpr XGuard(  //
        OptiQL *dest,
        const uint64_t qid,
        const uint32_t ver)
        : dest_{dest}, qid_{qid}, old_ver_{ver}, new_ver_{ver + 1U}
    {
    }

    XGuard(const XGuard &) = delete;

    constexpr XGuard(  //
        XGuard &&obj) noexcept
        : dest_{obj.dest_}, qid_{obj.qid_}, old_ver_{obj.old_ver_}, new_ver_{obj.new_ver_}
    {
      obj.dest_ = nullptr;
    }

    auto operator=(const XGuard &) -> XGuard & = delete;

    auto operator=(             //
        XGuard &&rhs) noexcept  //
        -> XGuard &;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    /**
     * @brief Destroy this instance and release a lock if holding.
     *
     */
    ~XGuard();

    /*##########################################################################
     * Public APIs
     *########################################################################*/

    /**
     * @retval true if this instance has the lock ownership.
     * @retval false otherwise.
     */
    constexpr explicit
    operator bool() const
    {
      return dest_;
    }

    /**
     * @return The version when this guard was created.
     */
    [[nodiscard]] constexpr auto
    GetVersion() const  //
        -> uint32_t
    {
      return old_ver_;
    }

    /**
     * @brief Set a desired version after unlocking.
     *
     * @param ver A desired version after unlocking.
     */
    constexpr void
    SetVersion(  //
        const uint32_t ver)
    {
      new_ver_ = ver;
    }

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    OptiQL *dest_{};

    /// @brief The corresponding queue node for unlocking.
    uint64_t qid_{};

    /// @brief A version when creating this guard.
    uint32_t old_ver_{};

    /// @brief A version when failing verification.
    uint32_t new_ver_{};
  };

  /**
   * @brief A class for representing a guard instance for optimistic locking.
   *
   */
  class OptGuard
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr OptGuard() = default;

    /**
     * @param dest The address of a target lock.
     * @param ver The current version.
     */
    constexpr OptGuard(  //
        const OptiQL *dest,
        const uint32_t ver)
        : dest_{dest}, ver_{ver}
    {
    }

    constexpr OptGuard(const OptGuard &) = default;
    constexpr OptGuard(OptGuard &&) noexcept = default;

    constexpr auto operator=(const OptGuard &) noexcept -> OptGuard & = default;
    constexpr auto operator=(OptGuard &&) noexcept -> OptGuard & = default;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    ~OptGuard() = default;

    /*##########################################################################
     * Public getters
     *########################################################################*/

    /**
     * @return false.
     */
    constexpr explicit
    operator bool() const
    {
      return false;
    }

    /**
     * @return The version when this guard was created.
     */
    [[nodiscard]] constexpr auto
    GetVersion() const  //
        -> uint32_t
    {
      return ver_;
    }

    /*##########################################################################
     * Public APIs
     *########################################################################*/

    /**
     * @retval true if a target version does not change from an expected one.
     * @retval false otherwise.
     */
    [[nodiscard]] auto VerifyVersion()  //
        -> bool;

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The address of a target lock.
    const OptiQL *dest_{};

    /// @brief A version when creating this guard.
    uint32_t ver_{};
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
   * @return An empty guard instance with the current version value.
   *
   * @note This function does not give up reading a version value and continues
   * with spinlock and back-off.
   */
  [[nodiscard]] auto GetVersion() const  //
      -> OptGuard;

  /**
   * @brief Get an exclusive lock.
   *
   * @return A guard instance for the acquired lock.
   * @note This function does not give up acquiring a lock and continues with
   * spinlock and back-off.
   */
  [[nodiscard]] auto LockX()  //
      -> XGuard;

 private:
  /*############################################################################
   * Internal APIs
   *##########################################################################*/

  /**
   * @brief Release an exclusive lock.
   *
   * @param qnode The queue node corresponding to this lock.
   * @param ver A desired version after unlocking.
   * @note If a thread calls this function without acquiring an X lock, it will
   * corrupt an internal lock state.
   */
  void UnlockX(  //
      uint64_t qid,
      uint64_t ver);

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief The current lock state.
  std::atomic_uint64_t lock_{0};
};

}  // namespace dbgroup::lock

#endif  // CPP_UTILITY_DBGROUP_LOCK_OPTIQL_HPP_
