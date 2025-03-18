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
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

// local sources
#include "dbgroup/thread/epoch_guard.hpp"
#include "dbgroup/thread/id_manager.hpp"

namespace dbgroup::thread
{
/**
 * @brief A class to manage epochs for epoch-based garbage collection.
 *
 */
class EpochManager
{
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using Epoch = component::Epoch;

 public:
  /*############################################################################
   * Public constants
   *##########################################################################*/

  /// @brief The capacity of nodes for protected epochs.
  static constexpr size_t kCapacity = 256;

  /// @brief The initial value of epochs.
  static constexpr size_t kInitialEpoch = kCapacity;

  /// @brief The minimum value of epochs.
  static constexpr size_t kMinEpoch = 0;

  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  EpochManager();

  EpochManager(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = delete;

  auto operator=(const EpochManager &) -> EpochManager & = delete;
  auto operator=(EpochManager &&) -> EpochManager & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the instance.
   *
   */
  ~EpochManager();

  /*############################################################################
   * Public APIs
   *##########################################################################*/

  /**
   * @return A current global epoch value.
   */
  [[nodiscard]] auto GetCurrentEpoch() const  //
      -> size_t;

  /**
   * @return The minimum protected epoch value.
   */
  [[nodiscard]] auto GetMinEpoch() const  //
      -> size_t;

  /**
   * @brief Get protected epoch values as shared_ptr.
   *
   * Protected epoch values are sorted by descending order and include the
   * current epoch value. Note that the returned vector cannot be modified
   * because it is referred from multiple threads concurrently.
   *
   * @retval 1st: The corresponding epoch guard for this thread.
   * @retval 2nd: Protected epoch values.
   */
  [[nodiscard]] auto GetProtectedEpochs()  //
      -> std::pair<EpochGuard, const std::vector<size_t> &>;

  /**
   * @brief Create a guard instance based on the scoped locking pattern.
   *
   * @return A created epoch guard.
   */
  [[nodiscard]] auto CreateEpochGuard()  //
      -> EpochGuard;

  /**
   * @brief Increment a current epoch value.
   *
   * This function also updates protected epoch values.
   */
  void ForwardGlobalEpoch();

 private:
  /*############################################################################
   * Internal structs
   *##########################################################################*/

  /**
   * @brief A class for representing thread local epoch storages.
   *
   */
  struct alignas(kCashLineSize) TLSEpoch {
    /// @brief An epoch object for each thread.
    Epoch epoch{};

    /// @brief A flag for indicating the corresponding thread has exited.
    std::weak_ptr<size_t> heartbeat{};
  };

  /**
   * @brief A class for composing a linked list of epochs in each thread.
   *
   */
  class alignas(kCashLineSize) ProtectedNode
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    /**
     * @brief Construct a new instance.
     *
     * @param epoch Upper bits of epoch values to be retained in this node.
     * @param next A pointer to the next node.
     */
    ProtectedNode(  //
        size_t epoch,
        ProtectedNode *next);

    ProtectedNode(const ProtectedNode &) = delete;
    ProtectedNode(ProtectedNode &&) = delete;

    auto operator=(const ProtectedNode &) -> ProtectedNode & = delete;
    auto operator=(ProtectedNode &&) -> ProtectedNode & = delete;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    /**
     * @brief Destroy the instance.
     *
     */
    ~ProtectedNode() = default;

    /*##########################################################################
     * Public utility functions
     *########################################################################*/

    /**
     * @brief Get protected epoch values based on a given epoch.
     *
     * @param epoch A target epoch value.
     * @param node The head pointer of a linked list.
     * @return Protected epochs.
     */
    [[nodiscard]] static constexpr auto
    GetProtectedEpochs(  //
        const size_t epoch,
        ProtectedNode *node)  //
        -> std::vector<size_t> &
    {
      // go to the target node
      const auto upper_epoch = epoch & kUpperMask;
      while (node->upper_epoch_ > upper_epoch) {
        node = node->next;
      }

      return node->epoch_lists_.at(epoch & kLowerMask);
    }

    /**
     * @return The upper bits of the current epoch.
     */
    [[nodiscard]] constexpr auto
    GetUpperBits() const  //
        -> size_t
    {
      return upper_epoch_;
    }

    /*##########################################################################
     * Public member variables
     *########################################################################*/

    /// @brief A pointer to the next node.
    ProtectedNode *next{nullptr};  // NOLINT

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// @brief The upper bits of epoch values to be retained in this node.
    size_t upper_epoch_{};

    /// @brief The list of protected epochs.
    std::array<std::vector<size_t>, kCapacity> epoch_lists_ = {};
  };

  /*############################################################################
   * Internal constants
   *##########################################################################*/

  /// @brief A bitmask for extracting lower bits from epochs.
  static constexpr size_t kLowerMask = kCapacity - 1UL;

  /// @brief A bitmask for extracting upper bits from epochs.
  static constexpr size_t kUpperMask = ~kLowerMask;

  /*############################################################################
   * Internal utility functions
   *##########################################################################*/

  /**
   * @brief Collect epoch value for epoch-based protection.
   *
   * This function also removes dead epochs from the internal list while
   * computing.
   *
   * @param cur_epoch The current global epoch value.
   * @param protected_epochs Protected epoch values.
   */
  void CollectProtectedEpochs(  //
      size_t cur_epoch,
      std::vector<size_t> &protected_epochs);

  /**
   * @brief Remove unprotected epoch nodes from a linked-list.
   *
   * @param protected_epochs Protected epoch values.
   */
  void RemoveOutDatedLists(  //
      std::vector<size_t> &protected_epochs);

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief A global epoch counter.
  std::atomic_size_t global_epoch_{kInitialEpoch};

  /// @brief The minimum protected epoch value.
  std::atomic_size_t min_epoch_{kInitialEpoch};

  /// @brief The head pointer of a linked list of epochs.
  ProtectedNode *protected_lists_{new ProtectedNode{kInitialEpoch, nullptr}};

  /// @brief The array of epochs to use as thread local storages.
  TLSEpoch tls_fields_[kMaxThreadNum]{};
};

}  // namespace dbgroup::thread

#endif  // CPP_UTILITY_DBGROUP_THREAD_EPOCH_MANAGER_HPP_
