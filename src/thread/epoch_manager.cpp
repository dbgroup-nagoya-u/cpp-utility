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

// the corresponding header
#include "dbgroup/thread/epoch_manager.hpp"

// C++ standard libraries
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <functional>
#include <limits>
#include <utility>
#include <vector>

// local sources
#include "dbgroup/thread/id_manager.hpp"

namespace dbgroup::thread
{
/*##############################################################################
 * Public constructors and destructors
 *############################################################################*/

/**
 * @brief Construct a new instance.
 *
 */
EpochManager::EpochManager()
{
  auto &protected_epochs = ProtectedNode::GetProtectedEpochs(kInitialEpoch, protected_lists_);
  protected_epochs.emplace_back(kInitialEpoch);
}

EpochManager::~EpochManager()
{
  // remove the retained protected epochs
  [[maybe_unused]] const auto dummy = global_epoch_.load(std::memory_order_acquire);
  auto *pro_next = protected_lists_;
  while (pro_next != nullptr) {
    auto *current = pro_next;
    pro_next = current->next;
    delete current;
  }
}

/*##############################################################################
 * Public APIs
 *############################################################################*/

auto
EpochManager::GetCurrentEpoch() const  //
    -> size_t
{
  return global_epoch_.load(std::memory_order_relaxed);
}

auto
EpochManager::GetMinEpoch() const  //
    -> size_t
{
  return min_epoch_.load(std::memory_order_relaxed);
}

auto
EpochManager::GetProtectedEpochs()  //
    -> std::pair<EpochGuard, const std::vector<size_t> &>
{
  auto &&guard = CreateEpochGuard();
  const auto e = guard.GetProtectedEpoch();
  const auto &protected_epochs = ProtectedNode::GetProtectedEpochs(e, protected_lists_);

  return {std::move(guard), protected_epochs};
}

auto
EpochManager::CreateEpochGuard()  //
    -> EpochGuard
{
  auto &tls = tls_fields_[IDManager::GetThreadID()];
  if (tls.heartbeat.expired()) {
    tls.epoch.SetGlobalEpoch(&global_epoch_);
    tls.heartbeat = IDManager::GetHeartBeat();
  }

  return EpochGuard{&(tls.epoch)};
}

void
EpochManager::ForwardGlobalEpoch()
{
  const auto cur_epoch = global_epoch_.load(std::memory_order_relaxed);
  const auto next_epoch = cur_epoch + 1;

  // create a new node if needed
  if ((next_epoch & kLowerMask) == 0UL) {
    protected_lists_ = new ProtectedNode{next_epoch, protected_lists_};
  }

  // update protected epoch values
  auto &protected_epochs = ProtectedNode::GetProtectedEpochs(next_epoch, protected_lists_);
  CollectProtectedEpochs(cur_epoch, protected_epochs);
  RemoveOutDatedLists(protected_epochs);

  // store the max/min epoch values for efficiency
  global_epoch_.store(next_epoch, std::memory_order_release);
  min_epoch_.store(protected_epochs.back(), std::memory_order_relaxed);
}

/*##############################################################################
 * Internal APIs
 *############################################################################*/

void
EpochManager::CollectProtectedEpochs(  //
    const size_t cur_epoch,
    std::vector<size_t> &protected_epochs)
{
  protected_epochs.reserve(kMaxThreadNum);
  protected_epochs.emplace_back(cur_epoch + 1);  // reserve the next epoch
  protected_epochs.emplace_back(cur_epoch);

  for (size_t i = 0; i < kMaxThreadNum; ++i) {
    auto &tls = tls_fields_[i];
    if (tls.heartbeat.expired()) continue;

    const auto protected_epoch = tls.epoch.GetProtectedEpoch();
    if (protected_epoch < std::numeric_limits<size_t>::max()) {
      protected_epochs.emplace_back(protected_epoch);
    }
  }

  // remove duplicate values
  std::sort(protected_epochs.begin(), protected_epochs.end(), std::greater<size_t>{});
  auto &&end_iter = std::unique(protected_epochs.begin(), protected_epochs.end());
  protected_epochs.erase(end_iter, protected_epochs.end());
}

void
EpochManager::RemoveOutDatedLists(  //
    std::vector<size_t> &protected_epochs)
{
  const auto &it_end = protected_epochs.cend();
  auto &&it = protected_epochs.cbegin();
  auto protected_epoch = *it & kUpperMask;

  // remove out-dated lists
  auto *prev = protected_lists_;
  auto *current = protected_lists_;
  while (current->next != nullptr) {
    const auto upper_bits = current->GetUpperBits();
    if (protected_epoch == upper_bits) {
      // this node is still referred, so skip
      prev = current;
      current = current->next;

      // search the next protected epoch
      do {
        if (++it == it_end) {
          protected_epoch = kMinEpoch;
          break;
        }
        protected_epoch = *it & kUpperMask;
      } while (protected_epoch == upper_bits);
      continue;
    }

    if (prev != current) {
      // remove the out-dated list
      prev->next = current->next;
      delete current;
      current = prev->next;
    }
  }
}

/*##############################################################################
 * Internal class: ProtectedNode
 *############################################################################*/

EpochManager::ProtectedNode::ProtectedNode(  //
    const size_t epoch,
    ProtectedNode *next)
    : next{next}, upper_epoch_(epoch)
{
}

}  // namespace dbgroup::thread
