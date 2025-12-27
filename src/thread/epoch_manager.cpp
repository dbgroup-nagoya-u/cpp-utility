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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/thread/id_manager.hpp"
#include "dbgroup/types.hpp"

namespace dbgroup::thread
{
/*############################################################################*
 * Local type aliases
 *############################################################################*/

using IDManager = dbgroup::thread::IDManager;

/*############################################################################*
 * Public constructors and destructors
 *############################################################################*/

template <class Serial>
EpochManager<Serial>::EpochManager(  //
    const size_t epoch_interval)
    : global_epoch_{Serial{1}},
      min_epoch_{Serial{0}},
      thread_num_{IDManager::GetMaxThreadNum()},
      epoch_interval_{epoch_interval},
      tls_fields_{std::make_unique<TLSEpoch[]>(thread_num_)},
      running_{true},
      manager_{&EpochManager::AdvanceEpochWorker, this,
               [this]() -> Serial { return ++GetCurrentEpoch(); }}
{
}

template <class Serial>
EpochManager<Serial>::EpochManager(  //
    const size_t epoch_interval,
    const std::function<Serial(void)> &get_new_epoch)
    : global_epoch_{get_new_epoch()},
      min_epoch_{--GetCurrentEpoch()},
      thread_num_{IDManager::GetMaxThreadNum()},
      epoch_interval_{epoch_interval},
      tls_fields_{std::make_unique<TLSEpoch[]>(thread_num_)},
      running_{true},
      manager_{&EpochManager::AdvanceEpochWorker, this, get_new_epoch}
{
}

template <class Serial>
EpochManager<Serial>::~EpochManager()
{
  if (running_.load(kRelaxed)) {
    running_.store(false, kRelaxed);
    try {
      manager_.join();
    } catch (const std::exception &e) {
      std::cerr << "The epoch manager could not be joined: " << e.what() << "\n";
    }
  }
}

/*############################################################################*
 * Public APIs
 *############################################################################*/

template <class Serial>
auto
EpochManager<Serial>::GetCurrentEpoch() const noexcept  //
    -> Serial
{
  return global_epoch_.load(kRelaxed);
}

template <class Serial>
auto
EpochManager<Serial>::GetMinEpoch() const noexcept  //
    -> Serial
{
  return min_epoch_.load(kRelaxed);
}

template <class Serial>
auto
EpochManager<Serial>::CreateEpochGuard()  //
    -> EpochGuard
{
  auto &tls = tls_fields_[dbgroup::thread::IDManager::GetThreadID()];
  tls.entered = --GetCurrentEpoch();  // add a safety buffer
  return EpochGuard{&(tls.active)};
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

template <class Serial>
void
EpochManager<Serial>::AdvanceEpochWorker(  //
    const std::function<Serial(void)> &get_new_epoch)
{
  auto wake_time = std::chrono::system_clock::now();
  while (running_.load(kRelaxed)) {
    wake_time += epoch_interval_;
    auto min = --GetCurrentEpoch();  // add a safety buffer
    for (size_t thd_id = 0; thd_id < thread_num_; ++thd_id) {
      auto &tls = tls_fields_[thd_id];
      if (!tls.active.load(kAcquire)) continue;
      min = std::min(min, tls.entered);
    }
    global_epoch_.store(get_new_epoch(), kRelaxed);
    if (GetMinEpoch() != min) {
      min_epoch_.store(min, kRelaxed);
    }
    std::this_thread::sleep_until(wake_time);
  }
}

/*############################################################################*
 * Explicit instantiation definitions
 *############################################################################*/

template class EpochManager<Serial8_t>;
template class EpochManager<Serial16_t>;
template class EpochManager<Serial32_t>;
template class EpochManager<Serial64_t>;

}  // namespace dbgroup::thread
