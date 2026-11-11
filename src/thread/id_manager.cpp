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

// corresponding header
#include "dbgroup/thread/id_manager.hpp"

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

// local sources
#include "dbgroup/constants.hpp"

namespace dbgroup::thread
{
namespace
{
/*############################################################################*
 * Local variables
 *############################################################################*/

/// @brief A bool array for managing reservation states of thread IDs.
alignas(kVMPageSize) std::atomic_bool _id_vec[kMaxThreadCapacity] = {};  // NOLINT

static_assert(DBGROUP_MAX_THREAD_NUM <= kMaxThreadCapacity,
              "DBGROUP_MAX_THREAD_NUM must not exceed the thread capacity.");

/// @brief The maximum number of worker threads.
std::atomic_size_t _max_thread_num{static_cast<size_t>(DBGROUP_MAX_THREAD_NUM)};  // NOLINT

}  // namespace

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
IDManager::GetMaxThreadNum() noexcept  //
    -> size_t
{
  return _max_thread_num.load(kRelaxed);
}

void
IDManager::SetMaxThreadNum(  //
    const size_t thread_num)
{
  if (thread_num > kMaxThreadCapacity) {
    throw std::out_of_range{"IDManager: The number of worker threads exceeded the upperbound."};
  }
  _max_thread_num.store(thread_num, kRelaxed);
}

auto
IDManager::GetThreadID()  //
    -> size_t
{
  return GetHeartBeater().GetID();
}

auto
IDManager::GetHeartBeat()  //
    -> std::weak_ptr<size_t>
{
  return GetHeartBeater().GetHeartBeat();
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

auto
IDManager::GetHeartBeater()  //
    -> const HeartBeater&
{
  thread_local HeartBeater hb{};
  if (!hb.HasID()) [[unlikely]] {
    const auto n = GetMaxThreadNum();
    const auto retry_limit = 2 * n;
    auto id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % n;
    auto loop_num = 0U;
    do {
      if (++loop_num > retry_limit) {
        std::cerr << "[WARN] IDManager failed to assign a thread ID. To resolve "
                     "this issue, increase the thread pool capacity by calling "
                     "`IDManager::SetMaxThreadNum()` during process initialization.\n";
        throw std::range_error{"IDManager: Too many threads was assigned."};
      }
      if (++id >= n) [[unlikely]] {
        id = 0;
      }
    } while (_id_vec[id].load(kRelaxed) || _id_vec[id].exchange(true, kAcquire));
    hb.SetID(id);
  }
  return hb;
}

/*############################################################################*
 * Internal classes: HeartBeater
 *############################################################################*/

IDManager::HeartBeater::~HeartBeater()
{
  if (!id_) return;  // failed to assign a thread ID

  // stop the heart beat before releasing the ID for other threads
  const auto id = *id_;
  id_.reset();
  _id_vec[id].store(false, kRelease);
}

auto
IDManager::HeartBeater::HasID() const noexcept  //
    -> bool
{
  return id_.use_count() > 0;
}

auto
IDManager::HeartBeater::GetID() const  //
    -> size_t
{
  return *id_;
}

auto
IDManager::HeartBeater::GetHeartBeat() const noexcept  //
    -> std::weak_ptr<size_t>
{
  return std::weak_ptr<size_t>{id_};
}

void
IDManager::HeartBeater::SetID(  //
    const size_t id)
{
  id_ = std::make_shared<size_t>(id);
}

}  // namespace dbgroup::thread
