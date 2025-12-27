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
#include <memory>
#include <thread>
#include <vector>

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

/// @brief The maximum number of worker threads.
std::atomic_size_t _max_thread_num{2 * kLogicalCoreNum};  // NOLINT

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
    -> const HeartBeater &
{
  thread_local HeartBeater hb{};
  if (!hb.HasID()) [[unlikely]] {
    const auto n = GetMaxThreadNum();
    auto id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % n;
    do {
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
{  //
  _id_vec[*id_].store(false, kRelease);
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
