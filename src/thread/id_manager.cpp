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
#include "thread/id_manager.hpp"

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

// local sources
#include "thread/common.hpp"

namespace dbgroup::thread
{
namespace
{
/*##############################################################################
 * Local utilities
 *############################################################################*/

/**
 * @return An initialized atomic_bool array.
 */
auto
Initialize()  //
    -> std::unique_ptr<std::atomic_bool[]>
{
  auto &&vec = std::make_unique<std::atomic_bool[]>(kMaxThreadNum);
  for (size_t i = 0; i < kMaxThreadNum; ++i) {
    vec[i].store(false, kRelaxed);
  }
  return std::move(vec);
}

/*##############################################################################
 * Local variables
 *############################################################################*/

/// @brief A bool array for managing reservation states of thread IDs.
std::unique_ptr<std::atomic_bool[]> id_vec = Initialize();

}  // namespace

/*##############################################################################
 * Public APIs
 *############################################################################*/

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

/*##############################################################################
 * Internal APIs
 *############################################################################*/

auto
IDManager::GetHeartBeater()  //
    -> const HeartBeater &
{
  thread_local HeartBeater hb{};
  if (!hb.HasID()) {
    auto id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % kMaxThreadNum;
    while (true) {
      auto &dst = id_vec[id];
      auto reserved = dst.load(kRelaxed);
      if (!reserved && dst.compare_exchange_strong(reserved, true, kRelaxed)) {
        hb.SetID(id);
        break;
      }
      if (++id >= kMaxThreadNum) {
        id = 0;
      }
    }
  }
  return hb;
}

/*##############################################################################
 * Internal classes: HeartBeater
 *############################################################################*/

IDManager::HeartBeater::~HeartBeater()
{  //
  id_vec[*id_].store(false, kRelaxed);
}

auto
IDManager::HeartBeater::HasID() const  //
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
IDManager::HeartBeater::GetHeartBeat() const  //
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
