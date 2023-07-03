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

#ifndef CPP_UTILITY_THREAD_ID_MANAGER_HPP
#define CPP_UTILITY_THREAD_ID_MANAGER_HPP

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <type_traits>
#include <vector>

namespace dbgroup::thread
{

/// The maximum number of threads used in a process.
constexpr size_t kMaxThreadNum = DBGROUP_MAX_THREAD_NUM;

/**
 * @brief A singleton class for managing IDs for each thread.
 *
 */
class IDManager
{
 public:
  /*####################################################################################
   * No constructors and destructor
   *##################################################################################*/

  IDManager() = delete;
  ~IDManager() = delete;

  IDManager(const IDManager &) = delete;
  IDManager(IDManager &&) noexcept = delete;

  auto operator=(const IDManager &) -> IDManager & = delete;
  auto operator=(IDManager &&) noexcept -> IDManager & = delete;

  /*####################################################################################
   * Public static utilities
   *##################################################################################*/

  /**
   * @return the unique thread ID in [0, DBGROUP_MAX_THREAD_NUM).
   */
  static auto
  GetThreadID()  //
      -> size_t
  {
    return GetHeartBeater()->GetID();
  }

  /**
   * @return a shared pointer object to check heart beats of the current thread.
   */
  static auto
  GetHeartBeat()  //
      -> std::shared_ptr<std::atomic_bool>
  {
    return GetHeartBeater()->GetHeartBeat();
  }

 private:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  /**
   * @brief A class for managing heart beats of threads.
   *
   * This class is used with thread local storages and stops heart beats of the
   * corresponding thread when it has exited.
   */
  class HeartBeater
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new object.
     *
     * @param id the assigned thread ID.
     */
    explicit HeartBeater(const size_t id) : id_{id}
    {
      heart_beat_ = std::make_shared<std::atomic_bool>(true);
    }

    HeartBeater(const HeartBeater &) = delete;
    HeartBeater(HeartBeater &&) noexcept = delete;

    auto operator=(const HeartBeater &obj) -> HeartBeater & = delete;
    auto operator=(HeartBeater &&) noexcept -> HeartBeater & = delete;

    /*##################################################################################
     * Public destructor
     *################################################################################*/

    /**
     * @brief Destroy the object.
     *
     * This destructor removes the heart beat and thread reservation flags.
     */
    ~HeartBeater()
    {
      heart_beat_->store(false, std::memory_order_relaxed);
      id_vec_[id_].store(false, std::memory_order_relaxed);
    }

    /*##################################################################################
     * Public getters
     *################################################################################*/

    /**
     * @return the assigned ID for this object.
     */
    [[nodiscard]] constexpr auto
    GetID() const  //
        -> size_t
    {
      return id_;
    }

    /**
     * @return a shared pointer object to check heart beats of this object.
     */
    [[nodiscard]] auto
    GetHeartBeat() const  //
        -> std::shared_ptr<std::atomic_bool>
    {
      return std::shared_ptr<std::atomic_bool>{heart_beat_};
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// The assigned ID for this object.
    size_t id_{};

    /// Instance to indicate that the corresponding thread is alive.
    std::shared_ptr<std::atomic_bool> heart_beat_{};
  };

  /*####################################################################################
   * Internal static utilities
   *##################################################################################*/

  /**
   * @return an initialized atomic_bool array.
   */
  static auto
  Initialize()  //
      -> std::unique_ptr<std::atomic_bool[]>
  {
    auto &&vec = std::make_unique<std::atomic_bool[]>(kMaxThreadNum);
    for (size_t i = 0; i < kMaxThreadNum; ++i) {
      vec[i].store(false, std::memory_order_relaxed);
    }

    return std::move(vec);
  }

  /**
   * @brief Get the reference to a heart beater.
   *
   * When a thread calls this function for the first time, it prepares its unique ID and
   * heart beat manager.
   *
   * @return const std::unique_ptr<HeartBeater>&
   */
  static auto
  GetHeartBeater()  //
      -> const std::unique_ptr<HeartBeater> &
  {
    thread_local std::unique_ptr<HeartBeater> hb{};
    if (!hb) {
      auto id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % kMaxThreadNum;
      while (true) {
        auto &dst = id_vec_[id];
        auto reserved = dst.load(std::memory_order_relaxed);
        if (!reserved && dst.compare_exchange_strong(reserved, true, std::memory_order_relaxed)) {
          hb = std::make_unique<HeartBeater>(id);
          break;
        }

        if (++id >= kMaxThreadNum) {
          id = 0;
        }
      }
    }

    return hb;
  }

  /*####################################################################################
   * Internal class variables
   *##################################################################################*/

  /// A bool array for managing reservation states of thread IDs.
  static inline std::unique_ptr<std::atomic_bool[]> id_vec_ = Initialize();  // NOLINT
};

}  // namespace dbgroup::thread

#endif  // CPP_UTILITY_THREAD_ID_MANAGER_HPP
