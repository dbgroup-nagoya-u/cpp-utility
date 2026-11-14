/*
 * Copyright 2026 Database Group, Nagoya University
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

#ifndef CPP_UTILITY_DBGROUP_INDEX_FIX_LEN_WRAPPER_HPP_
#define CPP_UTILITY_DBGROUP_INDEX_FIX_LEN_WRAPPER_HPP_

// C++ standard libraries
#include <concepts>
#include <cstddef>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

// local sources
#include "dbgroup/index/concepts.hpp"
#include "dbgroup/index/utility.hpp"

namespace dbgroup::index
{
/**
 * @brief A wrapper class providing APIs that return `std::optional` for
 * indexes with fixed-length payloads.
 *
 * @tparam Index An index class providing the standard APIs.
 */
template <StandardIndex Index>
class FixLenWrapper
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Key = typename Index::KeyType;
  using Payload = typename Index::PayloadType;
  using PayloadBuf = typename Index::PayloadBuf;
  using ScanKey = std::optional<std::tuple<Key, size_t, bool>>;

 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a new index.
   *
   * @param args Arguments for constructing an actual index.
   */
  template <class... Args>
    requires std::constructible_from<Index, Args...>
  explicit FixLenWrapper(  //
      Args&&... args)
      : index_(std::forward<Args>(args)...)  // avoid narrowing checks for forwarding
  {
  }

  FixLenWrapper(const FixLenWrapper&) = delete;
  FixLenWrapper(FixLenWrapper&&) = delete;

  auto operator=(const FixLenWrapper&) -> FixLenWrapper& = delete;
  auto operator=(FixLenWrapper&&) -> FixLenWrapper& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the object.
   *
   */
  ~FixLenWrapper() = default;

  /*##########################################################################*
   * Public read APIs
   *##########################################################################*/

  /**
   * @brief Read the payload corresponding to a given key if it exists.
   *
   * @param key A search key.
   * @param key_len The length of a target key.
   * @retval The corresponding payload of a given key if exist.
   * @retval std::nullopt otherwise.
   */
  auto
  Read(  //
      const Key& key,
      const size_t key_len = sizeof(Key))  //
      -> std::optional<Payload>
    requires HasReadWithBufMethod<Index, Key, Payload>
  {
    PayloadBuf buf;
    if (index_.Read(key, key_len, &buf)) return buf.Get();
    return std::nullopt;
  }

  /**
   * @brief Perform a range scan in forward order with given keys.
   *
   * @param begin_key A pair of a begin key and its openness (true=closed).
   * @param end_key A pair of an end key and its openness (true=closed).
   * @return An iterator for accessing the records in forward order.
   */
  auto
  Scan(  //
      const ScanKey& begin_key = std::nullopt,
      const ScanKey& end_key = std::nullopt)
    requires HasScanMethod<Index, Key, Payload>
  {
    return index_.Scan(begin_key, end_key);
  }

  /**
   * @brief Perform a range scan in backward order with given keys.
   *
   * @param begin_key A pair of a begin key and its openness (true=closed).
   * @param end_key A pair of an end key and its openness (true=closed).
   * @return An iterator for accessing the records in backward order.
   */
  auto
  ScanBackward(  //
      const ScanKey& begin_key = std::nullopt,
      const ScanKey& end_key = std::nullopt)
    requires HasScanBackwardMethod<Index, Key, Payload>
  {
    return index_.ScanBackward(begin_key, end_key);
  }

  /*##########################################################################*
   * Public write APIs
   *##########################################################################*/

  /**
   * @brief Write (i.e., put) a given key/payload pair.
   *
   * @param key A target key.
   * @param payload A target payload.
   * @param key_len The length of a target key.
   * @param merger A function for merging payloads.
   */
  void
  Write(  //
      const Key& key,
      const Payload& payload,
      const size_t key_len = sizeof(Key),
      Merger<Payload, Payload> merger = nullptr)
    requires HasWriteWithBufMethod<Index, Key, Payload>
  {
    index_.Write(key, key_len, payload, sizeof(Payload), merger);
  }

  /**
   * @brief Upsert (update or insert) a given key/payload pair.
   *
   * @param key A target key.
   * @param payload A target payload.
   * @param key_len The length of a target key.
   * @param merger A function for merging payloads.
   * @retval The previous payload if exist (i.e., the function acts as update).
   * @retval std::nullopt otherwise.
   */
  auto
  Upsert(  //
      const Key& key,
      const Payload& payload,
      const size_t key_len = sizeof(Key),
      Merger<Payload, Payload> merger = nullptr)  //
      -> std::optional<Payload>
    requires HasUpsertWithBufMethod<Index, Key, Payload>
  {
    PayloadBuf buf;
    if (index_.Upsert(key, key_len, payload, sizeof(Payload), &buf, merger)) return buf.Get();
    return std::nullopt;
  }

  /**
   * @brief Insert a given key/payload pair.
   *
   * @param key A target key.
   * @param payload A target payload.
   * @param key_len The length of a target key.
   * @retval The current payload if exist (i.e., failed).
   * @retval std::nullopt otherwise (i.e., succeeded).
   */
  auto
  Insert(  //
      const Key& key,
      const Payload& payload,
      const size_t key_len = sizeof(Key))  //
      -> std::optional<Payload>
    requires HasInsertWithBufMethod<Index, Key, Payload>
  {
    PayloadBuf buf;
    if (index_.Insert(key, key_len, payload, sizeof(Payload), &buf)) return std::nullopt;
    return buf.Get();
  }

  /**
   * @brief Update a record using a given kay/payload pair.
   *
   * @tparam T A class of a given payload or delta.
   * @param key A target key.
   * @param payload A target payload or delta.
   * @param key_len The length of a target key.
   * @param merger A function for merging payloads.
   * @param pay_len The length of a target payload or delta.
   * @retval The previous payload if exist (i.e., succeeded).
   * @retval std::nullopt otherwise (i.e., failed).
   */
  template <class T = Payload>
  auto
  Update(  //
      const Key& key,
      const std::type_identity_t<T>& payload,
      const size_t key_len = sizeof(Key),
      Merger<Payload, T> merger = nullptr,
      const size_t pay_len = sizeof(T))  //
      -> std::optional<Payload>
    requires HasUpdateWithBufMethod<Index, Key, Payload>
  {
    PayloadBuf buf;
    if (index_.template Update<T>(key, key_len, payload, pay_len, &buf, merger)) {
      return buf.Get();
    }
    return std::nullopt;
  }

  /**
   * @brief Delete a record using a given kay.
   *
   * @param key A target key.
   * @param key_len The length of a target key.
   * @retval The deleted payload if exist (i.e., succeeded).
   * @retval std::nullopt otherwise (i.e., failed).
   */
  auto
  Delete(  //
      const Key& key,
      const size_t key_len = sizeof(Key))  //
      -> std::optional<Payload>
    requires HasDeleteWithBufMethod<Index, Key, Payload>
  {
    PayloadBuf buf;
    if (index_.Delete(key, key_len, &buf)) return buf.Get();
    return std::nullopt;
  }

  /*##########################################################################*
   * Public utilities
   *##########################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs.
   *
   * @tparam Entry A container for a key/payload pair.
   * @param entries All entries for bulkload.
   * @param thread_num The number of threads used for bulkload.
   */
  template <class Entry>
  void
  Bulkload(  //
      const std::vector<Entry>& entries,
      const size_t thread_num = 1)
    requires HasBulkloadMethod<Index, Key, Payload>
  {
    index_.Bulkload(entries, thread_num);
  }

  /**
   * @return The memory usage of an actual index.
   */
  [[nodiscard]]
  auto
  MemoryUsage()
    requires HasMemoryUsageMethod<Index>
  {
    return index_.MemoryUsage();
  }

  /**
   * @brief Set up an actual index for each thread.
   *
   */
  void
  SetUp()
    requires HasSetUpMethod<Index>
  {
    index_.SetUp();
  }

  /**
   * @brief Tear down an actual index for each thread.
   *
   */
  void
  TearDown()
    requires HasTearDownMethod<Index>
  {
    index_.TearDown();
  }

  /**
   * @brief Wait for other workers.
   *
   */
  void
  Barrier()
    requires HasBarrierMethod<Index>
  {
    index_.Barrier();
  }

  /**
   * @return The actual index.
   */
  [[nodiscard]]
  auto
  GetIndex() noexcept  //
      -> Index&
  {
    return index_;
  }

 private:
  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  static_assert(  //
      !IsVarLenData<Payload>(),
      "FixLenWrapper accepts only fixed-length payloads.");

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief An actual index.
  Index index_;
};

}  // namespace dbgroup::index

#endif  // CPP_UTILITY_DBGROUP_INDEX_FIX_LEN_WRAPPER_HPP_
