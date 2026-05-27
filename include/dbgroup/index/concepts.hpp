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

#ifndef CPP_UTILITY_DBGROUP_INDEX_CONCEPTS_HPP_
#define CPP_UTILITY_DBGROUP_INDEX_CONCEPTS_HPP_

// C++ standard libraries
#include <concepts>
#include <cstddef>
#include <optional>
#include <tuple>
#include <vector>

namespace dbgroup::index
{
/*############################################################################*
 * Concepts to detect implemented APIs
 *############################################################################*/

template <class Index, class Key, class Payload>
concept HasReadMethod = requires(Index& idx, Key key, size_t key_len) {
  { idx.Read(key, key_len) } -> std::same_as<std::optional<Payload>>;
};

template <class Index, class Key, class Payload>
constexpr auto
HasRead() noexcept  //
    -> bool
{
  return HasReadMethod<Index, Key, Payload>;
}

template <class Iterator, class Key, class Payload>
concept IteratorConcept = requires(Iterator& it) {
  { ++it };
  { *it } -> std::same_as<std::pair<Key, Payload>>;
  { static_cast<bool>(it) } -> std::same_as<bool>;
};

template <class Index, class Key, class Payload>
concept HasScanMethod =
    requires(Index& idx, std::optional<std::tuple<Key, size_t, bool>> scan_key) {
      { idx.Scan(scan_key, scan_key) };
      requires IteratorConcept<decltype(idx.Scan(scan_key, scan_key)), Key, Payload>;
    };

template <class Index, class Key, class Payload>
constexpr auto
HasScan() noexcept  //
    -> bool
{
  return HasScanMethod<Index, Key, Payload>;
}

template <class Index, class Key, class Payload>
concept HasScanBackwardMethod =
    requires(Index& idx, std::optional<std::tuple<Key, size_t, bool>> scan_key) {
      { idx.ScanBackward(scan_key, scan_key) };
      requires IteratorConcept<decltype(idx.ScanBackward(scan_key, scan_key)), Key, Payload>;
    };

template <class Index, class Key, class Payload>
constexpr auto
HasScanBackward() noexcept  //
    -> bool
{
  return HasScanBackwardMethod<Index, Key, Payload>;
}

template <class Index, class Key, class Payload>
concept HasWriteMethod = requires(Index& idx, Key key, Payload payload, size_t key_len) {
  { idx.Write(key, payload, key_len) };
};

template <class Index, class Key, class Payload>
constexpr auto
HasWrite() noexcept  //
    -> bool
{
  return HasWriteMethod<Index, Key, Payload>;
}

template <class Index, class Key, class Payload>
concept HasUpsertMethod = requires(Index& idx, Key key, Payload payload, size_t key_len) {
  { idx.Upsert(key, payload, key_len) } -> std::same_as<std::optional<Payload>>;
};

template <class Index, class Key, class Payload>
constexpr auto
HasUpsert() noexcept  //
    -> bool
{
  return HasUpsertMethod<Index, Key, Payload>;
}

template <class Index, class Key, class Payload>
concept HasInsertMethod = requires(Index& idx, Key key, Payload payload, size_t key_len) {
  { idx.Insert(key, payload, key_len) } -> std::same_as<std::optional<Payload>>;
};

template <class Index, class Key, class Payload>
constexpr auto
HasInsert() noexcept  //
    -> bool
{
  return HasInsertMethod<Index, Key, Payload>;
}

template <class Index, class Key, class Payload>
concept HasUpdateMethod = requires(Index& idx, Key key, Payload payload, size_t key_len) {
  { idx.Update(key, payload, key_len) } -> std::same_as<std::optional<Payload>>;
};

template <class Index, class Key, class Payload>
constexpr auto
HasUpdate() noexcept  //
    -> bool
{
  return HasUpdateMethod<Index, Key, Payload>;
}

template <class Index, class Key, class Payload>
concept HasDeleteMethod = requires(Index& idx, Key key, size_t key_len) {
  { idx.Delete(key, key_len) } -> std::same_as<std::optional<Payload>>;
};

template <class Index, class Key, class Payload>
constexpr auto
HasDelete() noexcept  //
    -> bool
{
  return HasDeleteMethod<Index, Key, Payload>;
}

template <class Index, class Key, class Payload>
concept HasBulkloadMethod =
    requires(Index& idx, std::vector<std::tuple<Key, Payload, size_t>> records, size_t thread_num) {
      { idx.Bulkload(records, thread_num) };
    };

template <class Index, class Key, class Payload>
constexpr auto
HasBulkload() noexcept  //
    -> bool
{
  return HasBulkloadMethod<Index, Key, Payload>;
}

template <class Index>
concept HasSetUpMethod = requires(Index& idx) {
  { idx.SetUp() };
};

template <class Index>
constexpr auto
HasSetUp() noexcept  //
    -> bool
{
  return HasSetUpMethod<Index>;
}

template <class Index>
concept HasTearDownMethod = requires(Index& idx) {
  { idx.TearDown() };
};

template <class Index>
constexpr auto
HasTearDown() noexcept  //
    -> bool
{
  return HasTearDownMethod<Index>;
}

template <class Index>
concept HasBarrierMethod = requires(Index& idx) {
  { idx.Barrier() };
};

template <class Index>
constexpr auto
HasBarrier() noexcept  //
    -> bool
{
  return HasBarrierMethod<Index>;
}

}  // namespace dbgroup::index

#endif  // CPP_UTILITY_DBGROUP_INDEX_CONCEPTS_HPP_
