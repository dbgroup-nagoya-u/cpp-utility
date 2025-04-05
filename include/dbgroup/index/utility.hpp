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

#ifndef CPP_UTILITY_DBGROUP_INDEX_UTILITY_HPP_
#define CPP_UTILITY_DBGROUP_INDEX_UTILITY_HPP_

// C++ standard libraries
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <tuple>
#include <type_traits>

// external system libraries
#ifdef CPP_UTILITY_HAS_BYTESWAP
#include <byteswap.h>
#endif

namespace dbgroup::index
{
/*############################################################################*
 * Global types
 *############################################################################*/

/**
 * @brief Return codes for DB Group's index implementations.
 *
 */
enum ReturnCode {
  kSuccess = 0,
  kKeyNotExist = -100,
  kKeyExist,
};

/**
 * @brief Compare binary keys as CString. The end of every key must be '\\0'.
 *
 */
struct CompareAsCString {
  /**
   * @param lhs A left-hand side instance.
   * @param rhs A right-hand side instance.
   * @retval true if `lhs < rhs`.
   * @retval false otherwise.
   */
  constexpr auto
  operator()(  //
      const void *lhs,
      const void *rhs) const noexcept  //
      -> bool
  {
    if (lhs == nullptr) return false;
    if (rhs == nullptr) return true;
    return std::strcmp(static_cast<const char *>(lhs), static_cast<const char *>(rhs)) < 0;
  }
};

/**
 * @brief A dummy struct for filling zeros in garbage collection.
 *
 * @tparam kPageSize A target page size.
 */
template <size_t kPageSize>
struct ZeroFilling {
  // Do not use as a general class.
  ZeroFilling() = delete;
  ZeroFilling(const ZeroFilling &) = delete;
  ZeroFilling(ZeroFilling &&) = delete;
  auto operator=(const ZeroFilling &) -> ZeroFilling & = delete;
  auto operator=(ZeroFilling &&) -> ZeroFilling & = delete;

  /// @brief Fill this page with zeros.
  ~ZeroFilling() { std::memset(std::bit_cast<void *>(this), 0, kPageSize); }
};

/*############################################################################*
 * Global constants
 *############################################################################*/

/// @brief A flag for indicating closed intervals
constexpr bool kClosed = true;

/// @brief A flag for indicating closed intervals
constexpr bool kOpen = false;

/*############################################################################*
 * Global utilities
 *############################################################################*/

/**
 * @tparam T a target class.
 * @retval true if a target class represents variable-length data.
 * @retval false otherwise.
 * @note By default, this function treats only the `std::byte *` as variable
 * length data. You need to specialize this function to add another variable
 * length type.
 */
template <class T>
constexpr auto
IsVarLenData() noexcept  //
    -> bool
{
  return std::is_same_v<T, std::byte *>;
}

/**
 * @tparam T A target class.
 * @retval true if a target class is trivially copyable.
 * @retval false otherwise.
 */
template <class T>
constexpr auto
IsTriviallyCopyable() noexcept  //
    -> bool
{
  using Target = std::conditional_t<IsVarLenData<T>(), std::remove_pointer_t<T>, T>;
  return std::is_trivially_copyable_v<Target>;
}

/**
 * @tparam Comp A comparator class.
 * @tparam T A target class.
 * @param lhs A left-hand side instance.
 * @param rhs A right-hand side instance.
 * @retval true if given instances are equivalent.
 * @retval false otherwise.
 */
template <class Comp, class T>
constexpr auto
IsEqual(  //
    const T &lhs,
    const T &rhs)  //
    -> bool
{
  return !Comp{}(lhs, rhs) && !Comp{}(rhs, lhs);
}

/**
 * @brief Shift a memory address by a byte offset.
 *
 * @param addr An original address.
 * @param offset An offset in bytes.
 * @return A shifted address.
 */
constexpr auto
ShiftAddr(  //
    const void *addr,
    const int64_t offset) noexcept  //
    -> void *
{
  return std::bit_cast<std::byte *>(addr) + offset;
}

/**
 * @tparam T A target class.
 * @param obj A target instance.
 * @return The address of a given instance for `std::memcpy` or `std::memcmp`.
 */
template <class T>
constexpr auto
GetSrcAddr(                 //
    const T &obj) noexcept  //
    -> std::conditional_t<IsVarLenData<T>(), const T, const T *>
{
  if constexpr (IsVarLenData<T>()) {
    return obj;
  } else {
    return &obj;
  }
}

/**
 * @brief Parse an entry for bulkload.
 *
 * @tparam Entry std::pair or std::tuple for storing entries.
 * @param entry A bulkload entry.
 * @retval 1st: A target key.
 * @retval 2nd: A target payload.
 * @retval 3rd: The length of a target key.
 * @retval 4th: The length of a target payload.
 */
template <class Entry>
constexpr auto
ParseEntry(                       //
    const Entry &entry) noexcept  //
    -> std::tuple<std::tuple_element_t<0, Entry>, std::tuple_element_t<1, Entry>, size_t, size_t>
{
  using Key = std::tuple_element_t<0, Entry>;
  using Payload = std::tuple_element_t<1, Entry>;

  constexpr auto kTupleSize = std::tuple_size_v<Entry>;
  static_assert(2 <= kTupleSize && kTupleSize <= 4);

  if constexpr (kTupleSize == 4) {
    return entry;
  } else if constexpr (kTupleSize == 3) {
    const auto &[key, payload, key_len] = entry;
    return {key, payload, key_len, sizeof(Payload)};
  } else {
    const auto &[key, payload] = entry;
    return {key, payload, sizeof(Key), sizeof(Payload)};
  }
}

/**
 * @brief Parse and extract a key from an entry for bulkload.
 *
 * @tparam Entry std::pair or std::tuple for containing entries.
 * @param entry A bulkload entry.
 * @retval 1st: A target key.
 * @retval 2nd: The length of a target key.
 */
template <class Entry>
constexpr auto
ParseKey(                         //
    const Entry &entry) noexcept  //
    -> std::pair<std::tuple_element_t<0, Entry>, size_t>
{
  using Key = std::tuple_element_t<0, Entry>;

  constexpr auto kTupleSize = std::tuple_size_v<Entry>;
  static_assert(2 <= kTupleSize && kTupleSize <= 4);

  if constexpr (kTupleSize == 4) {
    const auto &[key, payload, key_len, pay_len] = entry;
    return {key, key_len};
  } else if constexpr (kTupleSize == 3) {
    const auto &[key, payload, key_len] = entry;
    return {key, key_len};
  } else {
    const auto &[key, payload] = entry;
    return {key, sizeof(Key)};
  }
}

/**
 * @brief Parse and extract a payload from an entry for bulkload.
 *
 * @tparam Entry std::pair or std::tuple for storing entries.
 * @param entry A bulkload entry.
 * @retval 1st: A target payload.
 * @retval 2nd: The length of a target payload.
 */
template <class Entry>
constexpr auto
ParsePayload(                     //
    const Entry &entry) noexcept  //
    -> std::tuple<std::tuple_element_t<1, Entry>, size_t>
{
  using Payload = std::tuple_element_t<1, Entry>;

  constexpr auto kTupleSize = std::tuple_size_v<Entry>;
  static_assert(2 <= kTupleSize && kTupleSize <= 4);

  if constexpr (kTupleSize == 4) {
    const auto &[key, payload, key_len, pay_len] = entry;
    return {payload, pay_len};
  } else if constexpr (kTupleSize == 3) {
    const auto &[key, payload, key_len] = entry;
    return {payload, sizeof(Payload)};
  } else {
    const auto &[key, payload] = entry;
    return {payload, sizeof(Payload)};
  }
}

/**
 * @tparam UInt Unsigned integer types.
 * @param v An unsigned integer value.
 * @return A byte swapped value.
 */
template <class UInt>
auto
ByteSwap(    //
    UInt v)  //
    -> UInt
{
  static_assert(                                             //
      std::is_integral_v<UInt> && std::is_unsigned_v<UInt>,  //
      "An unexpected type has been given.");

#ifdef CPP_UTILITY_HAS_BYTESWAP
  if constexpr (std::is_same_v<UInt, uint64_t>) {
    return bswap_64(v);
  } else if constexpr (std::is_same_v<UInt, uint32_t>) {
    return bswap_32(v);
  } else {  // if constexpr (std::is_same_v<UInt, uint16_t>)
    return bswap_16(v);
  }
#else   // NOLINTBEGIN
  if constexpr (std::is_same_v<UInt, uint64_t>) {
    v = ((v & 0xff00ff00ff00ff00ULL) >> 8UL) | ((v & 0x00ff00ff00ff00ffULL) << 8UL);
    v = ((v & 0xffff0000ffff0000ULL) >> 16UL) | ((v & 0x0000ffff0000ffffULL) << 16UL);
    return (v >> 32UL) | (v << 32UL);
  } else if constexpr (std::is_same_v<UInt, uint32_t>) {
    v = ((v & 0xff00ff00UL) >> 8U) | ((v & 0x00ff00ffUL) << 8U);
    return (v >> 16U) | (v << 16U);
  } else {  // if constexpr (std::is_same_v<UInt, uint16_t>)
    return (v << 8) | (v >> 8);
  }
#endif  // NOLINTEND
}

/**
 * @brief Convert into binary data (i.e., comparable using `std::memcmp`).
 *
 * @tparam T A target type.
 * @tparam Binary A destination binary type.
 * @param src A source instance.
 * @return The address of a converted data.
 * @note This function returns the address of a given instance for pointers and
 * unspecialized types, so the converted data's consistency depends on the
 * source instance.
 * @note This function stores converted data in a thread-local variable, so you
 * must copy the converted data before calling this again.
 */
template <class T, class Binary = uint8_t>
auto
ConvertToBinaryData(  //
    const T &src)     //
    -> const Binary *
{
  thread_local uint64_t buf{};  // zero filled for 32 bit sources

  if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>) {
    /*------------------------*
     * Unsigend integers
     *------------------------*/
    auto *swapped = std::bit_cast<T *>(&buf);
    *swapped = ByteSwap(src);
    return std::bit_cast<const Binary *>(swapped);
  } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t>) {
    /*------------------------*
     * Sigend integers
     *------------------------*/
    using UInt = std::make_unsigned_t<T>;
    constexpr UInt kLSB = UInt{1} << (UInt{8} * sizeof(T) - UInt{1});
    auto *swapped = std::bit_cast<UInt *>(&buf);
    *swapped = ByteSwap(std::bit_cast<UInt>(src) ^ kLSB);
    return std::bit_cast<const Binary *>(swapped);
  } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
    /*------------------------*
     * Floating point numbers
     *------------------------*/
    using UInt = std::conditional_t<std::is_same_v<T, float>, uint32_t, uint64_t>;
    constexpr UInt kLSB = UInt{1} << (UInt{8} * sizeof(T) - UInt{1});
    constexpr UInt kMask = ~UInt{0};
    auto *swapped = std::bit_cast<UInt *>(&buf);
    if (std::isnan(src)) {
      *swapped = 0;
    } else if (std::signbit(src)) {
      *swapped = ByteSwap(std::bit_cast<UInt>(src) ^ kMask);
    } else {
      *swapped = ByteSwap(std::bit_cast<UInt>(src) ^ kLSB);
    }
    return std::bit_cast<const Binary *>(swapped);
  } else if constexpr (std::is_array_v<T> || std::is_pointer_v<T>) {
    /*------------------------*
     * Pointers
     *------------------------*/
    return std::bit_cast<const Binary *>(src);
  } else {
    /*------------------------*
     * Others
     *------------------------*/
    return std::bit_cast<const Binary *>(&src);
  }
}

/**
 * @brief Convert a given binary data into a destination type.
 *
 * @tparam T A destination type.
 * @tparam Binary A source binary type.
 * @param src A source binary data.
 * @return A converted data.
 * @note This function returns the address of a given binary data for pointers,
 * so the converted data's consistency depends on the source instance.
 */
template <class T, class Binary = uint8_t>
auto
ConvertFromBinaryData(  //
    const Binary *src)  //
    -> T
{
  if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>) {
    /*------------------------*
     * Unsigend integers
     *------------------------*/
    return ByteSwap(*std::bit_cast<T *>(src));
  } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t>) {
    /*------------------------*
     * Sigend integers
     *------------------------*/
    using UInt = std::make_unsigned_t<T>;
    constexpr UInt kLSB = UInt{1} << (UInt{8} * sizeof(T) - UInt{1});
    return std::bit_cast<T>(ByteSwap(*std::bit_cast<UInt *>(src)) ^ kLSB);
  } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
    /*------------------------*
     * Floating point numbers
     *------------------------*/
    using UInt = std::conditional_t<std::is_same_v<T, float>, uint32_t, uint64_t>;
    constexpr UInt kLSB = UInt{1} << (UInt{8} * sizeof(T) - UInt{1});
    constexpr UInt kMask = ~UInt{0};
    const auto swapped = ByteSwap(*std::bit_cast<UInt *>(src));
    if (swapped == 0) return NAN;
    if (swapped & kLSB) return std::bit_cast<T>(swapped ^ kLSB);
    return std::bit_cast<T>(swapped ^ kMask);
  } else if constexpr (std::is_array_v<T> || std::is_pointer_v<T>) {
    /*------------------------*
     * Pointers
     *------------------------*/
    return std::bit_cast<T>(src);
  } else {
    /*------------------------*
     * Others
     *------------------------*/
    return *std::bit_cast<const T *>(src);
  }
}

}  // namespace dbgroup::index

#endif  // CPP_UTILITY_DBGROUP_INDEX_UTILITY_HPP_
