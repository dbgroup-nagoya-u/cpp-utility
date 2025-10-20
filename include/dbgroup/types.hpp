/*
 * Copyright 2025 Database Group, Nagoya University
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

#ifndef CPP_UTILITY_DBGROUP_TYPES_HPP_
#define CPP_UTILITY_DBGROUP_TYPES_HPP_

// C++ standard libraries
#include <atomic>
#include <cstddef>

namespace dbgroup
{
/*############################################################################*
 * Global types
 *############################################################################*/

/**
 * @brief A class for representing serial numbers.
 *
 * @tparam UInt An unsigned integer type for internal representation.
 * @note This class is based on Serial Number Arithmetic
 * ([RFC1982](https://www.rfc-editor.org/rfc/rfc1982)).
 */
template <class UInt>
struct Serial {
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Int = std::make_signed_t<UInt>;

  /*##########################################################################*
   * Public assignment operators
   *##########################################################################*/

  constexpr auto
  operator+=(                 //
      const UInt n) noexcept  //
      -> Serial &
  {
    v += n;
    return *this;
  }

  constexpr auto
  operator+=(                     //
      const Serial rhs) noexcept  //
      -> Serial &
  {
    v += rhs.v;
    return *this;
  }

  constexpr auto
  operator-=(                 //
      const UInt n) noexcept  //
      -> Serial &
  {
    v -= n;
    return *this;
  }

  constexpr auto
  operator-=(                     //
      const Serial rhs) noexcept  //
      -> Serial &
  {
    v -= rhs.v;
    return *this;
  }

  constexpr auto
  operator&=(                 //
      const UInt n) noexcept  //
      -> Serial &
  {
    v &= n;
    return *this;
  }

  constexpr auto
  operator&=(                     //
      const Serial rhs) noexcept  //
      -> Serial &
  {
    v &= rhs.v;
    return *this;
  }

  constexpr auto
  operator|=(                 //
      const UInt n) noexcept  //
      -> Serial &
  {
    v |= n;
    return *this;
  }

  constexpr auto
  operator|=(                     //
      const Serial rhs) noexcept  //
      -> Serial &
  {
    v |= rhs.v;
    return *this;
  }

  constexpr auto
  operator^=(                 //
      const UInt n) noexcept  //
      -> Serial &
  {
    v ^= n;
    return *this;
  }

  constexpr auto
  operator^=(                     //
      const Serial rhs) noexcept  //
      -> Serial &
  {
    v ^= rhs.v;
    return *this;
  }

  constexpr auto
  operator<<=(                //
      const UInt n) noexcept  //
      -> Serial &
  {
    v <<= n;
    return *this;
  }

  constexpr auto
  operator>>=(                //
      const UInt n) noexcept  //
      -> Serial &
  {
    v >>= n;
    return *this;
  }

  /*##########################################################################*
   * Public increment/decrement operators
   *##########################################################################*/

  constexpr auto
  operator++() noexcept  //
      -> Serial &
  {
    ++v;
    return *this;
  }

  constexpr auto
  operator++(int) noexcept  //
      -> Serial
  {
    auto ret = *this;
    ++v;
    return ret;
  }

  constexpr auto
  operator--() noexcept  //
      -> Serial &
  {
    --v;
    return *this;
  }

  constexpr auto
  operator--(int) noexcept  //
      -> Serial
  {
    auto ret = *this;
    --v;
    return ret;
  }

  /*##########################################################################*
   * Public arithmetic operators
   *##########################################################################*/

  [[nodiscard]] constexpr auto
  operator+(                        //
      const UInt n) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v + n)};
  }

  [[nodiscard]] constexpr auto
  operator+(                            //
      const Serial rhs) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v + rhs.v)};
  }

  [[nodiscard]] constexpr auto
  operator-(                        //
      const UInt n) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v - n)};
  }

  [[nodiscard]] constexpr auto
  operator-(                            //
      const Serial rhs) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v - rhs.v)};
  }

  constexpr auto
  operator~() noexcept  //
      -> Serial &
  {
    v = ~v;
    return *this;
  }

  [[nodiscard]] constexpr auto
  operator&(                        //
      const UInt n) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v & n)};
  }

  [[nodiscard]] constexpr auto
  operator&(                            //
      const Serial rhs) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v & rhs.v)};
  }

  [[nodiscard]] constexpr auto
  operator|(                        //
      const UInt n) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v | n)};
  }

  [[nodiscard]] constexpr auto
  operator|(                            //
      const Serial rhs) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v | rhs.v)};
  }

  [[nodiscard]] constexpr auto
  operator^(                        //
      const UInt n) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v ^ n)};
  }

  [[nodiscard]] constexpr auto
  operator^(                            //
      const Serial rhs) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v ^ rhs.v)};
  }

  [[nodiscard]] constexpr auto
  operator<<(                       //
      const UInt n) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v << n)};
  }

  [[nodiscard]] constexpr auto
  operator>>(                       //
      const UInt n) const noexcept  //
      -> Serial
  {
    return Serial{static_cast<UInt>(v >> n)};
  }

  /*##########################################################################*
   * Public comparison operators
   *##########################################################################*/

  [[nodiscard]] constexpr auto
  operator==(                          //
      const UInt &rhs) const noexcept  //
      -> bool
  {
    return v == rhs;
  }

  [[nodiscard]] constexpr auto
  operator==(                            //
      const Serial &rhs) const noexcept  //
      -> bool
  {
    return v == rhs.v;
  }

  [[nodiscard]] constexpr auto
  operator!=(                          //
      const UInt &rhs) const noexcept  //
      -> bool
  {
    return v != rhs;
  }

  [[nodiscard]] constexpr auto
  operator!=(                            //
      const Serial &rhs) const noexcept  //
      -> bool
  {
    return v != rhs.v;
  }

  [[nodiscard]] constexpr auto
  operator<(                           //
      const UInt &rhs) const noexcept  //
      -> bool
  {
    return static_cast<Int>(v - rhs) < 0;
  }

  [[nodiscard]] constexpr auto
  operator<(                             //
      const Serial &rhs) const noexcept  //
      -> bool
  {
    return static_cast<Int>(v - rhs.v) < 0;
  }

  [[nodiscard]] constexpr auto
  operator<=(                          //
      const UInt &rhs) const noexcept  //
      -> bool
  {
    return static_cast<Int>(v - rhs) <= 0;
  }

  [[nodiscard]] constexpr auto
  operator<=(                            //
      const Serial &rhs) const noexcept  //
      -> bool
  {
    return static_cast<Int>(v - rhs.v) <= 0;
  }

  [[nodiscard]] constexpr auto
  operator>(                           //
      const UInt &rhs) const noexcept  //
      -> bool
  {
    return static_cast<Int>(v - rhs) > 0;
  }

  [[nodiscard]] constexpr auto
  operator>(                             //
      const Serial &rhs) const noexcept  //
      -> bool
  {
    return static_cast<Int>(v - rhs.v) > 0;
  }

  [[nodiscard]] constexpr auto
  operator>=(                          //
      const UInt &rhs) const noexcept  //
      -> bool
  {
    return static_cast<Int>(v - rhs) >= 0;
  }

  [[nodiscard]] constexpr auto
  operator>=(                            //
      const Serial &rhs) const noexcept  //
      -> bool
  {
    return static_cast<Int>(v - rhs.v) >= 0;
  }

  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  static_assert(  //
      std::is_integral_v<UInt> && std::is_unsigned_v<UInt>,
      "A value type must be an unsigned integer.");

  /*##########################################################################*
   * Public member variables
   *##########################################################################*/

  /// @brief An actual value.
  UInt v{};
};

// aliases
using Serial8_t = Serial<uint8_t>;
using Serial16_t = Serial<uint16_t>;
using Serial32_t = Serial<uint32_t>;
using Serial64_t = Serial<uint64_t>;

}  // namespace dbgroup

#endif  // CPP_UTILITY_DBGROUP_TYPES_HPP_
