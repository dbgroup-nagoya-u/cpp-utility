/*
 * Copyright 2021 Database Group, Nagoya University
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

#ifndef CPP_UTILITY_TEST_COMMON_HPP_
#define CPP_UTILITY_TEST_COMMON_HPP_

#include <cassert>
#include <cstddef>
#include <cstdint>

/// the number of threads for testing.
constexpr size_t kThreadNum = DBGROUP_TEST_THREAD_NUM;

/// a fixed seed value to reproduce unit tests.
constexpr size_t kRandomSeed = DBGROUP_TEST_RANDOM_SEED;

namespace dbgroup::lock::test
{
/*############################################################################*
 * Internal enum and constants
 *############################################################################*/

enum LockType {
  kFree,
  kSLock,
  kXLock,
  kSIXLock,
};

}  // namespace dbgroup::lock::test

#endif  // CPP_UTILITY_TEST_COMMON_HPP_
