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

#ifndef CPP_UTILITY_TEST_COMMON_HPP
#define CPP_UTILITY_TEST_COMMON_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>

#ifdef CPP_UTILITY_TEST_THREAD_NUM
constexpr size_t kThreadNum = CPP_UTILITY_TEST_THREAD_NUM;
#else
constexpr size_t kThreadNum = 8;
#endif

#endif  // CPP_UTILITY_TEST_COMMON_HPP