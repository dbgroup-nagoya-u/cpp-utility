cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(cpp_utility
  VERSION "0.1.0"
  DESCRIPTION "An open-source MwCAS library."
  LANGUAGES "CXX"
)

# define source directories
set(CPP_UTILITY_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

# build as a header-only library
add_library(cpp_utility INTERFACE)
target_compile_features(cpp_utility INTERFACE "cxx_std_17")
target_compile_options(cpp_utility INTERFACE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_include_directories(cpp_utility INTERFACE
  "${CPP_UTILITY_SOURCE_DIR}/include"
)

# build unit tests if required
option(CPP_UTILITY_BUILD_TESTS, "Build unit tests" OFF)
if(${CPP_UTILITY_BUILD_TESTS})
  enable_testing()
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
