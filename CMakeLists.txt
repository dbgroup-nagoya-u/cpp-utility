cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(cpp_utility
  VERSION "0.2.0"
  DESCRIPTION "An open-source C++ utility library for reseach use."
  LANGUAGES "CXX"
)

# define source directories
set(CPP_UTILITY_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

# check SSE support for spinlock hints
include(CheckIncludeFileCXX)
CHECK_INCLUDE_FILE_CXX("xmmintrin.h" CPP_UTILITY_HAS_SPINLOCK_HINT)

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET cpp_utility::cpp_utility)
  # build as a header-only library
  add_library(cpp_utility INTERFACE)
  add_library(cpp_utility::cpp_utility ALIAS cpp_utility)
  target_compile_features(cpp_utility INTERFACE
    "cxx_std_17"
  )
  target_include_directories(cpp_utility INTERFACE
    "${CPP_UTILITY_SOURCE_DIR}/include"
  )
  target_compile_definitions(cpp_utility INTERFACE
    $<$<BOOL:${CPP_UTILITY_HAS_SPINLOCK_HINT}>:CPP_UTILITY_HAS_SPINLOCK_HINT>
  )
endif()

#--------------------------------------------------------------------------------------#
# Build Unit Tests
#--------------------------------------------------------------------------------------#

option(CPP_UTILITY_BUILD_TESTS, "Build unit tests" OFF)
if(${CPP_UTILITY_BUILD_TESTS})
  enable_testing()
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
