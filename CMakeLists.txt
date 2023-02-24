cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(cpp_utility
  VERSION "0.3.0"
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

if(NOT TARGET dbgroup::${PROJECT_NAME})
  # build as a header-only library
  add_library(${PROJECT_NAME} INTERFACE)
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} INTERFACE
    "cxx_std_17"
  )
  target_include_directories(${PROJECT_NAME} INTERFACE
    "${CPP_UTILITY_SOURCE_DIR}/include"
  )
  target_compile_definitions(${PROJECT_NAME} INTERFACE
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
