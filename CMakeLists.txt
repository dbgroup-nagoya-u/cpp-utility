cmake_minimum_required(VERSION "3.11")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(cpp_utility
  VERSION "0.14.2"
  DESCRIPTION "An open-source C++ utility library for research use."
  LANGUAGES "CXX"
)

if(NOT TARGET dbgroup::${PROJECT_NAME})
  # enable features
  include(FetchContent)
  include(CheckIncludeFileCXX)

  #----------------------------------------------------------------------------#
  # Build parameters
  #----------------------------------------------------------------------------#

  set(
    DBGROUP_WORD_SIZE
    "8" CACHE STRING
    "The expected cache line size in bytes."
  )

  set(
    DBGROUP_CACHE_LINE_SIZE
    "64" CACHE STRING
    "The expected cache line size in bytes."
  )

  set(
    DBGROUP_VIRTUAL_PAGE_SIZE
    "4096" CACHE STRING
    "The expected page size of virtual memory in bytes."
  )

  set(
    DBGROUP_MAX_THREAD_NUM
    "1024" CACHE STRING
    "The maximum number of worker threads."
  )

  set(
    DBGROUP_LOGICAL_CORE_NUM
    "" CACHE STRING
    "The number of logical cores."
  )

  set(
    CPP_UTILITY_SPINLOCK_RETRY_NUM
    "10" CACHE STRING
    "The number of spinlock retries."
  )

  set(
    CPP_UTILITY_BACKOFF_TIME
    "10" CACHE STRING
    "A back-off time interval in microseconds."
  )

  #----------------------------------------------------------------------------#
  # Configuration
  #----------------------------------------------------------------------------#

  # check SSE support for spinlock hints
  CHECK_INCLUDE_FILE_CXX("x86intrin.h" CPP_UTILITY_HAS_SPINLOCK_HINT)

  # check a header for byteswap functions
  CHECK_INCLUDE_FILE_CXX("byteswap.h" CPP_UTILITY_HAS_BYTESWAP)

  # set the default number of threads if needed
  if ("${DBGROUP_LOGICAL_CORE_NUM}" STREQUAL "")
    message("[${PROJECT_NAME}] DBGROUP_LOGICAL_CORE_NUM is not set, so check the number of CPU cores.")

    # check the number of logical cores
    include(ProcessorCount)
    ProcessorCount(DBGROUP_LOGICAL_CORE_NUM)
    if(NOT DBGROUP_LOGICAL_CORE_NUM EQUAL 0)
      message("[${PROJECT_NAME}] The detected number of cores: ${DBGROUP_LOGICAL_CORE_NUM}")
    else()
      message(WARNING "[${PROJECT_NAME}] The number of cores could not be detected, so use 64 as default.")
      set(
        DBGROUP_LOGICAL_CORE_NUM
        "64" CACHE STRING
        "The number of logical cores." FORCE
      )
    endif()
  endif()

  #----------------------------------------------------------------------------#
  # Build targets
  #----------------------------------------------------------------------------#

  # build as a header-only library
  add_library(${PROJECT_NAME} STATIC
    "${CMAKE_CURRENT_SOURCE_DIR}/src/lock/pessimistic_lock.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/lock/optimistic_lock.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/lock/mcs_lock.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/lock/optiql.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/random/zipf.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/thread/id_manager.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/thread/epoch_manager.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/thread/epoch_guard.cpp"
  )
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} PUBLIC
    "cxx_std_20"
  )
  target_compile_options(${PROJECT_NAME} PRIVATE
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Release">:-march=native>
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Debug">:-g3>
    -Wall
    -Wextra
  )
  target_include_directories(${PROJECT_NAME} PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_compile_definitions(${PROJECT_NAME} PUBLIC
    DBGROUP_WORD_SIZE=${DBGROUP_WORD_SIZE}
    DBGROUP_CACHE_LINE_SIZE=${DBGROUP_CACHE_LINE_SIZE}
    DBGROUP_VIRTUAL_PAGE_SIZE=${DBGROUP_VIRTUAL_PAGE_SIZE}
    DBGROUP_MAX_THREAD_NUM=${DBGROUP_MAX_THREAD_NUM}
    DBGROUP_LOGICAL_CORE_NUM=${DBGROUP_LOGICAL_CORE_NUM}
    $<$<BOOL:${CPP_UTILITY_HAS_SPINLOCK_HINT}>:CPP_UTILITY_HAS_SPINLOCK_HINT>
    $<$<BOOL:${CPP_UTILITY_HAS_BYTESWAP}>:CPP_UTILITY_HAS_BYTESWAP>
    CPP_UTILITY_SPINLOCK_RETRY_NUM=${CPP_UTILITY_SPINLOCK_RETRY_NUM}
    CPP_UTILITY_BACKOFF_TIME=${CPP_UTILITY_BACKOFF_TIME}
  )

  #----------------------------------------------------------------------------#
  # Build Unit Tests
  #----------------------------------------------------------------------------#

  option(CPP_UTILITY_BUILD_TESTS, "Build unit tests" OFF)
  if(${CPP_UTILITY_BUILD_TESTS})
    enable_testing()
    # include unit tests
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
  endif()
endif()
