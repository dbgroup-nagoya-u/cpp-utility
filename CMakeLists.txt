cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(cpp_utility
  VERSION "0.7.0"
  DESCRIPTION "An open-source C++ utility library for reseach use."
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
    DBGROUP_MAX_THREAD_NUM
    "" CACHE STRING
    "The maximum number of worker threads."
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

  # set the default number of threads if needed
  if ("${DBGROUP_MAX_THREAD_NUM}" STREQUAL "")
    message("[${PROJECT_NAME}] DBGROUP_MAX_THREAD_NUM is not set, so check the number of CPU cores.")

    # check the number of logical cores
    include(ProcessorCount)
    ProcessorCount(CPP_UTILITY_MAX_CORES)
    if(NOT CPP_UTILITY_MAX_CORES EQUAL 0)
      message("[${PROJECT_NAME}] The detected number of cores: ${CPP_UTILITY_MAX_CORES}")
      set(
        DBGROUP_MAX_THREAD_NUM
        "(2 * ${CPP_UTILITY_MAX_CORES})" CACHE STRING
        "The number of worker threads." FORCE
      )
    else()
      message(WARNING "[${PROJECT_NAME}] The number of cores could not be detected.")
      set(
        DBGROUP_MAX_THREAD_NUM
        "256" CACHE STRING
        "The number of worker threads." FORCE
      )
    endif()
  endif()
  message("[${PROJECT_NAME}] Use ${DBGROUP_MAX_THREAD_NUM} as the maximum number of worker threads.")

  #----------------------------------------------------------------------------#
  # Build targets
  #----------------------------------------------------------------------------#

  # build as a header-only library
  add_library(${PROJECT_NAME} STATIC
    "${CMAKE_CURRENT_SOURCE_DIR}/src/lock/pessimistic_lock.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/lock/optimistic_lock.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/random/zipf.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/thread/id_manager.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/thread/epoch_manager.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/thread/epoch_guard.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/thread/component/epoch.cpp"
  )
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} PUBLIC
    "cxx_std_17"
  )
  target_compile_options(${PROJECT_NAME} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_include_directories(${PROJECT_NAME} PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_compile_definitions(${PROJECT_NAME} PUBLIC
    $<$<BOOL:${CPP_UTILITY_HAS_SPINLOCK_HINT}>:CPP_UTILITY_HAS_SPINLOCK_HINT>
    DBGROUP_MAX_THREAD_NUM=${DBGROUP_MAX_THREAD_NUM}
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
