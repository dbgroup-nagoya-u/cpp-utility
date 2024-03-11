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

  #----------------------------------------------------------------------------#
  # Configuration
  #----------------------------------------------------------------------------#

  # check SSE support for spinlock hints
  CHECK_INCLUDE_FILE_CXX("xmmintrin.h" CPP_UTILITY_HAS_SPINLOCK_HINT)

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
  add_library(${PROJECT_NAME} INTERFACE)
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} INTERFACE
    "cxx_std_17"
  )
  target_include_directories(${PROJECT_NAME} INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_compile_definitions(${PROJECT_NAME} INTERFACE
    $<$<BOOL:${CPP_UTILITY_HAS_SPINLOCK_HINT}>:CPP_UTILITY_HAS_SPINLOCK_HINT>
    DBGROUP_MAX_THREAD_NUM=${DBGROUP_MAX_THREAD_NUM}
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
