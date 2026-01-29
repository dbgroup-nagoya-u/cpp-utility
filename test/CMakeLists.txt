#------------------------------------------------------------------------------#
# Configure GoogleTest
#------------------------------------------------------------------------------#

FetchContent_Declare(
  googletest
  GIT_REPOSITORY "https://github.com/google/googletest.git"
  GIT_TAG "f8d7d77c06936315286eb55f8de22cd23c188571" # v1.14.0
)
FetchContent_MakeAvailable(googletest)

#------------------------------------------------------------------------------#
# Build parameters
#------------------------------------------------------------------------------#

set(
  DBGROUP_TEST_THREAD_NUM
  "2" CACHE STRING
  "The number of worker threads for testing."
)

set(
  DBGROUP_TEST_RANDOM_SEED
  "0" CACHE STRING
  "A fixed seed value to reproduce unit tests."
)

#------------------------------------------------------------------------------#
# Build unit tests
#------------------------------------------------------------------------------#

# unit tests use multi-threads
find_package(Threads)

# define function to add unit tests in the same format
function(ADD_DBGROUP_TEST DBGROUP_TEST_TARGET)
  add_executable(${DBGROUP_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${DBGROUP_TEST_TARGET}.cpp"
  )
  target_compile_options(${DBGROUP_TEST_TARGET} PRIVATE
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Release">:-march=native>
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Debug">:-g3>
    -Wall
    -Wextra
  )
  target_include_directories(${DBGROUP_TEST_TARGET} PRIVATE
    "${PROJECT_SOURCE_DIR}/test"
  )
  target_compile_definitions(${DBGROUP_TEST_TARGET} PRIVATE
    DBGROUP_TEST_THREAD_NUM=${DBGROUP_TEST_THREAD_NUM}
    DBGROUP_TEST_RANDOM_SEED=${DBGROUP_TEST_RANDOM_SEED}
  )
  target_link_libraries(${DBGROUP_TEST_TARGET} PRIVATE
    dbgroup::${PROJECT_NAME}
    GTest::gtest_main
    Threads::Threads
  )

  add_test(NAME ${DBGROUP_TEST_TARGET} COMMAND $<TARGET_FILE:${DBGROUP_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_DBGROUP_TEST("serial_test")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/benchmark")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/index")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/lock")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/random")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/thread")
