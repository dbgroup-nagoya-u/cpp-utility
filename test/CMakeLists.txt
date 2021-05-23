#--------------------------------------------------------------------------------------#
# Configure GoogleTest
#--------------------------------------------------------------------------------------#

# Prevent overriding the parent INDEX_BENCH's compiler/linker settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/googletest")

#--------------------------------------------------------------------------------------#
# Build unit tests
#--------------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(ADD_CPP_UTILITY_TEST CPP_UTILITY_TARGET_DIR CPP_UTILITY_TEST_TARGET)
  add_executable(${CPP_UTILITY_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${CPP_UTILITY_TARGET_DIR}/${CPP_UTILITY_TEST_TARGET}.cpp"
  )
  target_compile_features(${CPP_UTILITY_TEST_TARGET} PRIVATE "cxx_std_17")
  target_compile_options(${CPP_UTILITY_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_include_directories(${CPP_UTILITY_TEST_TARGET} PRIVATE
    "${CPP_UTILITY_SOURCE_DIR}/include"
  )
  target_link_libraries(${CPP_UTILITY_TEST_TARGET} PRIVATE
    gtest_main
    pthread
  )
  add_test(NAME ${CPP_UTILITY_TEST_TARGET} COMMAND $<TARGET_FILE:${CPP_UTILITY_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_CPP_UTILITY_TEST("random" "zipf_test")
