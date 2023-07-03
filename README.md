# C++ Utility Library

![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/cpp-utility/workflows/Ubuntu-20.04/badge.svg?branch=main)

- [Build](#build)
    - [Prerequisites](#prerequisites)
    - [Build Options](#build-options)
    - [Build and Run Unit Tests](#build-and-run-unit-tests)
- [Usage](#usage)
    - [Linking by CMake](#linking-by-cmake)
    - [Example Usage](#example-usage)
- [Acknowledgments](#acknowledgments)

## Build

**Note**: this is a header-only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

- `DBGROUP_MAX_THREAD_NUM`: The maximum number of worker threads (default `1024`).

#### Parameters for Unit Testing

- `CPP_UTILITY_BUILD_TESTS`: Build unit tests if `ON` (default `OFF`).
- `CPP_UTILITY_TEST_THREAD_NUM`: The number of threads to run unit tests (default `8`).
- `CPP_UTILITY_TEST_RANDOM_SEED`: A fixed seed value to reproduce the results of unit tests (default `0`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCPP_UTILITY_BUILD_TESTS=ON ..
make -j
ctest -C Release
```

## Usage

### Linking by CMake

1. Download the files in any way you prefer (e.g., `git submodule`).

    ```bash
    cd <your_project_workspace>
    mkdir external
    git submodule add https://github.com/dbgroup-nagoya-u/cpp-utility.git external/cpp-utility
    ```

1. Add this library to your build in `CMakeLists.txt`.

    ```cmake
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/cpp-utility")

    add_executable(
      <target_bin_name>
      [<source> ...]
    )
    target_link_libraries(<target_bin_name> PRIVATE
      dbgroup::cpp-utility
    )
    ```

### Example Usage

The design summaries and examples of the developed classes are located in `design_doc`.

## Acknowledgments

This work is based on results from project JPNP16007 commissioned by the New Energy and Industrial Technology Development Organization (NEDO), and it was supported partially by KAKENHI (JP16H01722, JP20K19804, JP21H03555, and JP22H03594).
