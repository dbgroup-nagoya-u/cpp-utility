# C++ Utility Library

[![Ubuntu 22.04](https://github.com/dbgroup-nagoya-u/cpp-utility/actions/workflows/ubuntu_22.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/cpp-utility/actions/workflows/ubuntu_22.yaml) [![Ubuntu 20.04](https://github.com/dbgroup-nagoya-u/cpp-utility/actions/workflows/ubuntu_20.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/cpp-utility/actions/workflows/ubuntu_20.yaml) [![macOS](https://github.com/dbgroup-nagoya-u/cpp-utility/actions/workflows/mac.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/cpp-utility/actions/workflows/mac.yaml)

- [Build](#build)
    - [Prerequisites](#prerequisites)
    - [Build Options](#build-options)
    - [Build and Run Unit Tests](#build-and-run-unit-tests)
- [Usage](#usage)
    - [Linking by CMake](#linking-by-cmake)
    - [Example Usage](#example-usage)
- [Acknowledgments](#acknowledgments)

## Build

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

- `DBGROUP_MAX_THREAD_NUM`: The maximum number of worker threads (defaults to the number of logical cores x2).
- `CPP_UTILITY_SPINLOCK_RETRY_NUM`: The number of spinlock retries (default `10`).
- `CPP_UTILITY_BACKOFF_TIME`: A back-off time interval in microseconds (default `10`).

#### Parameters for Unit Testing

- `CPP_UTILITY_BUILD_TESTS`: Build unit tests if `ON` (default `OFF`).
- `CPP_UTILITY_TEST_THREAD_NUM`: The number of threads to run unit tests (default `2`).
- `CPP_UTILITY_TEST_RANDOM_SEED`: A fixed seed value to reproduce the results of unit tests (default `0`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCPP_UTILITY_BUILD_TESTS=ON
cmake --build . --parallel --config Release
ctest -C Release --output-on-failure
```

## Usage

### Linking by CMake

1. Add this library to your build in `CMakeLists.txt`.

    ```cmake
    FetchContent_Declare(
        cpp-utility
        GIT_REPOSITORY "https://github.com/dbgroup-nagoya-u/cpp-utility.git"
        GIT_TAG "<commit_tag_>"
    )
    FetchContent_MakeAvailable(cpp-utility)

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
