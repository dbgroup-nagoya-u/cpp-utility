# C++ Utility Library

![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/cpp-utility/workflows/Ubuntu-20.04/badge.svg?branch=main)

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Parameters for Unit Testing

- `CPP_UTILITY_BUILD_TESTS`: build unit tests if `ON`: default `OFF`.
- `CPP_UTILITY_TEST_THREAD_NUM`: the number of threads to run unit tests: default `8`.

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCPP_UTILITY_BUILD_TESTS=ON ..
make -j
ctest -C Release
```
