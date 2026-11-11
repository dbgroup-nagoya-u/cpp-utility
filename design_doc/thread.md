# `::dbgroup::thread`

- [class IDManager](#class-idmanager)
    - [Example of Usages](#example-of-usages)
- [class EpochManager](#class-epochmanager)

## class IDManager

This class manages thread IDs in a single process and has the following APIs.

The `IDManager::GetThreadID` function returns a unique thread ID in [0, `IDManager::GetMaxThreadNum()`). Since the assigned thread ID is maintained in the thread's local storage, this function always returns the same ID for the same thread, and the ID is released when the thread exits. The maximum number of threads is initialized with `DBGROUP_MAX_THREAD_NUM` (twice the number of logical cores if it is not specified), and you can change it using the `IDManager::SetMaxThreadNum` function during process initialization. If all IDs are reserved, `IDManager::GetThreadID` throws `std::range_error`.

The `IDManager::GetHeartBeat` function returns a `std::weak_ptr<size_t>` instance to allow you to check the alive monitoring of a particular thread. The `expired` function returns `false` until the corresponding thread exits.

### Example of Usages

```cpp
// C++ standard libraries
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

// our libraries
#include "dbgroup/thread/id_manager.hpp"

auto
main(  //
    const int argc,
    const char *argv[])  //
    -> int
{
  size_t id{0};
  std::weak_ptr<size_t> heartbeat{};
  std::atomic_bool assigned{false};
  std::mutex mtx{};

  // acquire a lock to prevent a worker thread
  std::unique_lock lock{mtx};

  // create a thread and get its ID and heartbeat
  std::thread t{[&] {
    id = ::dbgroup::thread::IDManager::GetThreadID();
    heartbeat = ::dbgroup::thread::IDManager::GetHeartBeat();
    assigned = true;
    std::lock_guard block{mtx};
  }};
  while (!assigned) {
    std::this_thread::sleep_for(std::chrono::microseconds{1});
  }

  // check the thread ID and aliveness
  std::cout << "id: " << id << std::endl  //
            << "thread alive: " << !heartbeat.expired() << std::endl;

  // the worker thread exits
  lock.unlock();
  t.join();

  // check the heartbeat stops
  std::cout << "thread alive: " << !heartbeat.expired() << std::endl;

  return 0;
}
```

This snippet returns the following result (the ID changes each time it is run).

```txt
id: 5
thread alive: 1
thread alive: 0
```

## class EpochManager

This class supports epoch management for multithreaded software.

A class object has a unique global epoch, and an internal thread advances it at a fixed interval given to the constructor. You can also give a callback function to compute a new global epoch. Worker threads can obtain the current epoch using the `GetCurrentEpoch` function.

The `CreateEpochGuard` function provides epoch protection for each thread with the scoped locking pattern. Worker threads can obtain the oldest protected epoch with the `GetMinEpoch` function. Note that each thread must not nest epoch guards of the same manager. Since an epoch manager allocates thread-local fields based on `IDManager::GetMaxThreadNum()` at its construction, set the maximum number of threads before constructing epoch managers; otherwise, `CreateEpochGuard` may throw `std::out_of_range`.
