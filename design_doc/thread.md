# `::dbgroup::thread`

## `IDManager`

This class manages thread IDs in a single process and has two APIs.

The `IDManager::GetThreadID` function returns a unique thread ID in [0, `DBGROUP_MAX_THREAD_NUM`). Since the assigned thread ID is maintained in the thread's local storage, this function always returns the same ID for the same thread. Note that the capacity of IDs is static (i.e., `DBGROUP_MAX_THREAD_NUM`), so some threads may be blocked if all IDs are reserved. In this case, the threads must wait for the previous threads to exit.

The `IDManager::GetHeartBeat` function returns a `std::weak_ptr<size_t>` instance to allow you to check the alive monitoring of a particular thread. The `expired` function returns `false` until the corresponding thread exits.

## Usage Example

```cpp
// C++ standard libraries
#include <chrono>
#include <iostream>
#include <mutex>

// our libraries
#include "thread/id_manager.hpp"

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
id: 462
thread alive: 1
thread alive: 0
```
