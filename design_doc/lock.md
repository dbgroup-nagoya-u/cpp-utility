# `::dbgroup::lock`

- [Pessimistic Locking](#pessimistic-locking)
    - [class PessimisticLock](#class-pessimisticlock)
    - [class MCSLock](#class-mcslock)
    - [Example of Usages](#example-of-usages)
- [Optimistic Locking](#optimistic-locking)
    - [class OptimisticLock](#class-optimisticlock)
    - [class OMCSLock](#class-omcslock)
    - [Example of Usages](#example-of-usages-1)

## Pessimistic Locking

We have implemented three types of locks: a shared lock (`S`), an exclusive lock (`X`), and a shared-with-intent-exclusive lock (`SIX`). The following table summarizes the compatibility between these locks. The check mark (`x`) indicates that there is no conflict between the corresponding locks.

|       |  `S`  | `SIX` |  `X`  |
| :---: | :---: | :---: | :---: |
|  `S`  |  `x`  |  `x`  |       |
| `SIX` |  `x`  |       |       |
|  `X`  |       |       |       |

In our implementation, we assume the following state transition of locks. Since we apply the scoped locking pattern to our lock classes, each function returns the corresponding lock guard instance (`SGuard`, `SIXGuard`, and `XGuard`). In their destructions, they release the holding locks.

```mermaid
stateDiagram-v2
    [*] --> SGuard : LockS
    [*] --> XGuard : LockX
    [*] --> SIXGuard : LockSIX
    SGuard --> [*] : Destructor
    XGuard --> [*] : Destructor
    XGuard --> SIXGuard : DowngradeToSIX
    XGuard --> SGuard : DowngradeToS
    SIXGuard --> [*] : Destructor
    SIXGuard --> XGuard : UpgradeToX
    SIXGuard --> SGuard : DowngradeToS
```

### class PessimisticLock

We maintain the internal lock state according to the following table. The last and second-to-last bits represent exclusive and shared-with-intent-exclusive locks, respectively. When these bits are set, a thread has acquired either X or SIX locks. The remaining bits maintain the number of threads that have acquired shared locks.

|       63       |        62        |         61-0          |
| :------------: | :--------------: | :-------------------: |
| an X lock flag | an SIX lock flag | a shared lock counter |

This lock uses *back-off* to schedule threads requesting lock acquisition, so getting a lock is unfair. While back-off scheduling will significantly reduce CPU usage, some threads may have to wait a long time for other threads.

### class MCSLock

We have implemented the MCS queue lock [^1] with shared and shared-with-intent-exclusive locks. We maintain the internal lock state according to the following table. The second-to-last bit is an intent-exclusive (IX) lock flag, which represents that the owner of the tail queue node holds (or waits for) an SIX or X lock. Our MCS lock does not distinguish these locks in the lock state because an X lock is an SIX lock that has waited for the preceding shared lock holders. The following 15 bits preserve the number of threads that hold (or wait for) shared locks after the tail queue node. The remaining bits contain the pointer of the tail queue node, and the last bit is unused.

|   63   |       62        |         61-47         |        46-0         |
| :----: | :-------------: | :-------------------: | :-----------------: |
| unused | an IX lock flag | a shared lock counter | a tail node pointer |

Each queue node has a lock state with the same layout except for the last bit. Its IX lock flag represents that the node's owner waits for the predecessor to pass the lock, and its shared lock counter preserves the number of shared lock holders that have entered after the predecessor. The last bit is an owner flag, which represents that the node's owner has not released its lock yet. If the owner of an SIX lock releases (or downgrades) it while the predecessor's shared lock holders remain, the last one of them passes the lock to the successor instead of the owner.

|      63       |       62        |         61-47         |        46-0         |
| :-----------: | :-------------: | :-------------------: | :-----------------: |
| an owner flag | an IX lock flag | a shared lock counter | a next node pointer |

Our MCS lock allows shared and shared-with-intent-exclusive locks, but note that *a queue orders their lock acquisitions*. For example, assume that five threads request locks to our MCS lock in the following order.

1. Thread 1: An exclusive lock.
2. Thread 2: A shared lock.
3. Thread 3: A shared-with-intent-exclusive lock.
4. Thread 4: A shared lock.
5. Thread 5: An exclusive lock.

These requests finally construct the following lock queue. Since Thread 1 holds the exclusive lock, Threads 2 and 3 need to wait for Thread 1. After Thread 1 releases its lock, Threads 2 and 3 can acquire their locks simultaneously because S/SIX locks coexist. Thread 4 also requests a shared lock, but *the queue order refuses this request and forces Thread 4 to wait*. After Threads 2 and 3 release their locks, Thread 4 can obtain the lock, and Thread 5 waits for it to release its lock.

```mermaid
stateDiagram-v2
    direction LR
    QNode1: QNode (1-X)
    QNode2: QNode (2-S/3-SIX)
    QNode3: QNode (4-S/5-X)
    [*] --> QNode3: tail
    QNode1 --> QNode2: next
    QNode2 --> QNode3: next
```

Since this lock uses *spinning* to wait for other threads to release locks, many concurrent lock requests will cause heavy and wasteful CPU usage. Although our implementation calls `std::this_thread::yield` to give other threads a chance to get CPU cores, we advise against creating more threads than logical CPU cores.

### Example of Usages

```cpp
// C++ standard libraries
#include <iostream>
#include <thread>
#include <vector>

// our libraries
#include "dbgroup/lock/pessimistic_lock.hpp"

auto
main(  //
    const int argc,
    const char *argv[])  //
    -> int
{
  // create a global lock and counter
  ::dbgroup::lock::PessimisticLock lock{};
  size_t count{0};

  // create and run workers
  std::vector<std::thread> threads{};
  for (size_t i = 0; i < 8; ++i) {
    threads.emplace_back([&]() {
      // increment the counter with an exclusive lock
      for (size_t loop = 0; loop < 10000; ++loop) {
        const auto &x_guard = lock.LockX();
        ++count;
      }
    });
  }

  // wait for the workers
  for (auto &&t : threads) {
    t.join();
  }

  {  // check the counter
    const auto &s_guard = lock.LockS();
    std::cout << count << std::endl;
  }
  return 0;
}
```

## Optimistic Locking

We use a *version-check* functionality for optimistic locking. You can get a version value with `GetVersion` and check its validity with `VerifyVersion`. Our optimistic lock implementation has the same locks as pessimistic locking, and so you can use shared/exclusive/shared-with-intent-exclusive locks with version-based concurrency controls. We also prepare `TryLock` functions to acquire locks with the version check.

The following state diagram shows how our lock guards and functions work. Note that each function directly modifies the version state in `OptGuard` if the version verification fails. `XGuard`'s destructor and its `DowngradeToSIX` and `DowngradeToS` functions modify the version of a target lock. Our implementation increments a version by one as default, but you can set a desired version value using `XGuard::SetVersion` function.

```mermaid
stateDiagram-v2
    Pessimistic: Into pessimistic locking
    [*] --> OptGuard : GetVersion
    OptGuard --> OptGuard : VerifyVersion, TryLockS, TryLockSIX, TryLockX (fail)
    OptGuard --> SGuard : TryLockS (succeed)
    OptGuard --> SIXGuard : TryLockSIX (succeed)
    OptGuard --> XGuard : TryLockX (succeed)
    SGuard --> Pessimistic
    SIXGuard --> Pessimistic
    XGuard --> Pessimistic
```

If version verification fails repeatedly, `VerifyVersion` with the `max_retry` argument falls back to a shared lock to avoid starving readers. We emphasize that only a single thread acquires a shared lock even if multiple threads fall back; the other threads continue with version-based optimistic reads while that thread holds the shared lock. The next call of `VerifyVersion` releases the shared lock and returns `true`.

### class OptimisticLock

We maintain the internal lock state according to the following table. The last and second-to-last bits represent exclusive and shared-with-intent-exclusive locks, respectively. The following 30 bits preserve the number of threads that have acquired shared locks. The remaining bits contain the current version value.

|       63       |        62        |         61-32         |      31-0       |
| :------------: | :--------------: | :-------------------: | :-------------: |
| an X lock flag | an SIX lock flag | a shared lock counter | a version value |

This lock uses *back-off* to schedule threads requesting lock acquisition, so getting a lock is unfair. While back-off scheduling will significantly reduce CPU usage, some threads may have to wait a long time for other threads.

### class OMCSLock

We have also implemented an optimistic MCS (OMCS) lock, which extends OptiQL [^2] to support shared and shared-with-intent-exclusive locks. Like `MCSLock`, this lock orders lock acquisitions with a queue. As in `OptimisticLock`, optimistic readers wait only while a thread holds an X lock, that is, after `UpgradeToX` has waited for the preceding shared lock holders; SIX locks do not block optimistic reads.

We maintain the internal lock state according to the following table. The last bit represents an X lock, and the second-to-last bit is an IX lock flag, which represents that the owner of the tail queue node holds (or waits for) an SIX or X lock as in `MCSLock`. The following 14 bits preserve the number of threads that hold (or wait for) shared locks after the tail queue node, and the next 16 bits contain the ID of the tail queue node. The remaining bits contain the current version value.

|       63       |       62        |         61-48         |     47-32      |      31-0       |
| :------------: | :-------------: | :-------------------: | :------------: | :-------------: |
| an X lock flag | an IX lock flag | a shared lock counter | a tail node ID | a version value |

Queue nodes are preallocated in a global array and identified by 16-bit IDs, and each thread caches some of them for reuse. Since the lock state embeds a node ID instead of a pointer, it can hold a version value in the same 64-bit word. Each queue node has an owner flag, an IX lock flag, and a shared lock counter as in `MCSLock`, and the lock is passed to the successor in the same manner.

### Example of Usages

#### Optimistic Read Procedure

```cpp
for (auto &&opt_guard = opt_lock.GetVersion();;) {  // keep a version-based guard
  // ...some codes for reading a shared region...

  if (opt_guard.VerifyVersion()) break;
  // the version check failed, so retry

  // you can check the new version and apply additional controls
  const uint32_t ver = opt_guard.GetVersion();
  // ...
}
```

#### Acquiring Locks with Version Verification

```cpp
for (auto &&opt_guard = opt_lock.GetVersion();;) {  // keep a version-based guard
  // ...some codes for reading a shared region...

  if (const auto &x_guard = opt_guard.TryLockX(); x_guard) {
    // the exclusive lock has been acquired
    // ...
    break;
  }
  // the version check failed, so retry
}
```

#### Controlling Internal Version State

```cpp
{
  auto &&x_guard = opt_lock.LockX();

  // ...some codes for modifying a shared region...

  // increment a version by 2
  x_guard.SetVersion(x_guard.GetVersion() + 2);
}
```

[^1]: M. Herlihy et al., “The art of multiprocessor programming,” chapter 7, Morgan Kaufmann, 2nd edition, 2021.
[^2]: G. Shi et al., "OptiQL: Robust Optimistic Locking for Memory-Optimized Indexes," PACMMOD, vol. 1, no. 3, article 216, 2023.
