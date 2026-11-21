/*
 * Copyright 2024 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// the corresponding header
#include "dbgroup/lock/mcs_lock.hpp"

// C++ standard libraries
#include <atomic>
#include <bit>
#include <cassert>
#include <cstdint>
#include <thread>

// local sources
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/utility.hpp"

namespace dbgroup::lock
{
namespace
{
/*############################################################################*
 * Static variables
 *############################################################################*/

/// @brief A thread local queue node container.
thread_local std::unique_ptr<MCSLock> _tls{};  // NOLINT

}  // namespace

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
MCSLock::LockS()  //
    -> SGuard
{
  auto* qnode = _tls ? _tls.release() : new MCSLock{};
  qnode->LockWord().store(kNull, kRelaxed);
  auto ptr = std::bit_cast<uint64_t>(qnode) | kSLock;

  auto cur = LockWord().load(kRelaxed);
  while (true) {
    if (cur) {  // there is the predecessor
      if (LockWord().compare_exchange_weak(cur, cur + kSLock, kAcquire, kRelaxed)) break;
    } else if (LockWord().compare_exchange_weak(cur, ptr, kAcqRel, kRelaxed)) {
      goto end;  // the initial shared lock
    }
    CPP_UTILITY_SPINLOCK_HINT
  }

  _tls.reset(qnode);
  ptr = cur & kPtrMask;
  qnode = std::bit_cast<MCSLock*>(ptr);
  if (cur & kXLock) {  // wait for the predecessor to release the lock
    while ((cur & kPtrMask) == ptr && (cur & kXLock)) {
      std::this_thread::yield();
      cur = LockWord().load(kAcquire);
    }
    if ((cur & kPtrMask) != ptr) {
      while (true) {  // wait for the successor to fill its pointer
        ptr = qnode->LockWord().load(kAcquire) & kPtrMask;
        if (ptr) break;
        CPP_UTILITY_SPINLOCK_HINT
      }
      auto* const next = std::bit_cast<MCSLock*>(ptr);
      while (next->LockWord().load(kAcquire) & kXLock) {
        std::this_thread::yield();
      }
    }
  }
end:
  return SGuard{this, qnode};
}

auto
MCSLock::LockSIX()  //
    -> SIXGuard
{
  auto* const qnode = _tls ? _tls.release() : new MCSLock{};
  const auto new_tail = std::bit_cast<uint64_t>(qnode) | kXLock;

  auto cur = LockWord().load(kRelaxed);
  while (true) {
    qnode->LockWord().store((cur & kLockMask) | kOwned, kRelaxed);
    if (LockWord().compare_exchange_weak(cur, new_tail, kAcqRel, kRelaxed)) break;
    CPP_UTILITY_SPINLOCK_HINT
  }

  auto* const tail = std::bit_cast<MCSLock*>(cur & kPtrMask);
  if (tail) {  // wait for the predecessor to release the lock
    tail->LockWord().fetch_add(new_tail & kPtrMask, kRelease);
    while (qnode->LockWord().load(kAcquire) & kXLock) {
      std::this_thread::yield();
    }
  }
  return SIXGuard{this, qnode};
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

void
MCSLock::UnlockS(  //
    MCSLock* const qnode)
{
  const auto this_ptr = std::bit_cast<uint64_t>(qnode);
  auto next_ptr = qnode->LockWord().load(kAcquire) & kPtrMask;
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = LockWord().load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      const auto unlock = cur - kSLock;
      if (unlock & kLockMask) {
        if (LockWord().compare_exchange_weak(cur, unlock, kRelease, kRelaxed)) return;
      } else if (LockWord().compare_exchange_weak(cur, kNull, kAcqRel, kRelaxed)) {
        _tls.reset(qnode);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait for the successor to fill its pointer
      next_ptr = qnode->LockWord().load(kAcquire) & kPtrMask;
      if (next_ptr) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  auto* const next = std::bit_cast<MCSLock*>(next_ptr);
  const auto old = next->LockWord().fetch_sub(kSLock, kAcqRel);
  if ((old & kWaitMask) == kSLock) {
    _tls.reset(qnode);
  }
  if ((old & (kOwned | kWaitMask)) == kSLock) {  // the successor's owner has released the lock
    PassLock(next);
  }
}

void
MCSLock::PassLock(  //
    MCSLock* const qnode)
{
  const auto this_ptr = std::bit_cast<uint64_t>(qnode);
  auto next_ptr = qnode->LockWord().load(kAcquire) & kPtrMask;
  if (next_ptr == kNull) {  // this is the tail node
    auto cur = LockWord().load(kRelaxed);
    while ((cur & kPtrMask) == this_ptr) {
      if (cur & kSMask) {  // the shared lock holders take over this node
        if (LockWord().compare_exchange_weak(cur, cur ^ kXLock, kRelease, kRelaxed)) return;
      } else if (LockWord().compare_exchange_weak(cur, kNull, kRelease, kRelaxed)) {
        _tls.reset(qnode);
        return;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }

    while (true) {  // wait until successor fills in its next field
      next_ptr = qnode->LockWord().load(kAcquire) & kPtrMask;
      if (next_ptr) break;
      CPP_UTILITY_SPINLOCK_HINT
    }
  }

  auto* const next = std::bit_cast<MCSLock*>(next_ptr);
  if ((next->LockWord().fetch_xor(kXLock, kRelease) & kWaitMask) == kXLock) {
    _tls.reset(qnode);
  }
}

}  // namespace dbgroup::lock
