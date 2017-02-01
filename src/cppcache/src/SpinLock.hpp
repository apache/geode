#pragma once

#ifndef GEODE_SPINLOCK_H_
#define GEODE_SPINLOCK_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "HostAsm.hpp"

#ifdef DEBUG
#define GF_SAFELOCK 1
#else
#define GF_SAFELOCK 0
#endif

namespace apache {
namespace geode {
namespace client {

/**
 * For an object that needs to be protected by spinlock, declare a field of type
 * SpinLock. To protect the operation, use a SpinLockGuard on the stack to
 * automatically lock and then release when the stack is unwound..
 */
class CPPCACHE_EXPORT SpinLock {
 public:
  SpinLock()
      : m_lockField(0)
#if GF_SAFELOCK
        ,
        m_ownerId(0)
#endif
  {
    HostAsm::spinLockInit(m_lockField);
  }

  ~SpinLock() {}

  void acquire() {
#if GF_SAFELOCK
    int32_t ownerId = (int32_t)ACE_OS::thr_self();
    GF_R_ASSERT(
        (ownerId == 0) ||
        (ownerId !=
         m_ownerId));  // detect attempt to lock something I already have.
#endif
    HostAsm::spinLockAcquire(m_lockField);
#if GF_SAFELOCK
    m_ownerId = ownerId;
#endif
  }

  void release() {
#if GF_SAFELOCK
    m_ownerId = 0;
#endif
    HostAsm::spinLockRelease(m_lockField);
  }

 private:
  SpinLockField m_lockField;
#if GF_SAFELOCK
  int32_t m_ownerId;
#endif
};

/**
 * Example:
 *  class Foo {
 *    private:
 *
 *    SpinLock m_lock;
 *
 *    public:
 *
 *    Bool doSomething( )
 *    { SpinLockGuard __guard( m_lock );
 *      if ( ?? ) {
 *        return false;
 *      } else {
 *        if ( ?? ) throw ??
 *        return true;
 *      }
 *    }
 *  };
 *
 * The lock is automatically released no matter what return path is taken.
 */
class SpinLockGuard {
 public:
  SpinLockGuard(SpinLock& spinlock) : m_lock(spinlock) { m_lock.acquire(); }

  ~SpinLockGuard() { m_lock.release(); }

 private:
  SpinLock& m_lock;
};

// Test function
CPPCACHE_EXPORT void* testSpinLockCreate();
CPPCACHE_EXPORT void testSpinLockAcquire(void* lock);
CPPCACHE_EXPORT void testSpinLockRelease(void* lock);
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_SPINLOCK_H_
