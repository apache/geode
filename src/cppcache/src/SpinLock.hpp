/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_UTIL_IMPL_SPINLOCK_HPP__
#define __GEMFIRE_UTIL_IMPL_SPINLOCK_HPP__

#include "HostAsm.hpp"

#ifdef DEBUG
#define GF_SAFELOCK 1
#else
#define GF_SAFELOCK 0
#endif

namespace gemfire {

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
}

#endif
