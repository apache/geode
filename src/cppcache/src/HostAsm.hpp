#ifndef __GEMFIRE_UTIL_IMPL_HOSTASM_HPP__
#define __GEMFIRE_UTIL_IMPL_HOSTASM_HPP__
#ifdef __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/*

This file wraps the assembly spinlock routines, and related atomic update
routines in the class gemfire::util::util_impl::Host. Some ace is included.

*/

#include "config.h"
#include <gfcpp/gfcpp_globals.hpp>
#include <ace/ACE.h>
#include <ace/Time_Value.h>
#include <ace/OS_NS_time.h>
#include <ace/OS_NS_sys_time.h>
#include <ace/Thread.h>
#ifdef _X86_SOLARIS
#include <ace/Thread.h>
#include <sys/atomic.h>
#endif
#if defined(_MACOSX)
#include <libkern/OSAtomic.h>
#endif
/**
 * @file
 */

namespace gemfire {

typedef volatile uint32_t SpinLockField;

/* These should be defined the same as their HOST_LOCK counterparts. */
#if defined(FLG_SOLARIS_UNIX)

// StaticSpinLock assumes that SpinLock values are initialized to zero!

/* no longer using the ldstub instruction, now using cas */
/*  Use the high order 4 bytes of the word as the lock */
enum { SPINLOCK_SET_INT = 0xf0000000 };
enum { SPINLOCK_SET_BYTE = 0xf0 };
enum { SPINLOCK_CLEAR_INT = 0 };
enum { SPINLOCK_CLEAR_BYTE = 0 };

#else

// StaticSpinLock assumes that SpinLock values are initialized to zero!

/* default Windows and x86 Linux */
enum { SPINLOCK_SET_INT = 1 };
enum { SPINLOCK_SET_BYTE = 1 };
enum { SPINLOCK_CLEAR_INT = 0 };
enum { SPINLOCK_CLEAR_BYTE = 0 };

#endif

#ifdef _SPARC_SOLARIS
// implemented in hostsolaris.asm
extern "C" {
void HostAsmUnlock(int32_t, SpinLockField*);
bool HostAsmTryLock(SpinLockField*, int32, uint32);
int32_t InterlockedExchangeAdd(volatile int32_t*, int32_t);
// int64_t InterlockedExchangeAddLong(volatile int64_t *, int64_t);
}
#endif
/*
#ifdef _X86_SOLARIS
  extern "C" {
typedef long LONG;
  int32_t InterlockedExchangeAdd(volatile int32_t *, int32_t);
int32_t InterlockedCompareExchange(volatile LONG*, int32_t, int32_t);
}
#endif*/
/**
 *  hold static wrappers for spinlocks and atomic updates..
 */
class CPPCACHE_EXPORT HostAsm {
 public:
  enum HostSleepConsts {
    SPIN_MIN_SLEEP = 250,
    SPIN_SLEEP_LIMIT = 5000,
    SPIN_COUNT = 3000
  };

  static void spinLockInit(SpinLockField& lockField) {
    // StaticSpinLock assumes that SpinLock values are initialized to zero!
    lockField = SPINLOCK_CLEAR_INT;
  }

  /**
   *  Get exclusive access to the lock, return when the lock is granted.
   */
  inline static void spinLockAcquire(SpinLockField& lockField) {
#if defined(_MACOSX)
    OSSpinLockLock((volatile int32_t*)&lockField);
#else
    uint32_t lockVal = SPINLOCK_SET_INT;
    int32_t spinCount = HostAsm::getSpinCount();
    if (!HostAsm::_tryLock(lockField, spinCount, lockVal)) {
      ACE_OS::thr_yield();
      if (!HostAsm::_tryLock(lockField, spinCount, lockVal)) {
        uint32_t lock_sleepTime = SPIN_MIN_SLEEP;
        do {
          HostAsm::nanoSleep(lock_sleepTime);
          if (lock_sleepTime < SPIN_SLEEP_LIMIT) {
            lock_sleepTime++;
          }
        } while (!HostAsm::_tryLock(lockField, spinCount, lockVal));
      }
    }
#endif
  }

  inline static int32_t getCpuCount() {
#ifdef _WIN32
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return si.dwNumberOfProcessors;
#else
    return (int32_t)sysconf(_SC_NPROCESSORS_ONLN);
#endif
  }

  inline static void nanoSleep(uint32_t time) {
    timespec nanos;
    nanos.tv_sec = 0;
    nanos.tv_nsec = time;
    timespec remaining;
    remaining.tv_sec = 0;
    remaining.tv_nsec = 0;
    ACE_OS::nanosleep(&nanos, &remaining);
  }

  inline static int64_t currentTimeMs() {
    return ACE_OS::gettimeofday().msec();
  }

#if 0  // Unused
    /**
     * Try to get exclusive access to the lock in the specified amount of time.
     * Return whether the lock is granted.
     */
    inline static bool spinLockTryAcquire(SpinLockField& lockField,
        int32_t msTimeout)
    {
#if defined(_MACOSX)
      return OSSpinLockTry((volatile int32_t *) &lockField);
#else
      uint32_t lockVal = SPINLOCK_SET_INT;
      int32_t spinCount = HostAsm::getSpinCount();
      int64_t startTime = HostAsm::currentTimeMs();
      if (!HostAsm::_tryLock(lockField, spinCount, lockVal)) {
	      ACE_OS::thr_yield();
	      if (!HostAsm::_tryLock(lockField, spinCount, lockVal)) {
	        uint32_t lock_sleepTime = SPIN_MIN_SLEEP;
	        do {
	          HostAsm::nanoSleep(lock_sleepTime);
            if (lock_sleepTime < SPIN_SLEEP_LIMIT) {
              lock_sleepTime++;
            }
            if (HostAsm::currentTimeMs() - startTime > (int64_t)msTimeout) {
              return false;
            }
          } while (!HostAsm::_tryLock(lockField, spinCount, lockVal));
        }
      }
      return true;
#endif
    }
#endif

  /**
   * Name - SpinUnlock
   * Purpose -
   *      Release the specified spinlock.
   */
  inline static void spinLockRelease(SpinLockField& lockField) {
#ifdef _WIN32
    InterlockedExchange((volatile LONG*)(&lockField), SPINLOCK_CLEAR_BYTE);
#elif defined(_LINUX) /*|| defined(_X86_SOLARIS)*/

    int oldval = SPINLOCK_CLEAR_BYTE;
    __asm__ __volatile__("xchg %0, %1"
                         : "=q"(oldval), "=m"(lockField)
                         : "0"(oldval)
                         : "memory");
#elif defined(_SPARC_SOLARIS)
    SpinLockField* lockPtr = &lockField;
    HostAsmUnlock(SPINLOCK_CLEAR_BYTE, lockPtr);
#elif defined(_X86_SOLARIS)
    // atomic_cas_32((volatile uin32_t*)&lockField, 1, 0);
    atomic_cas_32(&lockField, 1, 0);
#elif defined(_MACOSX)
    OSSpinLockUnlock((volatile int32_t*)&lockField);
#else
#error Port incomplete.
#endif
  }

  static int32_t m_SpinCount;

  inline static int32_t getSpinCount() {
    if (HostAsm::m_SpinCount == 0) {
      HostAsm::m_SpinCount =
          (HostAsm::getCpuCount() == 1 ? 1 : int32_t(SPIN_COUNT));
    }
    return HostAsm::m_SpinCount;
  }

#if defined(_LINUX) || defined(_X86_SOLARIS)
  inline static int32_t InterlockedExchangeAdd(volatile int32_t* val,
                                               int32_t add) {
#if defined(_LINUX)
    int32_t ret;
    __asm__ __volatile__("lock; xaddl %0, %1"
                         : "=r"(ret), "=m"(*val)
                         : "0"(add), "m"(*val));

    return (ret);
#endif
#if defined(_X86_SOLARIS)
    int32_t ret = *val;
    atomic_add_32((volatile uint32_t*)val, add);
    return ret + add;
#endif
  }
// _SOLARIS case is handled in hostsolaris.asm
#endif

  // _SOLARIS case is handled in hostsolaris.asm

  /**
   * Name - atomicAdd
   * Purpose -
   *   Add 'increment' to  the counter pointed to be 'ctrPtr'.
   *   Returns the value of the counter after the addition
   */
  inline static int32_t atomicAdd(volatile int32_t& counter, int32_t delta) {
#ifdef _WIN32
    return InterlockedExchangeAdd((volatile LONG*)(&counter), delta) + delta;
#endif

#ifdef _X86_SOLARIS
    int32_t ret = counter;
    atomic_add_32((volatile uint32_t*)&counter, delta);
    return ret + delta;
#endif

#if defined(_LINUX) || defined(_SPARC_SOLARIS)
    return InterlockedExchangeAdd(&counter, delta) + delta;
#endif

#if defined(_MACOSX)
    return OSAtomicAdd32Barrier(delta, &counter);
#endif
  }

  /**
   * Name - atomicAddPostfix
   * Purpose -
   *   Add 'increment' to  the counter pointed to be 'ctrPtr'.
   *   Returns the value of the counter before the addition
   */
  inline static int32_t atomicAddPostfix(volatile int32_t& counter,
                                         int32_t delta) {
#if defined(_WIN32)
    return InterlockedExchangeAdd((volatile LONG*)(&counter), delta);
#elif defined(_X86_SOLARIS)
    int32_t ret = counter;
    atomic_add_32((volatile uint32_t*)(&counter), delta);
    return ret;
#elif defined(_LINUX) || defined(_SPARC_SOLARIS)
    return InterlockedExchangeAdd(&counter, delta);
#elif defined(_MACOSX)
    int32_t ret = counter;
    OSAtomicAdd32Barrier(delta, &counter);
    return ret;
#else
#error Port incomplete
#endif
  }

  /**
   * Name - AtomicAnd
   *   Atomically AND the mask value into the given address
   */
  static void atomicAnd(volatile uint32_t& ctrField, uint32_t mask);

  static uint32_t atomicCompareAndExchange(volatile uint32_t& oldValue,
                                           uint32_t newValue,
                                           uint32_t valueToCompare);

  /**
   * Name - AtomicOr
   *  Atomically OR the mask value into the given address
   */
  static void atomicOr(volatile uint32_t& ctrField, uint32_t mask);

  /**
   * Atomically set masked bits to 1 in data.
   */
  inline static void atomicSetBits(volatile uint32_t& data, uint32_t mask) {
    return atomicOr(data, mask);
  }

  /**
   * Atomically set value of data to the given value.
   */
  static void atomicSet(volatile uint32_t& data, uint32_t newValue);

  /**
   * Atomically set masked bits to 0 in data.
   */
  inline static void atomicClearBits(volatile uint32_t& data, uint32_t mask) {
    return atomicAnd(data, ~mask);
  }

 private:
#if !defined(_MACOSX)
  inline static bool _tryLock(SpinLockField& lockField, int32_t count,
                              uint32_t lockVal) {
    GF_DEV_ASSERT(count > 0);
#if defined(_LINUX)
    int oldval = 1;  // Set to 1, since we use 0 as success.
    do {
      oldval = 1;
      __asm__ __volatile__(
          "lock\n"
          "xchg %0,%1"
          : "=q"(oldval), "=m"(lockField)
          : "0"(SPINLOCK_SET_BYTE)
          : "memory");
      if (oldval == 0) {
        return true;
      }
      __asm__ __volatile__("pause");
      count--;
    } while (count > 0);
    if (oldval == 0) {
      return true;
    }
    return false;
#elif defined(_SPARC_SOLARIS)
    SpinLockField* lockPtr = &lockField;
    return HostAsmTryLock(lockPtr, count, lockVal);
#elif defined(_X86_SOLARIS)
    SpinLockField* lockPtr = &lockField;
    do {
      if (*lockPtr == SPINLOCK_CLEAR_INT) {
        // if oldValue is zero, then it must have been updated to 1
        // else if CAS was unsuccessful then it will still be locked i.e. 1
        if (atomic_cas_32(lockPtr, 0, 1) == 0) {
          return true;
        }
      }
      // yield the thread if required to avoid tight spin
      ACE_Thread::yield();
      count--;
    } while (count > 0);
    return false;
#elif defined(_WIN32)
    SpinLockField* lockPtr = &lockField;
    SpinLockField prevValue;
    // SpinLockField prevCopy;
    prevValue = *lockPtr;
    do {
      if (prevValue == SPINLOCK_CLEAR_INT) {
        if (InterlockedCompareExchangeAcquire((volatile LONG*)lockPtr, lockVal,
                                              SPINLOCK_CLEAR_INT) ==
            SPINLOCK_CLEAR_INT) {
          return true;
        }
      } else {
#if defined(_MANAGED)
        Sleep(0);
#else
        YieldProcessor();
        YieldProcessor();
        YieldProcessor();
        YieldProcessor();
        YieldProcessor();
#endif
      }
      // Fancy atomic read, equivalent to prevValue = *lockPtr
      prevValue = InterlockedExchangeAdd((volatile LONG*)(lockPtr), 0);
    } while (--count >= 0);
    return false;
/*
if(count--) {
  return false;
}
prevCopy = prevValue;
if(prevValue == SPINLOCK_CLEAR_INT) {
  prevValue = InterlockedCompareExchangeAcquire(
    (volatile LONG*)lockPtr, lockVal, prevValue);
} else {
  //Fancy atomic read, equivalent to prevValue = *lockPtr
  prevValue = InterlockedExchangeAdd((volatile LONG*)(lockPtr), 0);

  //This might be slightly faster
  //prevValue = InterlockedCompareExchange((volatile LONG*)lockPtr,
  //                                       prevValue, prevValue);
}
} while( prevCopy != prevValue );
return true;
*/
#else
#error Port impcomplete
#endif
    return true;
  }
#endif  // !defined(_MACOSX)
};
}

#ifdef __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__
#pragma clang diagnostic pop
#endif
#endif
