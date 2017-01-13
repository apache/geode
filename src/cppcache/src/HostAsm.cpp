/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "HostAsm.hpp"
#include <ace/Atomic_Op_T.h>
#include <ace/Recursive_Thread_Mutex.h>
#ifdef __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif

using namespace gemfire;

int32_t HostAsm::m_SpinCount = 0;

// TODO refactor - why do we have our own atomic methods? why not use C++11?

#if defined(_LINUX) || defined(_X86_SOLARIS)

typedef long LONG;

inline uint32_t InterlockedCompareExchange(volatile LONG* dest, uint32_t exch,
                                           uint32_t comp) {
#if defined(_LINUX)
  uint32_t old;

  __asm__ __volatile__("lock; cmpxchgl %2, %0"
                       : "=m"(*dest), "=a"(old)
                       : "r"(exch), "m"(*dest), "a"(comp));
  return (old);
#endif
#if defined(_X86_SOLARIS)
  uint32_t old;
  old = atomic_cas_32((volatile uint32_t*)dest, comp, exch);
  return (old);
#endif
}

#endif

#ifdef _SPARC_SOLARIS
typedef long LONG;
extern "C" {
int32_t InterlockedCompareExchange(volatile LONG*, int32_t, int32_t);
}
#endif

void HostAsm::atomicAnd(volatile uint32_t& ctr, uint32_t mask) {
#if defined(_MACOSX)
  OSAtomicAnd32Barrier(mask, &ctr);
#else
  bool success = false;
  while (!success) {
    uint32_t oldValue = ctr;
    uint32_t newValue = oldValue & mask;
    volatile LONG* signedctr = (volatile LONG*)&ctr;
    if (InterlockedCompareExchange(signedctr, newValue, oldValue) == oldValue) {
      return;
    }
  }
#endif
}
// if return value is same as valuetoCompare that means you succeed, otherwise
// some other thread change this value
uint32_t HostAsm::atomicCompareAndExchange(volatile uint32_t& oldValue,
                                           uint32_t newValue,
                                           uint32_t valueToCompare) {
#if defined(_MACOSX)
  if (OSAtomicCompareAndSwap32Barrier(
          valueToCompare, newValue,
          reinterpret_cast<volatile int32_t*>(&oldValue))) {
    return valueToCompare;
  } else {
    return oldValue;
  }
#else
  volatile LONG* signedctr = (volatile LONG*)&oldValue;
  uint32_t retVal =
      InterlockedCompareExchange(signedctr, newValue, valueToCompare);

  return retVal;
#endif
}

void HostAsm::atomicOr(volatile uint32_t& ctr, uint32_t mask) {
#if defined(_MACOSX)
  OSAtomicOr32Barrier(mask, &ctr);
#else
  bool success = false;
  while (!success) {
    uint32_t oldValue = ctr;
    uint32_t newValue = oldValue | mask;
    volatile LONG* signedctr = (volatile LONG*)&ctr;
    if (InterlockedCompareExchange(signedctr, newValue, oldValue) == oldValue) {
      return;
    }
  }
#endif
}

void HostAsm::atomicSet(volatile uint32_t& data, uint32_t newValue) {
// TODO MACOSX this entire function makes no sense unless it is just to force
// memory barriers
#if defined(_MACOSX)
  uint32_t oldValue;
  do {
    oldValue = data;
  } while (!OSAtomicCompareAndSwap32Barrier(
      oldValue, newValue, reinterpret_cast<volatile int32_t*>(&data)));

#else
  bool success = false;
  while (!success) {
    uint32_t oldValue = data;
    volatile LONG* longData = (volatile LONG*)&data;
    if (InterlockedCompareExchange(longData, newValue, oldValue) == oldValue) {
      return;
    }
  }
#endif
}
#ifdef __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__
#pragma clang diagnostic pop
#endif
