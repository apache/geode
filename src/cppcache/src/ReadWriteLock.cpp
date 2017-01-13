/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *
 * The specification of function behaviors is found in the corresponding .cpp
 *file.
 *
 *========================================================================
 */

#include "ReadWriteLock.hpp"
#include <ace/Guard_T.h>

using namespace gemfire;

TimedTryWriteGuard::TimedTryWriteGuard(ACE_RW_Thread_Mutex& lock, uint32_t usec)
    : lock_(lock), isAcquired_(false), mutex_(), cond_(mutex_) {
  int cnt = 10;
  uint32_t timeSlice = usec / cnt;
  do {
    if (lock_.tryacquire_write() != -1) {
      isAcquired_ = true;
      break;
    }
    ACE_Time_Value tv = ACE_OS::gettimeofday();
    ACE_Time_Value offset(0, timeSlice);
    tv += offset;
    ACE_Time_Value stopAt(tv);
    ACE_OS::thr_yield();
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(cond_.mutex());
    cond_.waitUntil(&stopAt);
  } while (cnt-- > 0);
}

bool TimedTryWriteGuard::tryAcquireLock(uint32_t usec) {
  int cnt = 10;
  uint32_t timeSlice = usec / cnt;
  do {
    if (lock_.tryacquire_write() != -1) {
      isAcquired_ = true;
      break;
    }
    ACE_Time_Value tv = ACE_OS::gettimeofday();
    ACE_Time_Value offset(0, timeSlice);
    tv += offset;
    ACE_Time_Value stopAt(tv);
    ACE_OS::thr_yield();
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(cond_.mutex());
    cond_.waitUntil(&stopAt);
  } while (cnt-- > 0);
  return isAcquired();
}

TryReadGuard::TryReadGuard(ACE_RW_Thread_Mutex& lock,
                           const volatile bool& exitCondition)
    : lock_(lock), isAcquired_(false) {
  do {
    if (lock_.tryacquire_read() != -1) {
      isAcquired_ = true;
      break;
    }
    ACE_OS::thr_yield();
  } while (!exitCondition);
}

TryWriteGuard::TryWriteGuard(ACE_RW_Thread_Mutex& lock,
                             const volatile bool& exitCondition)
    : lock_(lock), isAcquired_(false) {
  do {
    if (lock_.tryacquire_write() != -1) {
      isAcquired_ = true;
      break;
    }
    ACE_OS::thr_yield();
  } while (!exitCondition);
}
