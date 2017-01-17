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
