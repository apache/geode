/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "NanoTimer.hpp"
#include <ace/OS.h>
#include <ace/Time_Value.h>
#include <ace/High_Res_Timer.h>

// @TODO: Make these actually nano granularity when possible.

namespace gemfire {

int64_t NanoTimer::now() {
  ACE_Time_Value now = ACE_High_Res_Timer::gettimeofday_hr();
  return (now.sec() * 1000000000ll) /* convert secs to nanosecs */
         + (now.usec() * 1000ll);   /* convert microsecs to nanosecs */
}

void NanoTimer::sleep(uint32_t nanos) {
  timespec spec;
  spec.tv_sec = 0L;
  spec.tv_nsec = nanos;
  timespec remaining;
  remaining.tv_sec = 0L;
  remaining.tv_nsec = 0L;
  ACE_OS::nanosleep(&spec, &remaining);
}

void millisleep(uint32_t millis) {
  time_t secs = millis / 1000;
  suseconds_t usecs = (millis % 1000) * 1000;
  ACE_Time_Value duration(secs, usecs);
  ACE_OS::sleep(duration);
}
}  // namespace gemfire
