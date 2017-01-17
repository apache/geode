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
