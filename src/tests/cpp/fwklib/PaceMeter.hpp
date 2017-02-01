#pragma once

#ifndef APACHE_GEODE_GUARD_0b9b6fa2558c2b1a9cd9f073630e90aa
#define APACHE_GEODE_GUARD_0b9b6fa2558c2b1a9cd9f073630e90aa

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


#include "fwklib/FwkLog.hpp"
#include <ace/Time_Value.h>
#include <ace/OS.h>

#ifdef WIN32
#include <windows.h>
#else
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace apache {
namespace geode {
namespace client {
namespace testframework {

class Waiter {
 public:
  Waiter() {}
  void waitUntil(ACE_Time_Value* until) {
    ACE_Time_Value now = ACE_OS::gettimeofday();
    if (*until < now) {
      return;
    }
    ACE_Time_Value interval = *until - now;
#ifdef WIN32
    WaitForSingleObject(GetCurrentThread(), interval.msec());
#else
    struct timeval tv;
    tv.tv_sec = interval.sec();
    tv.tv_usec = interval.usec();
    select(1, NULL, NULL, NULL, &tv);
#endif
  }
};

class PaceMeter {
 private:
  int32_t m_opsLimit;
  int32_t m_current;
  int32_t m_waitSeconds;
  int32_t m_waitMicros;
  ACE_Time_Value m_timeLimit;

 public:
  PaceMeter(int32_t ops, int32_t seconds = 1)
      : m_opsLimit(0),
        m_current(0),
        m_waitSeconds(0),
        m_waitMicros(0),
        m_timeLimit(0) {
    if ((ops <= 0) || (seconds <= 0)) {
      return;
    }

    double os = static_cast<double>(ops) / static_cast<double>(seconds);

    if ((ops / seconds) < 1) {  // This is for the "seconds per op" case
      double dsec = 1 / os;
      m_opsLimit = 1;
      m_waitSeconds = static_cast<int32_t>(dsec);
      dsec -= m_waitSeconds;
      m_waitMicros = static_cast<int32_t>(dsec * 1000000);
    } else {
      int32_t ios = ops / seconds;
      m_waitSeconds = 0;

      if (ios < 4) {
        m_opsLimit = ios;
        m_waitSeconds = 1;
        m_waitMicros = 0;
      } else {
        if ((ios % 3) >= (ios % 4)) {
          m_opsLimit = ios / 4;
          m_waitMicros = 250000;
        } else {
          m_opsLimit = ios / 3;
          m_waitMicros = 333333;
        }
      }
    }
    //    FWKINFO( "Args: " << ops << ", " << seconds << "     Ops: " <<
    //    m_opsLimit << "  Seconds: " << m_waitSeconds << "   Micros: " <<
    //    m_waitMicros );
  }

  void checkPace() {
    if (m_opsLimit == 0) {
      return;
    }
    if (m_current == 0) {
      //      FWKINFO( "Setting time limit to Seconds: " << m_waitSeconds << "
      //      Micros: " << m_waitMicros );
      m_timeLimit =
          ACE_OS::gettimeofday() + ACE_Time_Value(m_waitSeconds, m_waitMicros);
    }
    if (++m_current >= m_opsLimit) {
      //      FWKINFO( "m_current is: " << m_current << "  waiting." );
      m_current = 0;
      Waiter waiter;
      waiter.waitUntil(&m_timeLimit);
    }
  }
};

}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_0b9b6fa2558c2b1a9cd9f073630e90aa
