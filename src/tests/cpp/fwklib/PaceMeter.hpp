/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __PaceMeter_hpp__
#define __PaceMeter_hpp__

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

namespace gemfire {
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

    double os = (double)ops / (double)seconds;

    if ((ops / seconds) < 1) {  // This is for the "seconds per op" case
      double dsec = 1 / os;
      m_opsLimit = 1;
      m_waitSeconds = (int32_t)dsec;
      dsec -= m_waitSeconds;
      m_waitMicros = (int32_t)(dsec * 1000000);
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
}  // namespace gemfire

#endif  // __PaceMeter_hpp__
