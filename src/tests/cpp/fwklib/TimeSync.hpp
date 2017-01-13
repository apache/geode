/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __TimeSync_hpp__
#define __TimeSync_hpp__

#include "fwklib/PerfFwk.hpp"

#include <ace/Time_Value.h>
#include "ace/OS.h"
#include "ace/Task.h"

namespace gemfire {
namespace testframework {

// Some useful conversion constants
#define NANOS_PER_MICRO 1000
#define MICROS_PER_MILLI 1000
#define NANOS_PER_MILLI NANOS_PER_MICRO* MICROS_PER_MILLI

#define MILLIS_PER_SECOND 1000
#define MICROS_PER_SECOND MICROS_PER_MILLI* MILLIS_PER_SECOND
#define NANOS_PER_SECOND NANOS_PER_MICRO* MICROS_PER_SECOND

#define TIME_SYNC_PAUSE_SECONDS 1

class TimeSync : public ACE_Task_Base {
 private:
  int32_t* m_delta;
  bool volatile m_done;
  int32_t m_port;
  bool m_report;
  bool m_logged;

  int32_t svc();

  void sendTimeSync();

  void recvTimeSync();

 public:
  inline TimeSync(int32_t port, int32_t* delta = NULL, bool report = false)
      : m_delta(delta),
        m_done(false),
        m_port(port),
        m_report(report),
        m_logged(false) {
    activate();
  }

  inline void stop() {
    m_done = true;
    sendTimeSync();
    perf::sleepSeconds(1 + TIME_SYNC_PAUSE_SECONDS);
    wait();
  }

  inline int64_t adjustedNowMicros() {
    int64_t retVal;
    ACE_Time_Value tv = ACE_OS::gettimeofday();
    retVal = timevalMicros(tv);
    if (m_delta != NULL) {
      retVal += *m_delta;
    }
    return retVal;
  }

  inline static int64_t timevalMicros(const ACE_Time_Value& tv) {
    return ((tv.sec() * (int64_t)MICROS_PER_SECOND) + tv.usec());
  }
};

}  // testframework
}  // gemfire
#endif  // __TimeSync_hpp__
