#pragma once

#ifndef APACHE_GEODE_GUARD_b6cf051f15f6d158175a22b44bbe4b98
#define APACHE_GEODE_GUARD_b6cf051f15f6d158175a22b44bbe4b98

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


#include "fwklib/PerfFwk.hpp"

#include <ace/Time_Value.h>
#include "ace/OS.h"
#include "ace/Task.h"

namespace apache {
namespace geode {
namespace client {
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
    return ((tv.sec() * static_cast<int64_t>(MICROS_PER_SECOND)) + tv.usec());
  }
};

}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // APACHE_GEODE_GUARD_b6cf051f15f6d158175a22b44bbe4b98
