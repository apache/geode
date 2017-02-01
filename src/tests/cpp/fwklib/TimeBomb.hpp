#pragma once

#ifndef APACHE_GEODE_GUARD_79d494cc4b256bbd77fabec6e825390a
#define APACHE_GEODE_GUARD_79d494cc4b256bbd77fabec6e825390a

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
#include "fwklib/FwkLog.hpp"

#include <ace/Time_Value.h>
#include "ace/Task.h"
#include <ace/OS.h>

namespace apache {
namespace geode {
namespace client {
namespace testframework {

class TimeBomb : public ACE_Task_Base {
 private:
  bool m_stop;
  bool m_armed;
  ACE_Time_Value m_endOfLife;
  int32_t m_seconds;
  int32_t m_exitCode;
  std::string m_msg;

  int32_t svc();

 public:
  inline TimeBomb(uint32_t seconds, int32_t exitCode, const std::string& msg)
      : m_stop(false), m_armed(false) {
    arm(seconds, exitCode, msg);
    activate();
  }

  inline TimeBomb()
      : m_stop(false), m_armed(false), m_seconds(0), m_exitCode(-1) {}

  inline ~TimeBomb() {
    m_armed = false;
    m_stop = true;
    wait();
  }

  inline void arm(uint32_t seconds, int32_t exitCode, const std::string& msg) {
    //      FWKINFO( "Timebomb set for " << seconds << " seconds." );
    m_seconds = seconds;
    m_exitCode = exitCode;
    m_msg = msg;
    m_endOfLife = ACE_OS::gettimeofday() + ACE_Time_Value(m_seconds);
    m_armed = true;
    char tbuf[64];
    const time_t tsec = static_cast<const time_t>(m_endOfLife.sec());
    ACE_OS::ctime_r(&tsec, tbuf, 64);
    for (int32_t len = static_cast<int32_t>(strlen(tbuf)); len >= 0; len--) {
      if ((tbuf[len] == '\n') || (tbuf[len] == '\r')) {
        tbuf[len] = ' ';
      }
    }
    FWKINFO("Timebomb set for: " << tbuf);
  }
  inline void disarm() { m_armed = false; }
};

}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // APACHE_GEODE_GUARD_79d494cc4b256bbd77fabec6e825390a
