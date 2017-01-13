/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __TimeBomb_hpp__
#define __TimeBomb_hpp__

#include "fwklib/PerfFwk.hpp"
#include "fwklib/FwkLog.hpp"

#include <ace/Time_Value.h>
#include "ace/Task.h"
#include <ace/OS.h>

namespace gemfire {
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
    const time_t tsec = (const time_t)m_endOfLife.sec();
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

}  // testframework
}  // gemfire
#endif  // __TimeBomb_hpp__
