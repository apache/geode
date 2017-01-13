/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#include <gfcpp/gfcpp_globals.hpp>
#include "fwklib/TimeBomb.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"

using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::perf;

int32_t TimeBomb::svc() {
  while (!m_stop) {
    if (m_armed) {
      if (m_endOfLife < ACE_OS::gettimeofday()) {
        if (m_msg.length() > 0) {
          FWKSEVERE("Timeout failure has occurred, waited "
                    << m_seconds << " seconds. " << m_msg);
        } else {
          FWKSEVERE("Timeout failure has occurred, waited " << m_seconds
                                                            << " seconds.");
        }

#if defined(_SOLARIS)

        int32_t pid = ACE_OS::getpid();
        char buf[8192];
        sprintf(buf, "bash -c \"pstack %d\"", pid);
        FILE* pip = popen(buf, "r");
        if (pip == NULL) {
          FWKSEVERE("TimeBomb: Unable to dump threads.");
        } else {
          perf::sleepSeconds(10);

          std::string dump;
          buf[0] = 0;
          while (fgets(buf, 8192, pip) != NULL) {
            dump.append(buf);
            buf[0] = 0;
          }
          dump.append(buf);
          pclose(pip);

          FWKSEVERE("TimeBomb reports these threads in process:"
                    << dump << "End of dump.\n");
        }

#endif

#if defined(_LINUX)

        int32_t pid = ACE_OS::getpid();
        char buf[8192];
        sprintf(buf,
                "bash -c \"perl $GFCPP/../framework/scripts/gdb.pl %d ; cat "
                "gdbout.%d\"",
                pid, pid);
        FILE* pip = popen(buf, "r");
        if (pip == NULL) {
          FWKSEVERE("TimeBomb: Unable to dump threads.");
        } else {
          perf::sleepSeconds(10);

          std::string dump;
          buf[0] = 0;
          while (fgets(buf, 8192, pip) != NULL) {
            dump.append(buf);
            buf[0] = 0;
          }
          dump.append(buf);
          pclose(pip);

          FWKSEVERE("TimeBomb reports these threads in process:"
                    << dump << "End of dump.\n");
        }

#elif defined(_WIN32)

        int32_t pid = ACE_OS::getpid();
        char buf[8192];

        sprintf(buf, "bash -c \"perl $GFCPP/../framework/scripts/cdb.pl %d\"",
                pid);
        FILE* pip = _popen(buf, "r");
        if (pip == NULL) {
          FWKSEVERE("TimeBomb: Unable to dump threads.");
        } else {
          perf::sleepSeconds(20);

          std::string dump;
          buf[0] = 0;
          while (fgets(buf, 8192, pip) != NULL) {
            dump.append(buf);
            buf[0] = 0;
          }
          dump.append(buf);
          _pclose(pip);

          FWKSEVERE("TimeBomb reports these threads in process:"
                    << dump << "End of dump.\n");
        }

#endif

        FWKSEVERE("Will now abort by calling exit( " << m_exitCode << " ).");
        exit(m_exitCode);
      }
    }
    sleepSeconds(1);
  }
  return 0;
}
