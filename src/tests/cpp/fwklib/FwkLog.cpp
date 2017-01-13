/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include <gfcpp/Exception.hpp>

using namespace gemfire;
using namespace testframework;

static ACE_utsname u;

const char* gemfire::testframework::getNodeName() { return u.nodename; }

const char* gemfire::testframework::dirAndFile(const char* str) {
  if (str == NULL) {
    return "NULL";
  }

  const char* ptr = str + strlen(str);
  int32_t found = 0;
  while (ptr > str) {
    ptr--;
    if ((*ptr == '/') || (*ptr == '\\')) {
      found++;
      if (found >= 2) {
        if (ptr == str) {
          return str;
        }
        return ++ptr;
      }
    }
  }
  return ptr;
}

void gemfire::testframework::plog(const char* l, const char* s,
                                  const char* filename, int32_t lineno) {
  // ACE_TCHAR tstamp[64];
  // ACE::timestamp( tstamp, 64, 1 );
  // tstamp is like "Tue May 17 2005 12:54:22.546780"
  // for our purpose we just want "12:54:22.546780"
  char buf[256] = {0};
  const size_t MINBUFSIZE = 128;
  ACE_Time_Value clock = ACE_OS::gettimeofday();
  time_t secs = clock.sec();
  struct tm* tm_val = ACE_OS::localtime(&secs);
  char* pbuf = buf;
  pbuf += ACE_OS::strftime(pbuf, MINBUFSIZE, "%Y/%m/%d %H:%M:%S", tm_val);
  pbuf +=
      ACE_OS::snprintf(pbuf, 15, ".%06ld ", static_cast<long>(clock.usec()));
  pbuf += ACE_OS::strftime(pbuf, MINBUFSIZE, "%Z ", tm_val);
  static bool needInit = true;
  if (needInit) {
    ACE_OS::uname(&u);
    needInit = false;
  }

  const char* fil = dirAndFile(filename);

  fprintf(stdout, "[%s %s %s:P%d:T%lu]::%s::%d  %s  %s\n", buf, u.sysname,
          u.nodename, ACE_OS::getpid(), (unsigned long)(ACE_Thread::self()),
          fil, lineno, l, s);
  fflush(stdout);
}

void gemfire::testframework::dumpStack() {
  gemfire::Exception trace("StackTrace", "  ", true);
  trace.printStackTrace();
}
