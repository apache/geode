/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#if defined(_LINUX)

#include <ace/OS_NS_sys_utsname.h>
#include <ace/OS_NS_errno.h>
#include <glob.h>
#include "HostStatHelperLinux.hpp"
#include "LinuxProcessStats.hpp"
#include <ace/OS.h>

using namespace gemfire_statistics;

namespace gemfire_statistics {
double lastIdle = 0;
double lastUptime = 0;
}

uint8_t HostStatHelperLinux::m_logStatErrorCountDown = 5;

void HostStatHelperLinux::refreshProcess(ProcessStats* processStats) {
  static bool threadCountMissingWarning = false;
  // Get pid, LinuxProcessStats
  LinuxProcessStats* linProcessStat =
      dynamic_cast<LinuxProcessStats*>(processStats);
  if (linProcessStat == NULL) {
    LOGFINE(
        "HostStatHelperLinux::refreshProcess failed due to null processStat");
    return;
  }
  Statistics* stats = linProcessStat->stats;
  int32 thePid = (int32)stats->getNumericId();  // int64 is converted to int

  int32 pid = 0;
  char commandLine[100];
  commandLine[0] = '\0';
  commandLine[sizeof(commandLine) - 1] = '\0';
  char state = 0;
  uint32 userTime = 0;
  uint32 sysTime = 0;
  uint32 vsize = 0;
  int32 rss = 0;
  uint32 tempimageSize = 0;
  uint32 temprssSize = 0;
  int32_t cpuUsage = 0;
  char procFileName[64];

  FILE* fPtr;
  ACE_OS::snprintf(procFileName, 64, "/proc/%" PRIu32 "/stat", (uint32)thePid);
  fPtr = fopen(procFileName, "r"); /* read only */
  if (fPtr != NULL) {
    int32 status = fscanf(
        fPtr,
        "%d %100s %c %*d %*d %*d %*d %*d %*u %*u \
%*u %*u %*u %u %u %*d %*d %*d %*d %*d %*d %*u %u %d ",
        &pid, &commandLine[0], &state,
        // ppid,
        // pgrp,
        // session,
        // tty_nr,
        // tty_pgrp,
        // flags,
        // min_flt,   // end first line of format string

        // cmin_flt,
        // maj_flt,
        // cmaj_flt,
        &userTime,  // task->times.tms_utime,
        &sysTime,   // task->times.tms_stime,
        // deadChildUtime, // task->times.tms_cutime,
        // deadChildSysTime, // task->times.tms_cstime,
        // priority,
        // nice,
        // unused1, // 0UL /* removed */,
        // it_real_value,
        // start_time,
        &vsize,
        &rss  //  mm ? mm->rss : 0, /* you might want to shift this left 3 */
        );

    if (status != 7 && status != EOF) {
      int32 errNum = errno;  // for debugging
      if (m_logStatErrorCountDown-- > 0) {
        LOGFINE("Error reading procFileName %s, status %d errno %d pid %lu",
                procFileName, status, errNum, (uint32)thePid);
      }
      //  UTL_ASSERT(status == 7);
    }
    status = fclose(fPtr);
    if (status) {
      /*
      int32 errNum = errno; // for debugging
      errNum = errNum; // lint
      */
    }
  }
  tempimageSize = vsize / (1024 * 1024);  // assume linux units = Kbytes

  uint32 pageSize = 1;
  struct sysinfo info;
  int32 status = sysinfo(&info);
  if (status == 0) {
    pageSize = info.mem_unit;
  }
  if (pageSize != 1) {
    temprssSize = (rss * pageSize) / (1024 * 1024);
  }

  if (temprssSize == 0) {
    // Assuming 4096 pageSize
    temprssSize = (4 * rss) / 1024;
  }
  fPtr = fopen("/proc/uptime", "r");
  if (fPtr != NULL) {
    double newUptime = 0;
    double newIdle = 0;
    int32 status = fscanf(fPtr, "%lf %lf", &newUptime, &newIdle);
    if (status != 2 && status != EOF) {
      int32 errNum = errno;  // for debugging
      if (m_logStatErrorCountDown-- > 0) {
        LOGFINE("Error reading procFileName %s, status %d errno %d pid %lu",
                procFileName, status, errNum, (uint32)thePid);
      }
    }
    fclose(fPtr);
    if (lastUptime != 0) {
      double idleDelta = newIdle - lastIdle;
      double uptimeDelta = newUptime - lastUptime;
      if (uptimeDelta > 0) {
        double percentIdle = idleDelta / uptimeDelta;
        if (percentIdle < 0) {
          percentIdle *= -1.0;
        }
        cpuUsage = (int)(1.0 - percentIdle) * 100;
      }
    }
    lastIdle = newIdle;
    lastUptime = newUptime;
  }
  // thread count
  int threadCount = 0;
  glob_t g;
  if (glob("/proc/self/task/*", GLOB_ONLYDIR, NULL, &g) == 0) {
    threadCount = g.gl_pathc;
    // LOGDEBUG("gl_pathc: %d",g.gl_pathc);
    // for( unsigned int i =0; i < g.gl_pathc; i++ ) {
    //  LOGDEBUG(" Task: %s ",g.gl_pathv[i]);
    //}
  } else {
    if (threadCountMissingWarning == false) {
      threadCountMissingWarning = true;
      LOGWARN("Stats: Number of threads in process are not available.");
    }
  }
  globfree(&g);

  stats->setInt(linProcessStat->imageSizeINT, tempimageSize);
  stats->setInt(linProcessStat->rssSizeINT, temprssSize);
  stats->setInt(linProcessStat->userTimeINT, userTime);
  stats->setInt(linProcessStat->systemTimeINT, sysTime);
  stats->setInt(linProcessStat->hostCpuUsageINT, cpuUsage);
  stats->setInt(linProcessStat->threadsINT, threadCount);
}

#endif  // if defined(_LINUX)
