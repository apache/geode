/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#if defined(_SOLARIS)
#include <ace/OS_NS_sys_utsname.h>
#include <ace/OS_NS_errno.h>
#include "HostStatHelperSolaris.hpp"
#include <procfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/systeminfo.h>
#include <sys/proc.h>
#include <sys/kstat.h>
#include <glob.h>
#include <gfcpp/ExceptionTypes.hpp>
#include "SolarisProcessStats.hpp"
#include <ace/OS.h>

using namespace gemfire_statistics;

uint8_t HostStatHelperSolaris::m_logStatErrorCountDown = 5;
uint32_t HostStatHelperSolaris::m_cpuUtilPrev[CPU_STATES] = {0};
bool HostStatHelperSolaris::m_initialized = false;
kstat_ctl_t* HostStatHelperSolaris::m_kstat = NULL;

void HostStatHelperSolaris::refreshProcess(ProcessStats* processStats) {
  static bool threadCountMissingWarning = false;
  // Get pid, SolarisProcessStats
  SolarisProcessStats* solProcessStat =
      dynamic_cast<SolarisProcessStats*>(processStats);
  if (solProcessStat == NULL) {
    LOGFINE(
        "HostStatHelperSolaris::refreshProcess failed due to null processStat");
    return;
  }
  Statistics* stats = solProcessStat->stats;
  int32 thePid = (int32)stats->getNumericId();  // int64 is converted to int

  int32 pid = 0;
  uint32 userTime = 0;
  uint32 sysTime = 0;
  uint32 vsize = 0;
  int32 rss = 0;
  uint32 tempimageSize = 0;
  uint32 temprssSize = 0;
  int32_t cpuUsage = 0;
  int32_t processCpuUsage = 0;
  char procFileName[64];
  int fPtr;
  prusage_t currentUsage;
  psinfo_t currentInfo;

  ACE_OS::snprintf(procFileName, 64, "/proc/%lu/psinfo", (uint32)thePid);
  // fread was returning errno 2 FILENOTFOUND so it was switched to read
  // This matches what was done in the Gemfire Project also
  fPtr = open(procFileName, O_RDONLY, 0); /* read only */
  if (fPtr != -1) {
    if (read(fPtr, &currentInfo, sizeof(currentInfo)) != sizeof(currentInfo)) {
      int32 errNum = errno;  // for debugging
      if (m_logStatErrorCountDown-- > 0) {
        LOGFINE("Error reading procFileName %s, errno %d pid %lu", procFileName,
                errNum, (uint32)thePid);
      }
    } else {
      tempimageSize = currentInfo.pr_size / 1024UL;
      temprssSize = currentInfo.pr_rssize / 1024UL;
      processCpuUsage = (int)((currentInfo.pr_pctcpu / 32768.0) * 100);
    }
    close(fPtr);
  }
  ACE_OS::snprintf(procFileName, 64, "/proc/%u/usage", (uint32)thePid);
  fPtr = open(procFileName, O_RDONLY, 0);
  if (fPtr != -1) {
    if (read(fPtr, &currentUsage, sizeof(currentUsage)) !=
        sizeof(currentUsage)) {
      int32 errNum = errno;  // for debugging
      if (m_logStatErrorCountDown-- > 0) {
        LOGFINE("Error reading procFileName %s, errno %d pid %lu", procFileName,
                errNum, (uint32)thePid);
      }
    } else {
      uint32 usrTimeSecs = currentUsage.pr_utime.tv_sec;
      uint32 usrTimeNanos = currentUsage.pr_utime.tv_nsec;
      userTime = (usrTimeSecs * 1000 * 1000) + (usrTimeNanos / (1000));

      uint32 sysTimeSecs = currentUsage.pr_stime.tv_sec;
      uint32 sysTimeNanos = currentUsage.pr_stime.tv_nsec;
      sysTime = (sysTimeSecs * 1000 * 1000) + (sysTimeNanos / (1000));
    }
    close(fPtr);
  }
  uint32_t cpuUtil[CPU_STATES] = {0};
  try {
    getKernelStats(cpuUtil);
    if (m_initialized) {
      uint32_t idleDelta = cpuUtil[CPU_IDLE] - m_cpuUtilPrev[CPU_IDLE];
      double timeDelta = idleDelta;
      timeDelta += (cpuUtil[CPU_USER] - m_cpuUtilPrev[CPU_USER]);
      timeDelta += (cpuUtil[CPU_KERNEL] - m_cpuUtilPrev[CPU_KERNEL]);
      timeDelta += (cpuUtil[CPU_WAIT] - m_cpuUtilPrev[CPU_WAIT]);
      if (timeDelta > CPU_USAGE_STAT_THRESHOLD) {
        // Math is equivalent to 100*(timeDelta-idleDelta)/timeDelta, rounding
        // aside
        cpuUsage = 100 - 100 * (idleDelta) / timeDelta;
        // Update the previous copy
        memcpy(m_cpuUtilPrev, cpuUtil, sizeof(uint32_t[CPU_STATES]));
      } else {
        if (m_logStatErrorCountDown-- > 0) {
          LOGFINE(
              "Kernel stat sampling failed to meet CPU_USAGE_THRESHOLD, "
              "reporting 100%% cpu load");
        }
        cpuUsage = 100;
      }
    } else {
      // Update the previous copy
      memcpy(m_cpuUtilPrev, cpuUtil, sizeof(uint32_t[CPU_STATES]));
      m_initialized = true;
    }
  } catch (NullPointerException npe) {
    if (m_logStatErrorCountDown-- > 0) {
      LOGERROR(npe.getMessage());
    }
  }
  // thread count
  int threadCount = 0;
  glob_t g;
  if (glob("/proc/self/lwp/*", 0, NULL, &g) == 0) {
    threadCount = g.gl_pathc;
    // LOGDEBUG("gl_pathc: %d",g.gl_pathc);
    // for( unsigned int i =0; i < g.gl_pathc; i++ ) {
    //  LOGDEBUG(" LWP: %s ",g.gl_pathv[i]);
    //}
  } else {
    if (threadCountMissingWarning == false) {
      threadCountMissingWarning = true;
      LOGWARN("Stats: Number of threads in process are not available.");
    }
  }
  globfree(&g);

  stats->setInt(solProcessStat->imageSizeINT, tempimageSize);
  stats->setInt(solProcessStat->rssSizeINT, temprssSize);
  stats->setInt(solProcessStat->userTimeINT, userTime);
  stats->setInt(solProcessStat->systemTimeINT, sysTime);
  stats->setInt(solProcessStat->hostCpuUsageINT, cpuUsage);
  stats->setInt(solProcessStat->processCpuUsageINT, processCpuUsage);
  stats->setInt(solProcessStat->threadsINT, threadCount);
}

void HostStatHelperSolaris::getKernelStats(uint32_t* cpuUtil) {
  if (m_kstat == NULL) {
    m_kstat = kstat_open();
    if (m_kstat == NULL) {
      char buf[128];
      ACE_OS::snprintf(buf, 128, "Unable to obtain kstat data, errno %d",
                       errno);
      throw NullPointerException(buf);
    }
  }
  kstat_ctl_t wkc;
  kstat_t* ksp;
  kstat_t* wksp;
  cpu_stat_t rcw_cpu_stat_t;
  char module[KSTAT_STRLEN + 1];
  kid_t kcid;

  // Make a copy of it.
  memcpy(&wkc, m_kstat, sizeof(kstat_ctl_t));
  // Walk the chain.
  for (ksp = wkc.kc_chain; ksp != NULL; ksp = ksp->ks_next) {
    // Header information
    // kstats are identified by module, instance, class, and name.
    strncpy(module, ksp->ks_module, KSTAT_STRLEN);
    module[KSTAT_STRLEN] = '\0';
    // With raw data you must know what to expect.
    if (ksp->ks_type == KSTAT_TYPE_RAW) {
      if (strcmp(module, "cpu_stat") == 0) {
        wksp = ksp;
        // Read the data corresponding to the pointer. cpu_stat modules deliver
        // cpu_stat_t data.
        kcid = kstat_read(&wkc, wksp, &rcw_cpu_stat_t);
        if (kcid == -1) {
          LOGERROR("Error reading stats, errno \"%d\"", errno);
          throw NullPointerException(
              "Error reading cpu_stat struct from kernal stats");
        }
        /*
        LOGDEBUG("rcw_cpu_stat_t cpu idle= %u, user= %u, sys=%u, wait=%u, \n",
                    rcw_cpu_stat_t.cpu_sysinfo.cpu[CPU_IDLE],
                    rcw_cpu_stat_t.cpu_sysinfo.cpu[CPU_USER],
                    rcw_cpu_stat_t.cpu_sysinfo.cpu[CPU_KERNEL],
                    rcw_cpu_stat_t.cpu_sysinfo.cpu[CPU_WAIT] );
        */
        cpuUtil[CPU_IDLE] = rcw_cpu_stat_t.cpu_sysinfo.cpu[CPU_IDLE];
        cpuUtil[CPU_USER] = rcw_cpu_stat_t.cpu_sysinfo.cpu[CPU_USER];
        cpuUtil[CPU_KERNEL] = rcw_cpu_stat_t.cpu_sysinfo.cpu[CPU_KERNEL];
        cpuUtil[CPU_WAIT] = rcw_cpu_stat_t.cpu_sysinfo.cpu[CPU_WAIT];
      }
    }
  }
}

void HostStatHelperSolaris::closeHostStatHelperSolaris() {
  if (m_kstat != NULL) {
    kstat_close(m_kstat);
    m_kstat = NULL;
  }
}
#endif /* SOLARIS */
