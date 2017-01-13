/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include <ace/OS_NS_sys_utsname.h>
#include "HostStatHelper.hpp"
#include "GemfireStatisticsFactory.hpp"

using namespace gemfire_statistics;

/**
 * Provides native methods which fetch operating system statistics.
 * accessed by calling {@link #getInstance()}.
 *
 */

int32 HostStatHelper::PROCESS_STAT_FLAG = 1;
int32 HostStatHelper::SYSTEM_STAT_FLAG = 2;
GFS_OSTYPES HostStatHelper::osCode =
    static_cast<GFS_OSTYPES>(0);  // Default OS is Linux
ProcessStats* HostStatHelper::processStats = NULL;

/**
 * Determine the OS and creates Process Statistics Type for that OS
 */
void HostStatHelper::initOSCode() {
  ACE_utsname u;
  ACE_OS::uname(&u);
  std::string osName(u.sysname);

  if (osName == "Linux") {
    osCode = GFS_OSTYPE_LINUX;
  } else if ((osName == "Windows") || (osName == "Win32")) {
    osCode = GFS_OSTYPE_WINDOWS;
  } else if (osName == "SunOS") {
    osCode = GFS_OSTYPE_SOLARIS;
  } else if (osName == "Darwin") {
    osCode = GFS_OSTYPE_MACOSX;
  } else {
    char buf[1024] = {0};
    ACE_OS::snprintf(buf, 1024,
                     "HostStatHelper::initOSTypes:unhandled os type: %s",
                     osName.c_str());
    throw IllegalArgumentException(buf);
  }
}

/**
 * Refresh statistics of the process through operating system specific calls
 */
void HostStatHelper::refresh() {
  if (processStats != NULL) {
#if defined(_WIN32)
    HostStatHelperWin::refreshProcess(processStats);
#elif defined(_SOLARIS)
    HostStatHelperSolaris::refreshProcess(processStats);
#elif defined(_LINUX)
    HostStatHelperLinux::refreshProcess(processStats);
#elif defined(_MACOSX)
    HostStatHelperNull::refreshProcess(processStats);
#else
#error missing stats helper
#endif
  }
}

/**
 * Creates and returns a {@link Statistics} with
 * the given pid and name.
 */
void HostStatHelper::newProcessStats(int64 pid, const char* name) {
  // Init OsCode
  initOSCode();

  // Create processStats , Internally they will create own stats
  switch (osCode) {
    case GFS_OSTYPE_SOLARIS:
      processStats = new SolarisProcessStats(pid, name);
      break;
    case GFS_OSTYPE_LINUX:
      processStats = new LinuxProcessStats(pid, name);
      break;
    case GFS_OSTYPE_WINDOWS:
      processStats = new WindowsProcessStats(pid, name);
      break;
    case GFS_OSTYPE_MACOSX:
      processStats = new NullProcessStats(pid, name);
      break;
    default:
      throw IllegalArgumentException(
          "HostStatHelper::newProcess:unhandled osCodem");
  }
  GF_D_ASSERT(processStats != NULL);
}

void HostStatHelper::close() {
  if (processStats) {
    processStats->close();
  }
}

void HostStatHelper::cleanup() {
#if defined(_WIN32)
  HostStatHelperWin::closeHostStatHelperWin();  // close registry structures
#endif
#if defined(_SOLARIS)
  HostStatHelperSolaris::closeHostStatHelperSolaris();  // close kstats
#endif
  if (processStats) {
    delete processStats;
    processStats = NULL;
  }
}

int32 HostStatHelper::getCpuUsage() {
  if (HostStatHelper::processStats != NULL) {
    return HostStatHelper::processStats->getCpuUsage();
  }
  return 0;
}

int64 HostStatHelper::getCpuTime() {
  if (HostStatHelper::processStats != NULL) {
    return HostStatHelper::processStats->getAllCpuTime();
  }
  return 0;
}
int32 HostStatHelper::getNumThreads() {
  if (HostStatHelper::processStats != NULL) {
    return HostStatHelper::processStats->getNumThreads();
  }
  return 0;
}
