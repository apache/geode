#ifndef _GEMFIRE_STATISTICS_HOSTSTATHELPERSOLARIS_HPP_
#define _GEMFIRE_STATISTICS_HOSTSTATHELPERSOLARIS_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#if defined(_SOLARIS)
#include <gfcpp/gfcpp_globals.hpp>
#include <string>
#include <sys/sysinfo.h>
#include "ProcessStats.hpp"
#include <kstat.h>

/*
  * CPU_USAGE_STAT_THRESHOLD sets how much time must pass between samples
  * before a new cpu utilization is calculated.
  * Units are in 100ths of a second.
  * if set too low you will get divide by zero overflows or scewed data
  * due to rounding errors.
  * This is likely unnecesary with our stat sampling interval being 1 second.
*/
#define CPU_USAGE_STAT_THRESHOLD 10

/** @file
*/

namespace gemfire_statistics {

/**
 * Solaris Implementation to fetch operating system stats.
 *
 */

class HostStatHelperSolaris {
 public:
  static void refreshProcess(ProcessStats* processStats);
  static void closeHostStatHelperSolaris();
  // static refreeshSystem(Statistics* stats);

 private:
  static uint8_t m_logStatErrorCountDown;
  static kstat_ctl_t* m_kstat;
  static uint32_t m_cpuUtilPrev[CPU_STATES];
  static bool m_initialized;
  static void getKernelStats(uint32_t*);
};
};

#endif  // if def(_SOLARIS)

#endif  // _GEMFIRE_STATISTICS_HOSTSTATHELPERSOLARIS_HPP_
