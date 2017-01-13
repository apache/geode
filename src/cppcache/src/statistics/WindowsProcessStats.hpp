#ifndef _GEMFIRE_STATISTICS_WINDOWSPROCESSSTATS_HPP_
#define _GEMFIRE_STATISTICS_WINDOWSPROCESSSTATS_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticsType.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include "ProcessStats.hpp"
#include <gfcpp/ExceptionTypes.hpp>

using namespace gemfire;

/** @file
*/

namespace gemfire_statistics {
/**
 * <P>This class provides the interface for statistics about a
 * Windows operating system process that is using a GemFire system.
 */

class CPPCACHE_EXPORT WindowsProcessStats : public ProcessStats {
 private:
  /** The Static Type for Windows Process Stats */
  StatisticsType* m_statsType;

  /** Ids of All Stats Desciptors for seting new values */
  int32 handlesINT;
  int32 priorityBaseINT;
  int32 threadsINT;
  int32 activeTimeLONG;
  int32 pageFaultsLONG;
  int32 pageFileSizeLONG;
  int32 pageFileSizePeakLONG;
  int32 privateSizeLONG;
  int32 systemTimeLONG;
  int32 userTimeLONG;
  int32 virtualSizeLONG;
  int32 virtualSizePeakLONG;
  int32 workingSetSizeLONG;
  int32 workingSetSizePeakLONG;
  int32 cpuUsageINT;

  /** The underlying statistics */
  Statistics* stats;

  void createType(StatisticsFactory* statFactory);

 public:
  WindowsProcessStats(int64 pid, const char* name);
  ~WindowsProcessStats();

  int64 getProcessSize();
  int32 getCpuUsage();
  int64 getCPUTime();
  int32 getNumThreads();
  int64 getAllCpuTime();
  /**
   * Close Underline Statistics
   */
  void close();

  friend class HostStatHelperWin;
};
};

#endif  //_GEMFIRE_STATISTICS_WINDOWSPROCESSSTATS_HPP_
