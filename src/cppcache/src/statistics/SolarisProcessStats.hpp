#ifndef _GEMFIRE_STATISTICS_SOLARISPROCESSSTATS_HPP_
#define _GEMFIRE_STATISTICS_SOLARISPROCESSSTATS_HPP_
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
#include "ProcessStats.hpp"
#include "HostStatHelper.hpp"

using namespace gemfire;

/** @file
*/

namespace gemfire_statistics {
/**
 * <P>This class provides the interface for statistics about a
 * Solaris operating system process that is using a GemFire system.
 *
 */

class CPPCACHE_EXPORT SolarisProcessStats : public ProcessStats {
 private:
  /** The Static Type for Solaris Process Stats */
  StatisticsType* m_statsType;

  /** Ids of All Stats Desciptors for seting new values */
  int32 rssSizeINT;
  int32 imageSizeINT;
  int32 userTimeINT;
  int32 systemTimeINT;
  int32 processCpuUsageINT;
  int32 hostCpuUsageINT;
  int32 threadsINT;

  /** The underlying statistics */
  Statistics* stats;

  void createType(StatisticsFactory* statFactory);

 public:
  SolarisProcessStats(int64 pid, const char* name);
  ~SolarisProcessStats();

  int64 getProcessSize();
  int32 getCpuUsage();
  int64 getCPUTime();
  int32 getNumThreads();
  int64 getAllCpuTime();

  /**
   * Close Underline Statistics
   */
  void close();

#if defined(_SOLARIS)
  friend class HostStatHelperSolaris;
#endif  // if def(_SOLARIS)

};  // Class SolarisProcessStats

};  // NameSpace

#endif  //_GEMFIRE_STATISTICS_SOLARISPROCESSSTATS_HPP_
