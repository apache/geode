#ifndef _GEMFIRE_STATISTICS_NULLPROCESSSTATS_HPP_
#define _GEMFIRE_STATISTICS_NULLPROCESSSTATS_HPP_
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
 * Null operating system process that is using a GemFire system.
 *
 */

class CPPCACHE_EXPORT NullProcessStats : public ProcessStats {
 public:
  NullProcessStats(int64 pid, const char* name);
  ~NullProcessStats();

  int64 getProcessSize();
  int32 getCpuUsage();
  int64 getCPUTime();
  int32 getNumThreads();
  int64 getAllCpuTime();
  void close();
};
// Class NullProcessStats
}
// NameSpace

#endif  //_GEMFIRE_STATISTICS_NULLPROCESSSTATS_HPP_
