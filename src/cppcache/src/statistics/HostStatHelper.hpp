#ifndef _GEMFIRE_STATISTICS_HOSTSTATHELPER_HPP_
#define _GEMFIRE_STATISTICS_HOSTSTATHELPER_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <string>
#include "StatisticDescriptorImpl.hpp"
#include <gfcpp/statistics/StatisticsType.hpp>
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>
#include "ProcessStats.hpp"
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include "OsStatisticsImpl.hpp"
#include "LinuxProcessStats.hpp"
#include "SolarisProcessStats.hpp"
#include "StatsDef.hpp"
#include "HostStatHelperWin.hpp"
#include "HostStatHelperLinux.hpp"
#include "HostStatHelperSolaris.hpp"
#include "HostStatHelperNull.hpp"
#include "WindowsProcessStats.hpp"
#include "NullProcessStats.hpp"

// TODO refactor - conditionally include os specific impl headers.

/** @file
*/

namespace gemfire_statistics {
/**
 * Provides native methods which fetch operating system statistics.
 * accessed by calling {@link #getInstance()}.
 */

class CPPCACHE_EXPORT HostStatHelper {
 private:
  static int32 PROCESS_STAT_FLAG;

  static int32 SYSTEM_STAT_FLAG;

  static GFS_OSTYPES osCode;

  static ProcessStats* processStats;

  static void initOSCode();

 public:
  static int32 getCpuUsage();
  static int64 getCpuTime();

  static int32 getNumThreads();

  static void refresh();

  static void newProcessStats(int64 pid, const char* name);

  static void close();

  static void cleanup();
};
};
#endif  //_GEMFIRE_STATISTICS_HOSTSTATHELPER_HPP_
