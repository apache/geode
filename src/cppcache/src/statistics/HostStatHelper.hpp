#pragma once

#ifndef GEODE_STATISTICS_HOSTSTATHELPER_H_
#define GEODE_STATISTICS_HOSTSTATHELPER_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

namespace apache {
namespace geode {
namespace statistics {
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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_STATISTICS_HOSTSTATHELPER_H_
