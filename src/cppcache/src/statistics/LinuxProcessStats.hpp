#pragma once

#ifndef GEODE_STATISTICS_LINUXPROCESSSTATS_H_
#define GEODE_STATISTICS_LINUXPROCESSSTATS_H_

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
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticsType.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>
#include "ProcessStats.hpp"
#include "HostStatHelper.hpp"

using namespace apache::geode::client;

/** @file
*/

namespace apache {
namespace geode {
namespace statistics {
/**
 * <P>This class provides the interface for statistics about a
 * Linux operating system process that is using a GemFire system.
 *
 */

class CPPCACHE_EXPORT LinuxProcessStats : public ProcessStats {
 private:
  /** The Static Type for Linux Process Stats */
  StatisticsType* m_statsType;

  /** Ids of All Stats Desciptors for seting new values */
  int32 rssSizeINT;
  int32 imageSizeINT;
  int32 userTimeINT;
  int32 systemTimeINT;
  int32 hostCpuUsageINT;
  int32 threadsINT;

  /** The underlying statistics */
  Statistics* stats;

  void createType(StatisticsFactory* statFactory);

 public:
  LinuxProcessStats(int64 pid, const char* name);
  ~LinuxProcessStats();

  int64 getProcessSize();
  int32 getCpuUsage();
  int32 getNumThreads();
  int64 getCPUTime();
  int64 getAllCpuTime();
  /**
   * Close Underline Statistics
   */
  void close();

#if defined(_LINUX)
  friend class HostStatHelperLinux;
#endif  // if defined(_LINUX)

};  // Class LinuxProcessStats

}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_STATISTICS_LINUXPROCESSSTATS_H_
