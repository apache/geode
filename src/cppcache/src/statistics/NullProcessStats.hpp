#ifndef _GEMFIRE_STATISTICS_NULLPROCESSSTATS_HPP_
#define _GEMFIRE_STATISTICS_NULLPROCESSSTATS_HPP_
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
