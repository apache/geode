#ifndef _GEMFIRE_STATISTICS_STATISTICSSAMPLERSTATISTICS_HPP_
#define _GEMFIRE_STATISTICS_STATISTICSSAMPLERSTATISTICS_HPP_
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
#include <gfcpp/statistics/StatisticDescriptor.hpp>
#include <gfcpp/statistics/StatisticsType.hpp>
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticsFactory.hpp>

using namespace apache::geode::client;

/** @file
*/

namespace gemfire_statistics {

class StatisticsFactory;
/**
 * Statistics related to the statistic sampler.
 */
class CPPCACHE_EXPORT StatSamplerStats {
 private:
  StatisticsType* samplerType;
  Statistics* samplerStats;
  int32 sampleCountId;
  int32 sampleTimeId;
  StatisticDescriptor** statDescriptorArr;

 public:
  StatSamplerStats();
  void tookSample(int64 nanosSpentWorking);
  void close();
  void setInitialValues();
  ~StatSamplerStats();
};
}  // namespace gemfire_statistics
#endif  //_GEMFIRE_STATISTICS_STATISTICSSAMPLERSTATISTICS_HPP_
