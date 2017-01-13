#ifndef _GEMFIRE_STATISTICS_STATISTICSSAMPLERSTATISTICS_HPP_
#define _GEMFIRE_STATISTICS_STATISTICSSAMPLERSTATISTICS_HPP_
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>
#include <gfcpp/statistics/StatisticsType.hpp>
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticsFactory.hpp>

using namespace gemfire;

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
};
#endif  //_GEMFIRE_STATISTICS_STATISTICSSAMPLERSTATISTICS_HPP_
