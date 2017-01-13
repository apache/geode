/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "StatSamplerStats.hpp"
using namespace gemfire_statistics;

/**
  * Statistics related to the statistic sampler.
*/

StatSamplerStats::StatSamplerStats() {
  StatisticsFactory* statFactory = StatisticsFactory::getExistingInstance();
  statDescriptorArr = new StatisticDescriptor*[2];
  statDescriptorArr[0] = statFactory->createIntCounter(
      "sampleCount", "Total number of samples taken by this sampler.",
      "samples", false);

  statDescriptorArr[1] = statFactory->createLongCounter(
      "sampleTime", "Total amount of time spent taking samples.",
      "milliseconds", false);

  samplerType =
      statFactory->createType("StatSampler", "Stats on the statistic sampler.",
                              StatSamplerStats::statDescriptorArr, 2);
  sampleCountId = samplerType->nameToId("sampleCount");
  sampleTimeId = samplerType->nameToId("sampleTime");
  this->samplerStats = statFactory->createStatistics(samplerType, "statSampler",
                                                     statFactory->getId());
}

/**
 * The default values of the individual Stats descriptors are zero.
 * This function can be used to set initial values to the descriptors.
 */
void StatSamplerStats::setInitialValues() {
  if (samplerStats) {
    samplerStats->setInt(sampleCountId, 0);
    samplerStats->setLong(sampleTimeId, 0);
  }
}

/**
 * This function is called to refresh the values
 * of the individual Stats descriptors.
 */
void StatSamplerStats::tookSample(int64 nanosSpentWorking) {
  if (samplerStats) {
    samplerStats->incInt(sampleCountId, 1);
    samplerStats->incLong(sampleTimeId, nanosSpentWorking / 1000000);
  }
}

/**
 * Calls the destroyStatistics() function of the factory
 * which deletes the Stats object
 * It is mandatory to call this function for proper deletion of stats objects.
 */
void StatSamplerStats::close() {
  if (samplerStats) {
    samplerStats->close();
  }
}

/**
 * All objects are created by factory, and the reference is passed to this class
 * Factory takes the responsibility of deleting the objetcs.
 * Hence they can be simply set to NULL here.
 * But it is mandatory that StatSamplerStats::close() be called
 * before this destructor gets called,
 * otherwise samplerStats will not get deleted and a memory leak will occur.
 */
StatSamplerStats::~StatSamplerStats() {
  samplerType = NULL;
  for (int32 i = 0; i < 2; i++) {
    statDescriptorArr[i] = NULL;
  }
  samplerStats = NULL;
}
