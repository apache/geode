/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <ace/Thread_Mutex.h>
#include <ace/Singleton.h>
#include "SolarisProcessStats.hpp"
#include "GemfireStatisticsFactory.hpp"
#include "HostStatHelperSolaris.hpp"
using namespace gemfire_statistics;

/**
 * <P>This class provides the interface for statistics about a
 * Solaris operating system process that is using a GemFire system.
 *
 */

SolarisProcessStats::SolarisProcessStats(int64 pid, const char* name) {
  GemfireStatisticsFactory* statFactory =
      GemfireStatisticsFactory::getExistingInstance();

  // Create Statistics Type
  createType(statFactory);

  // Create Statistics
  this->stats = statFactory->createOsStatistics(m_statsType, name, pid);
  GF_D_ASSERT(this->stats != NULL);

// Refresh Stats Values
#if defined(_SOLARIS)
  HostStatHelperSolaris::refreshProcess(this);
#endif  // if def(_SOLARIS)
}

/**
 * Creates the StatisticsType for collecting the Stats of a Solaris process
 * This function is called by the class HostStatHelper before objects of
 * SolarisProcessStatistics are created by it.
 */
void SolarisProcessStats::createType(StatisticsFactory* statFactory) {
  try {
    StatisticDescriptor** statDescriptorArr = new StatisticDescriptor*[7];
    statDescriptorArr[0] = statFactory->createIntGauge(
        "imageSize", "The size of the process's image in megabytes.",
        "megabytes");

    statDescriptorArr[1] = statFactory->createIntGauge(
        "rssSize", "The size of the process's resident set size in megabytes.",
        "megabytes");

    statDescriptorArr[2] = statFactory->createIntCounter(
        "userTime", "The os statistic for the process cpu usage in user time",
        "microseconds");

    statDescriptorArr[3] = statFactory->createIntCounter(
        "systemTime",
        "The os statistic for the process cpu usage in system time",
        "microseconds");

    statDescriptorArr[4] = statFactory->createIntGauge(
        "processCpuUsage", "The os statistic for the cpu usage of this process",
        "percent cpu");

    statDescriptorArr[5] = statFactory->createIntGauge(
        "hostCpuUsage", "The os statistic for the host cpu usage",
        "percent cpu");

    statDescriptorArr[6] = statFactory->createIntGauge(
        "threads", "Number of threads currently active in this process.",
        "threads");

    try {
      m_statsType = statFactory->createType("SolarisProcessStats",
                                            "Statistics for a Solaris process.",
                                            statDescriptorArr, 7);
    } catch (Exception&) {
      m_statsType = statFactory->findType("SolarisProcessStats");
    }
    if (m_statsType == NULL) {
      throw OutOfMemoryException("SolarisProcessStats::createType: out memory");
    }
    imageSizeINT = m_statsType->nameToId("imageSize");
    rssSizeINT = m_statsType->nameToId("rssSize");
    userTimeINT = m_statsType->nameToId("userTime");
    systemTimeINT = m_statsType->nameToId("systemTime");
    processCpuUsageINT = m_statsType->nameToId("processCpuUsage");
    hostCpuUsageINT = m_statsType->nameToId("hostCpuUsage");
    threadsINT = m_statsType->nameToId("threads");
  } catch (IllegalArgumentException&) {
    GF_D_ASSERT(false);
    throw;
  }
}

int64 SolarisProcessStats::getProcessSize() {
  return static_cast<int64>(stats->getInt(rssSizeINT));
}

int32 SolarisProcessStats::getCpuUsage() {
  return stats->getInt(hostCpuUsageINT);
}
int64 SolarisProcessStats::getCPUTime() { return stats->getInt(userTimeINT); }
int32 SolarisProcessStats::getNumThreads() { return stats->getInt(threadsINT); }
int64 SolarisProcessStats::getAllCpuTime() {
  return ((stats->getInt(userTimeINT)) + (stats->getInt(systemTimeINT)));
}

void SolarisProcessStats::close() {
  if (stats != NULL) {
    stats->close();
  }
}

SolarisProcessStats::~SolarisProcessStats() {
  m_statsType = NULL;
  stats = NULL;
}
