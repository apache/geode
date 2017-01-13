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
#include "LinuxProcessStats.hpp"
#include "GemfireStatisticsFactory.hpp"
#include "HostStatHelperLinux.hpp"
using namespace gemfire_statistics;

/**
 * <P>This class provides the interface for statistics about a
 * Linux operating system process that is using a GemFire system.
 *
 */

LinuxProcessStats::LinuxProcessStats(int64 pid, const char* name) {
  GemfireStatisticsFactory* statFactory =
      GemfireStatisticsFactory::getExistingInstance();

  // Create Statistics Type
  createType(statFactory);

  // Create Statistics
  this->stats = statFactory->createOsStatistics(m_statsType, name, pid);
  GF_D_ASSERT(this->stats != NULL);

// Refresh Stats Values
#if defined(_LINUX)
  HostStatHelperLinux::refreshProcess(this);
#endif  // if defined(_LINUX)
}

/**
 * Creates the StatisticsType for collecting the Stats of a Linux process
 * This function is called by the class HostStatHelper before objects of
 * LinuxProcessStatistics are created by it.
 */
void LinuxProcessStats::createType(StatisticsFactory* statFactory) {
  try {
    StatisticDescriptor** statDescriptorArr = new StatisticDescriptor*[6];
    statDescriptorArr[0] = statFactory->createIntGauge(
        "imageSize", "The size of the process's image in megabytes.",
        "megabytes");

    statDescriptorArr[1] = statFactory->createIntGauge(
        "rssSize", "The size of the process's resident set size in megabytes.",
        "megabytes");

    statDescriptorArr[2] = statFactory->createIntCounter(
        "userTime", "The os statistic for the process cpu usage in user time",
        "jiffies");

    statDescriptorArr[3] = statFactory->createIntCounter(
        "systemTime",
        "The os statistic for the process cpu usage in system time", "jiffies");

    statDescriptorArr[4] = statFactory->createIntGauge(
        "hostCpuUsage", "The os statistic for the host cpu usage",
        "percent cpu");

    statDescriptorArr[5] = statFactory->createIntGauge(
        "threads", "Number of threads currently active in this process.",
        "threads");

    try {
      m_statsType = statFactory->createType("LinuxProcessStats",
                                            "Statistics for a Linux process.",
                                            statDescriptorArr, 6);
    } catch (Exception&) {
      m_statsType = statFactory->findType("LinuxProcessStats");
    }
    if (m_statsType == NULL) {
      throw OutOfMemoryException("LinuxProcessStats::createType: out memory");
    }
    imageSizeINT = m_statsType->nameToId("imageSize");
    rssSizeINT = m_statsType->nameToId("rssSize");
    userTimeINT = m_statsType->nameToId("userTime");
    systemTimeINT = m_statsType->nameToId("systemTime");
    hostCpuUsageINT = m_statsType->nameToId("hostCpuUsage");
    threadsINT = m_statsType->nameToId("threads");
  } catch (IllegalArgumentException&) {
    GF_D_ASSERT(false);
    throw;
  }
}

int64 LinuxProcessStats::getProcessSize() {
  return static_cast<int64>(stats->getInt(rssSizeINT));
}

int32 LinuxProcessStats::getCpuUsage() {
  return stats->getInt(hostCpuUsageINT);
}
int64 LinuxProcessStats::getCPUTime() { return stats->getInt(userTimeINT); }
int32 LinuxProcessStats::getNumThreads() { return stats->getInt(threadsINT); }
int64 LinuxProcessStats::getAllCpuTime() {
  return ((stats->getInt(userTimeINT)) + (stats->getInt(systemTimeINT)));
}

void LinuxProcessStats::close() {
  if (stats != NULL) {
    stats->close();
  }
}

LinuxProcessStats::~LinuxProcessStats() {
  m_statsType = NULL;
  stats = NULL;
}
