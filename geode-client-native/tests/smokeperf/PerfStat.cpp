/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "PerfStat.hpp"

using namespace gemfire_statistics;
using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::smokeperf;

PerfStatType * PerfStatType::single = NULL;

void PerfStatType::clean() {
  if (single != NULL) {
    delete single;
    single=NULL;
  }
}

StatisticsType * PerfStatType::getStatType() {
  StatisticsFactory * m_factory = StatisticsFactory::getExistingInstance();

  StatisticsType * statsType = m_factory->findType("cacheperf.CachePerfStats");
  bool largerIsBetter = true;
  if ( statsType == NULL ) {
    statDes[0] = m_factory->createIntCounter(PERF_PUTS,"Number of puts completed.","operations",largerIsBetter);
      statDes[1] = m_factory->createLongCounter(PERF_PUT_TIME,
        "Total time spent doing puts.", "nanoseconds",!largerIsBetter);
    statDes[2] = m_factory->createIntCounter(PERF_UPDATE_EVENTS,
        "Number of update events.", "events",largerIsBetter);
    statDes[3] = m_factory->createLongCounter(PERF_UPDATE_LATENCY,
        "Latency of update operations.", "nanoseconds",!largerIsBetter);
    statDes[4] = m_factory->createIntCounter(PERF_CREATES,
        "Number of creates completed.", "operations",largerIsBetter);
    statDes[5] = m_factory->createLongCounter(PERF_CREATE_TIME,
        "Total time spent doing creates.", "nanoseconds",!largerIsBetter);
    statDes[6] = m_factory->createIntCounter(PERF_LATENCY_SPIKES,
        "Number of latency spikes.", "spikes",!largerIsBetter);
    statDes[7]
        = m_factory->createIntCounter(
            PERF_NEGATIVE_LATENCIES,
            "Number of negative latencies (caused by insufficient clock skew correction).",
            "negatives",!largerIsBetter);
    statDes[8] = m_factory->createIntCounter(PERF_OPS,
        "Number of operations completed.", "operations",largerIsBetter);
    statDes[9] = m_factory->createLongCounter(PERF_OP_TIME,
        "Total time spent doing operations.", "nanoseconds",!largerIsBetter);
    statDes[10] = m_factory->createIntCounter(PERF_CONNECTS,
        "Number of connects completed.", "operations",largerIsBetter);
    statDes[11] = m_factory->createLongCounter(PERF_CONNECT_TIME,
        "Total time spent doing connects.", "nanoseconds",!largerIsBetter);
    statDes[12] = m_factory->createIntCounter(PERF_DISCONNECTS,
        "Number of disconnects completed.", "operations",largerIsBetter);
    statDes[13] = m_factory->createLongCounter(PERF_DISCONNECT_TIME,
        "Total time spent doing disconnects.", "nanoseconds",!largerIsBetter);
    statDes[14] = m_factory->createIntCounter(PERF_GETS,
        "Number of gets completed.", "operations",largerIsBetter);
    statDes[15] = m_factory->createLongCounter(PERF_GET_TIME,
        "Total time spent doing gets.", "nanoseconds",!largerIsBetter);
    statDes[16] = m_factory->createIntCounter(PERF_QUERIES,
            "Number of queries completed.", "operations",largerIsBetter);
    statDes[17] = m_factory->createLongCounter(PERF_QUERY_TIME,
            "Total time spent doing queries.", "nanoseconds",!largerIsBetter);
    statDes[18] = m_factory->createIntCounter(PERF_UPDATES,
                "Number of updates completed.", "operations",largerIsBetter);
    statDes[19] = m_factory->createLongCounter(PERF_UPDATES_TIME,
                "Total time spent doing updates.", "nanoseconds",!largerIsBetter);
    statDes[20] = m_factory->createIntCounter(PERF_GETALL,
                    "Number of getAlls completed.", "operations",largerIsBetter);
    statDes[21] = m_factory->createIntCounter(PERF_PUTALL,
                        "Number of putAlls completed.", "operations",largerIsBetter);
    statsType = m_factory->createType("cacheperf.CachePerfStats", "Application statistics.",statDes, 22);
  }

  return statsType;
}

PerfStatType * PerfStatType::getInstance() {
  if (single == NULL) {
    single =  new PerfStatType();
  }
  return single;
}

PerfStatType::PerfStatType()
{
}

PerfStat::PerfStat(uint32_t threadID)
{

  PerfStatType * regStatType = PerfStatType::getInstance();

  statsType = regStatType->getStatType();

  StatisticsFactory * factory = StatisticsFactory::getExistingInstance();

  char *buf = new char[256];
  sprintf(buf, "ThreadId-%u", threadID);
  //testStat = factory->createAtomicStatistics(statsType, buf);
  testStat = factory->createStatistics(statsType, buf);

}


PerfStat::~PerfStat()
{
  if (testStat != NULL)
  {
    //Don't Delete, Already closed, Just set NULL
  // delete testStat;
    testStat = NULL;
  }
}
