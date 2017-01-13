/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include "RegionStats.hpp"
//#include "StatisticsFactory.hpp"

#include <ace/Thread_Mutex.h>
#include <ace/Singleton.h>

const char* statsName = (const char*)"RegionStatistics";
const char* statsDesc = (const char*)"Statistics for this region";

////////////////////////////////////////////////////////////////////////////////

namespace gemfire {

using namespace gemfire_statistics;

////////////////////////////////////////////////////////////////////////////////

RegionStatType* RegionStatType::single = NULL;
SpinLock RegionStatType::m_singletonLock;
SpinLock RegionStatType::m_statTypeLock;

void RegionStatType::clean() {
  SpinLockGuard guard(m_singletonLock);
  if (single != NULL) {
    delete single;
    single = NULL;
  }
}

StatisticsType* RegionStatType::getStatType() {
  const bool largerIsBetter = true;
  SpinLockGuard guard(m_statTypeLock);
  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
  GF_D_ASSERT(!!factory);

  StatisticsType* statsType = factory->findType("RegionStatistics");

  if (statsType == NULL) {
    m_stats[0] = factory->createIntCounter(
        "creates", "The total number of cache creates for this region",
        "entries", largerIsBetter);
    m_stats[1] = factory->createIntCounter(
        "puts", "The total number of cache puts for this region", "entries",
        largerIsBetter);
    m_stats[2] = factory->createIntCounter(
        "gets", "The total number of cache gets for this region", "entries",
        largerIsBetter);
    m_stats[3] = factory->createIntCounter(
        "hits", "The total number of cache hits for this region", "entries",
        largerIsBetter);
    m_stats[4] = factory->createIntCounter(
        "misses", "The total number of cache misses for this region", "entries",
        !largerIsBetter);
    m_stats[5] = factory->createIntGauge(
        "entries", "The current number of cache entries for this region",
        "entries", largerIsBetter);
    m_stats[6] = factory->createIntCounter(
        "destroys", "The total number of cache destroys for this region",
        "entries", largerIsBetter);
    m_stats[7] =
        factory->createIntCounter("overflows",
                                  "The total number of cache overflows for "
                                  "this region to persistence backup",
                                  "entries", largerIsBetter);
    m_stats[8] =
        factory->createIntCounter("retrieves",
                                  "The total number of cache entries fetched "
                                  "from persistence backup into the cache",
                                  "entries", largerIsBetter);

    m_stats[9] = factory->createIntCounter(
        "nonSingleHopCount",
        "The total number of times client request observed multiple hops",
        "entries", !largerIsBetter);
    m_stats[10] =
        factory->createIntCounter("metaDataRefreshCount",
                                  "The total number of times matadata is "
                                  "refreshed due to hoping observed",
                                  "entries", !largerIsBetter);
    m_stats[11] = factory->createIntCounter(
        "getAll", "The total number of cache getAll for this region", "entries",
        largerIsBetter);
    m_stats[12] = factory->createIntCounter(
        "putAll", "The total number of cache putAll for this region", "entries",
        largerIsBetter);
    m_stats[13] = factory->createLongCounter(
        "getTime", "Total time spent doing get operations for this region",
        "Nanoseconds", !largerIsBetter);
    m_stats[14] = factory->createLongCounter(
        "putTime", "Total time spent doing puts operations for this region",
        "Nanoseconds", !largerIsBetter);
    m_stats[15] = factory->createLongCounter(
        "putAllTime",
        "Total time spent doing putAlls operations for this region",
        "Nanoseconds", !largerIsBetter);
    m_stats[16] = factory->createLongCounter(
        "getAllTime",
        "Total time spent doing the getAlls operations for this region",
        "Nanoseconds", !largerIsBetter);

    m_stats[17] = factory->createIntCounter(
        "cacheLoaderCallsCompleted",
        "Total number of times a load has completed for this region", "entries",
        largerIsBetter);
    m_stats[18] = factory->createLongCounter(
        "cacheLoaderCallTIme",
        "Total time spent invoking the loaders for this region", "Nanoseconds",
        !largerIsBetter);
    m_stats[19] =
        factory->createIntCounter("cacheWriterCallsCompleted",
                                  "Total number of times a cache writer call "
                                  "has completed for this region",
                                  "entries", largerIsBetter);
    m_stats[20] = factory->createLongCounter(
        "cacheWriterCallTime", "Total time spent doing cache writer calls",
        "Nanoseconds", !largerIsBetter);
    m_stats[21] =
        factory->createIntCounter("cacheListenerCallsCompleted",
                                  "Total number of times a cache listener call "
                                  "has completed for this region",
                                  "entries", largerIsBetter);
    m_stats[22] = factory->createLongCounter(
        "cacheListenerCallTime",
        "Total time spent doing cache listener calls for this region",
        "Nanoseconds", !largerIsBetter);
    m_stats[23] =
        factory->createIntCounter("clears",
                                  "The total number of times a clear has been "
                                  "done on this cache for this region",
                                  "entries", !largerIsBetter);
    m_stats[24] = factory->createIntCounter(
        "removeAll", "The total number of cache removeAll for this region",
        "entries", largerIsBetter);
    m_stats[25] = factory->createLongCounter(
        "removeAllTime",
        "Total time spent doing removeAlls operations for this region",
        "Nanoseconds", !largerIsBetter);
    statsType = factory->createType(statsName, statsDesc, m_stats, 26);
  }

  m_destroysId = statsType->nameToId("destroys");
  m_createsId = statsType->nameToId("creates");
  m_putsId = statsType->nameToId("puts");
  m_putTimeId = statsType->nameToId("putTime");
  m_putAllId = statsType->nameToId("putAll");
  m_putAllTimeId = statsType->nameToId("putAllTime");
  m_removeAllId = statsType->nameToId("removeAll");
  m_removeAllTimeId = statsType->nameToId("removeAllTime");
  m_getsId = statsType->nameToId("gets");
  m_getTimeId = statsType->nameToId("getTime");
  m_getAllId = statsType->nameToId("getAll");
  m_getAllTimeId = statsType->nameToId("getAllTime");
  m_hitsId = statsType->nameToId("hits");
  m_missesId = statsType->nameToId("misses");
  m_entriesId = statsType->nameToId("entries");
  m_overflowsId = statsType->nameToId("overflows");
  m_retrievesId = statsType->nameToId("retrieves");
  m_nonSingleHopId = statsType->nameToId("nonSingleHopCount");
  m_metaDataRefreshId = statsType->nameToId("metaDataRefreshCount");
  m_LoaderCallsCompletedId = statsType->nameToId("cacheLoaderCallsCompleted");
  m_LoaderCallTimeId = statsType->nameToId("cacheLoaderCallTIme");
  m_WriterCallsCompletedId = statsType->nameToId("cacheWriterCallsCompleted");
  m_WriterCallTimeId = statsType->nameToId("cacheWriterCallTime");
  m_ListenerCallsCompletedId =
      statsType->nameToId("cacheListenerCallsCompleted");
  m_ListenerCallTimeId = statsType->nameToId("cacheListenerCallTime");
  m_clearsId = statsType->nameToId("clears");

  return statsType;
}

RegionStatType* RegionStatType::getInstance() {
  SpinLockGuard guard(m_singletonLock);
  if (single == NULL) {
    single = new RegionStatType();
  }
  return single;
}

RegionStatType::RegionStatType() {}

////////////////////////////////////////////////////////////////////////////////

// typedef ACE_Singleton<RegionStatsInit, ACE_Thread_Mutex> TheRegionStatsInit;

////////////////////////////////////////////////////////////////////////////////

RegionStats::RegionStats(const char* regionName) {
  RegionStatType* regStatType = RegionStatType::getInstance();

  StatisticsType* statsType = regStatType->getStatType();

  GF_D_ASSERT(statsType != NULL);

  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();

  m_regionStats =
      factory->createAtomicStatistics(statsType, const_cast<char*>(regionName));

  m_destroysId = regStatType->getDestroysId();
  m_createsId = regStatType->getCreatesId();
  m_putsId = regStatType->getPutsId();
  m_putTimeId = regStatType->getPutTimeId();
  m_getsId = regStatType->getGetsId();
  m_getTimeId = regStatType->getGetTimeId();
  m_getAllId = regStatType->getGetAllId();
  m_getAllTimeId = regStatType->getGetAllTimeId();
  m_putAllId = regStatType->getPutAllId();
  m_putAllTimeId = regStatType->getPutAllTimeId();
  m_removeAllId = regStatType->getRemoveAllId();
  m_removeAllTimeId = regStatType->getRemoveAllTimeId();
  m_hitsId = regStatType->getHitsId();
  m_missesId = regStatType->getMissesId();
  m_entriesId = regStatType->getEntriesId();
  m_overflowsId = regStatType->getOverflowsId();
  m_retrievesId = regStatType->getRetrievesId();
  m_nonSingleHopId = regStatType->getNonSingleHopCount();
  m_metaDataRefreshId = regStatType->getMetaDataRefreshCount();
  m_LoaderCallsCompletedId = regStatType->getLoaderCallsCompletedId();
  m_LoaderCallTimeId = regStatType->getLoaderCallTimeId();
  m_WriterCallsCompletedId = regStatType->getWriterCallsCompletedId();
  m_WriterCallTimeId = regStatType->getWriterCallTimeId();
  m_ListenerCallsCompletedId = regStatType->getListenerCallsCompletedId();
  m_ListenerCallTimeId = regStatType->getListenerCallTimeId();
  m_clearsId = regStatType->getClearsId();

  m_regionStats->setInt(m_destroysId, 0);
  m_regionStats->setInt(m_createsId, 0);
  m_regionStats->setInt(m_putsId, 0);
  m_regionStats->setInt(m_putTimeId, 0);
  m_regionStats->setInt(m_getsId, 0);
  m_regionStats->setInt(m_getTimeId, 0);
  m_regionStats->setInt(m_getAllId, 0);
  m_regionStats->setInt(m_getAllTimeId, 0);
  m_regionStats->setInt(m_putAllId, 0);
  m_regionStats->setInt(m_putAllTimeId, 0);
  m_regionStats->setInt(m_removeAllId, 0);
  m_regionStats->setInt(m_removeAllTimeId, 0);
  m_regionStats->setInt(m_hitsId, 0);
  m_regionStats->setInt(m_missesId, 0);
  m_regionStats->setInt(m_entriesId, 0);
  m_regionStats->setInt(m_overflowsId, 0);
  m_regionStats->setInt(m_retrievesId, 0);
  m_regionStats->setInt(m_nonSingleHopId, 0);
  m_regionStats->setInt(m_metaDataRefreshId, 0);
  m_regionStats->setInt(m_LoaderCallsCompletedId, 0);
  m_regionStats->setInt(m_LoaderCallTimeId, 0);
  m_regionStats->setInt(m_WriterCallsCompletedId, 0);
  m_regionStats->setInt(m_WriterCallTimeId, 0);
  m_regionStats->setInt(m_ListenerCallsCompletedId, 0);
  m_regionStats->setInt(m_ListenerCallTimeId, 0);
  m_regionStats->setInt(m_clearsId, 0);
}

RegionStats::~RegionStats() {
  if (m_regionStats != NULL) {
    // Don't Delete, Already closed, Just set NULL
    // delete m_regionStats;
    m_regionStats = NULL;
  }
}

}  // namespace gemfire
