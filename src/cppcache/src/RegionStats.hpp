/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef __GEMFIRE_REGIONSTATS_H__
#define __GEMFIRE_REGIONSTATS_H__ 1

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include "SpinLock.hpp"
//#include "NanoTimer.hpp"
//#include <SystemProperties.hpp>
//#include <../DistributedSystem.hpp>

namespace gemfire {

using namespace gemfire_statistics;

class CPPCACHE_EXPORT RegionStats {
 public:
  /** hold statistics for a region.. */
  RegionStats(const char* regionName);

  /** disable stat collection for this item. */
  virtual ~RegionStats();

  void close() { m_regionStats->close(); }

  inline void incDestroys() { m_regionStats->incInt(m_destroysId, 1); }

  inline void incCreates() { m_regionStats->incInt(m_createsId, 1); }

  inline void incPuts() { m_regionStats->incInt(m_putsId, 1); }

  inline void incGets() { m_regionStats->incInt(m_getsId, 1); }

  inline void incGetAll() { m_regionStats->incInt(m_getAllId, 1); }

  inline void incPutAll() { m_regionStats->incInt(m_putAllId, 1); }

  inline void incRemoveAll() { m_regionStats->incInt(m_removeAllId, 1); }

  inline void incHits() { m_regionStats->incInt(m_hitsId, 1); }

  inline void incMisses() { m_regionStats->incInt(m_missesId, 1); }

  inline void incOverflows() { m_regionStats->incInt(m_overflowsId, 1); }

  inline void incRetrieves() { m_regionStats->incInt(m_retrievesId, 1); }

  inline void incNonSingleHopCount() {
    m_regionStats->incInt(m_nonSingleHopId, 1);
  }

  inline void incMetaDataRefreshCount() {
    m_regionStats->incInt(m_metaDataRefreshId, 1);
  }

  inline void setEntries(int32_t entries) {
    m_regionStats->setInt(m_entriesId, entries);
  }

  inline void incLoaderCallsCompleted() {
    m_regionStats->incInt(m_LoaderCallsCompletedId, 1);
  }

  inline void incWriterCallsCompleted() {
    m_regionStats->incInt(m_WriterCallsCompletedId, 1);
  }

  inline void incListenerCallsCompleted() {
    m_regionStats->incInt(m_ListenerCallsCompletedId, 1);
  }

  inline void incClears() { m_regionStats->incInt(m_clearsId, 1); }

  inline gemfire_statistics::Statistics* getStat() { return m_regionStats; }

 private:
  gemfire_statistics::Statistics* m_regionStats;

  int32_t m_destroysId;
  int32_t m_createsId;
  int32_t m_putsId;
  int32_t m_putTimeId;
  int32_t m_putAllId;
  int32_t m_putAllTimeId;
  int32_t m_removeAllId;
  int32_t m_removeAllTimeId;
  int32_t m_getsId;
  int32_t m_getTimeId;
  int32_t m_getAllId;
  int32_t m_getAllTimeId;
  int32_t m_hitsId;
  int32_t m_missesId;
  int32_t m_entriesId;
  int32_t m_overflowsId;
  int32_t m_retrievesId;
  int32_t m_nonSingleHopId;
  int32_t m_metaDataRefreshId;
  int32_t m_LoaderCallsCompletedId;
  int32_t m_LoaderCallTimeId;
  int32_t m_WriterCallsCompletedId;
  int32_t m_WriterCallTimeId;
  int32_t m_ListenerCallsCompletedId;
  int32_t m_ListenerCallTimeId;
  int32_t m_clearsId;
};

class RegionStatType {
 private:
  static int8 instanceFlag;
  static RegionStatType* single;
  static SpinLock m_singletonLock;
  static SpinLock m_statTypeLock;

 public:
  static RegionStatType* getInstance();

  StatisticsType* getStatType();

  static void clean();

 private:
  RegionStatType();
  StatisticDescriptor* m_stats[26];

  int32_t m_destroysId;
  int32_t m_createsId;
  int32_t m_putsId;
  int32_t m_putTimeId;
  int32_t m_putAllId;
  int32_t m_putAllTimeId;
  int32_t m_removeAllId;
  int32_t m_removeAllTimeId;
  int32_t m_getsId;
  int32_t m_getTimeId;
  int32_t m_getAllId;
  int32_t m_getAllTimeId;
  int32_t m_hitsId;
  int32_t m_missesId;
  int32_t m_entriesId;
  int32_t m_overflowsId;
  int32_t m_retrievesId;
  int32_t m_nonSingleHopId;
  int32_t m_metaDataRefreshId;
  int32_t m_LoaderCallsCompletedId;
  int32_t m_LoaderCallTimeId;
  int32_t m_WriterCallsCompletedId;
  int32_t m_WriterCallTimeId;
  int32_t m_ListenerCallsCompletedId;
  int32_t m_ListenerCallTimeId;
  int32_t m_clearsId;

 public:
  inline int32_t getDestroysId() { return m_destroysId; }

  inline int32_t getCreatesId() { return m_createsId; }

  inline int32_t getPutsId() { return m_putsId; }

  inline int32_t getPutTimeId() { return m_putTimeId; }

  inline int32_t getPutAllId() { return m_putAllId; }

  inline int32_t getPutAllTimeId() { return m_putAllTimeId; }

  inline int32_t getRemoveAllId() { return m_removeAllId; }

  inline int32_t getRemoveAllTimeId() { return m_removeAllTimeId; }

  inline int32_t getGetsId() { return m_getsId; }

  inline int32_t getGetTimeId() { return m_getTimeId; }

  inline int32_t getGetAllId() { return m_getAllId; }

  inline int32_t getGetAllTimeId() { return m_getAllTimeId; }

  inline int32_t getHitsId() { return m_hitsId; }

  inline int32_t getMissesId() { return m_missesId; }

  inline int32_t getEntriesId() { return m_entriesId; }

  inline int32_t getOverflowsId() { return m_overflowsId; }

  inline int32_t getRetrievesId() { return m_retrievesId; }

  inline int32_t getNonSingleHopCount() { return m_nonSingleHopId; }

  inline int32_t getMetaDataRefreshCount() { return m_metaDataRefreshId; }

  inline int32_t getLoaderCallsCompletedId() {
    return m_LoaderCallsCompletedId;
  }

  inline int32_t getLoaderCallTimeId() { return m_LoaderCallTimeId; }

  inline int32_t getWriterCallsCompletedId() {
    return m_WriterCallsCompletedId;
  }

  inline int32_t getWriterCallTimeId() { return m_WriterCallTimeId; }

  inline int32_t getListenerCallsCompletedId() {
    return m_ListenerCallsCompletedId;
  }

  inline int32_t getListenerCallTimeId() { return m_ListenerCallTimeId; }

  inline int32_t getClearsId() { return m_clearsId; }
};
}

#endif  // __GEMFIRE_REGIONSTATS_H__
