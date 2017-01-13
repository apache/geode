/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_CQ_QUERY_STATS_H__
#define __GEMFIRE_CQ_QUERY_STATS_H__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include "SpinLock.hpp"

#include <gfcpp/CqStatistics.hpp>

namespace gemfire {

using namespace gemfire_statistics;

class CPPCACHE_EXPORT CqQueryVsdStats : public CqStatistics {
 public:
  /** hold statistics for a cq. */
  CqQueryVsdStats(const char* cqName);

  /** disable stat collection for this item. */
  virtual ~CqQueryVsdStats();

  void close() { m_cqQueryVsdStats->close(); }

  inline void incNumInserts() { m_cqQueryVsdStats->incInt(m_numInsertsId, 1); }

  inline void incNumUpdates() { m_cqQueryVsdStats->incInt(m_numUpdatesId, 1); }

  inline void incNumDeletes() { m_cqQueryVsdStats->incInt(m_numDeletesId, 1); }

  inline void incNumEvents() { m_cqQueryVsdStats->incInt(m_numEventsId, 1); }

  inline uint32_t numInserts() const {
    return m_cqQueryVsdStats->getInt(m_numInsertsId);
  }
  inline uint32_t numUpdates() const {
    return m_cqQueryVsdStats->getInt(m_numUpdatesId);
  }
  inline uint32_t numDeletes() const {
    return m_cqQueryVsdStats->getInt(m_numDeletesId);
  }
  inline uint32_t numEvents() const {
    return m_cqQueryVsdStats->getInt(m_numEventsId);
  }

 private:
  gemfire_statistics::Statistics* m_cqQueryVsdStats;

  int32_t m_numInsertsId;
  int32_t m_numUpdatesId;
  int32_t m_numDeletesId;
  int32_t m_numEventsId;
};

class CqQueryStatType {
 private:
  static int8 instanceFlag;
  static CqQueryStatType* single;
  static SpinLock m_singletonLock;
  static SpinLock m_statTypeLock;

 public:
  static CqQueryStatType* getInstance();

  StatisticsType* getStatType();

  static void clean();

 private:
  CqQueryStatType();
  StatisticDescriptor* m_stats[4];

  int32_t m_numInsertsId;
  int32_t m_numUpdatesId;
  int32_t m_numDeletesId;
  int32_t m_numEventsId;

 public:
  inline int32_t getNumInsertsId() { return m_numInsertsId; }

  inline int32_t getNumUpdatesId() { return m_numUpdatesId; }

  inline int32_t getNumDeletesId() { return m_numDeletesId; }

  inline int32_t getNumEventsId() { return m_numEventsId; }
};
}

#endif  // __GEMFIRE_CQ_QUERY_STATS_H__
