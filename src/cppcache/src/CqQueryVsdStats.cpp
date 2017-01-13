/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include "CqQueryVsdStats.hpp"
//#include "StatisticsFactory.hpp"

#include <ace/Thread_Mutex.h>
#include <ace/Singleton.h>

const char* cqStatsName = (const char*)"CqQueryStatistics";
const char* cqStatsDesc = (const char*)"Statistics for this cq query";

////////////////////////////////////////////////////////////////////////////////

namespace gemfire {

using namespace gemfire_statistics;

////////////////////////////////////////////////////////////////////////////////

CqQueryStatType* CqQueryStatType::single = NULL;
SpinLock CqQueryStatType::m_singletonLock;
SpinLock CqQueryStatType::m_statTypeLock;

void CqQueryStatType::clean() {
  SpinLockGuard guard(m_singletonLock);
  if (single != NULL) {
    delete single;
    single = NULL;
  }
}

StatisticsType* CqQueryStatType::getStatType() {
  const bool largerIsBetter = true;
  SpinLockGuard guard(m_statTypeLock);
  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
  GF_D_ASSERT(!!factory);

  StatisticsType* statsType = factory->findType("CqQueryStatistics");

  if (statsType == NULL) {
    m_stats[0] = factory->createIntCounter(
        "inserts", "The total number of inserts this cq qurey", "entries",
        largerIsBetter);
    m_stats[1] = factory->createIntCounter(
        "updates", "The total number of updates for this cq query", "entries",
        largerIsBetter);
    m_stats[2] = factory->createIntCounter(
        "deletes", "The total number of deletes for this cq query", "entries",
        largerIsBetter);
    m_stats[3] = factory->createIntCounter(
        "events", "The total number of events for this cq query", "entries",
        largerIsBetter);

    statsType = factory->createType(cqStatsName, cqStatsDesc, m_stats, 4);

    m_numInsertsId = statsType->nameToId("inserts");
    m_numUpdatesId = statsType->nameToId("updates");
    m_numDeletesId = statsType->nameToId("deletes");
    m_numEventsId = statsType->nameToId("events");
  }

  return statsType;
}

CqQueryStatType* CqQueryStatType::getInstance() {
  SpinLockGuard guard(m_singletonLock);
  if (single == NULL) {
    single = new CqQueryStatType();
  }
  return single;
}

CqQueryStatType::CqQueryStatType()
    : /* adongre
       * CID 28931: Uninitialized scalar field (UNINIT_CTOR)
       */
      m_numInsertsId(0),
      m_numUpdatesId(0),
      m_numDeletesId(0),
      m_numEventsId(0) {
  memset(m_stats, 0, sizeof(m_stats));
}

////////////////////////////////////////////////////////////////////////////////

// typedef ACE_Singleton<CqQueryVsdStatsInit, ACE_Thread_Mutex>
// TheCqQueryVsdStatsInit;

////////////////////////////////////////////////////////////////////////////////

CqQueryVsdStats::CqQueryVsdStats(const char* cqqueryName) {
  CqQueryStatType* regStatType = CqQueryStatType::getInstance();

  StatisticsType* statsType = regStatType->getStatType();

  GF_D_ASSERT(statsType != NULL);

  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();

  m_cqQueryVsdStats = factory->createAtomicStatistics(
      statsType, const_cast<char*>(cqqueryName));

  m_numInsertsId = regStatType->getNumInsertsId();
  m_numUpdatesId = regStatType->getNumUpdatesId();
  m_numDeletesId = regStatType->getNumDeletesId();
  m_numEventsId = regStatType->getNumEventsId();

  m_cqQueryVsdStats->setInt(m_numInsertsId, 0);
  m_cqQueryVsdStats->setInt(m_numUpdatesId, 0);
  m_cqQueryVsdStats->setInt(m_numDeletesId, 0);
  m_cqQueryVsdStats->setInt(m_numEventsId, 0);
}

CqQueryVsdStats::~CqQueryVsdStats() {
  if (m_cqQueryVsdStats != NULL) {
    // Don't Delete, Already closed, Just set NULL
    // delete m_CqQueryVsdStats;
    m_cqQueryVsdStats = NULL;
  }
}

}  // namespace gemfire
