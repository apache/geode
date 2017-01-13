/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include "CqServiceVsdStats.hpp"
//#include "StatisticsFactory.hpp"

#include <ace/Thread_Mutex.h>
#include <ace/Singleton.h>

const char* cqServiceStatsName = (const char*)"CqServiceStatistics";
const char* cqServiceStatsDesc = (const char*)"Statistics for this cq Service";

////////////////////////////////////////////////////////////////////////////////

namespace gemfire {

using namespace gemfire_statistics;

////////////////////////////////////////////////////////////////////////////////

CqServiceStatType* CqServiceStatType::single = NULL;
SpinLock CqServiceStatType::m_singletonLock;
SpinLock CqServiceStatType::m_statTypeLock;

void CqServiceStatType::clean() {
  SpinLockGuard guard(m_singletonLock);
  if (single != NULL) {
    delete single;
    single = NULL;
  }
}

StatisticsType* CqServiceStatType::getStatType() {
  const bool largerIsBetter = true;
  SpinLockGuard guard(m_statTypeLock);
  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
  GF_D_ASSERT(!!factory);

  StatisticsType* statsType = factory->findType("CqServiceStatistics");

  if (statsType == NULL) {
    m_stats[0] = factory->createIntCounter(
        "CqsActive", "The total number of CqsActive this cq qurey", "entries",
        largerIsBetter);
    m_stats[1] = factory->createIntCounter(
        "CqsCreated", "The total number of CqsCreated for this cq Service",
        "entries", largerIsBetter);
    m_stats[2] = factory->createIntCounter(
        "CqsClosed", "The total number of CqsClosed for this cq Service",
        "entries", largerIsBetter);
    m_stats[3] = factory->createIntCounter(
        "CqsStopped", "The total number of CqsStopped for this cq Service",
        "entries", largerIsBetter);
    m_stats[4] = factory->createIntCounter(
        "CqsOnClient",
        "The total number of Cqs on the client for this cq Service", "entries",
        largerIsBetter);

    statsType =
        factory->createType(cqServiceStatsName, cqServiceStatsDesc, m_stats, 5);

    m_numCqsActiveId = statsType->nameToId("CqsActive");
    m_numCqsCreatedId = statsType->nameToId("CqsCreated");
    m_numCqsOnClientId = statsType->nameToId("CqsOnClient");
    m_numCqsClosedId = statsType->nameToId("CqsClosed");
    m_numCqsStoppedId = statsType->nameToId("CqsStopped");
  }

  return statsType;
}

CqServiceStatType* CqServiceStatType::getInstance() {
  SpinLockGuard guard(m_singletonLock);
  if (single == NULL) {
    single = new CqServiceStatType();
  }
  return single;
}

CqServiceStatType::CqServiceStatType()
    : /* adongre
       * CID 28932: Uninitialized scalar field (UNINIT_CTOR)
       */
      m_numCqsActiveId(0),
      m_numCqsCreatedId(0),
      m_numCqsOnClientId(0),
      m_numCqsClosedId(0),
      m_numCqsStoppedId(0) {
  memset(m_stats, 0, sizeof(m_stats));
}

////////////////////////////////////////////////////////////////////////////////

// typedef ACE_Singleton<CqServiceVsdStatsInit, ACE_Thread_Mutex>
// TheCqServiceVsdStatsInit;

////////////////////////////////////////////////////////////////////////////////

CqServiceVsdStats::CqServiceVsdStats(const char* cqServiceName) {
  CqServiceStatType* regStatType = CqServiceStatType::getInstance();

  StatisticsType* statsType = regStatType->getStatType();

  GF_D_ASSERT(statsType != NULL);

  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();

  m_cqServiceVsdStats = factory->createAtomicStatistics(
      statsType, const_cast<char*>(cqServiceName));

  m_numCqsActiveId = regStatType->getNumCqsActiveId();
  m_numCqsCreatedId = regStatType->getNumCqsCreatedId();
  m_numCqsOnClientId = regStatType->getNumCqsOnClientId();
  m_numCqsClosedId = regStatType->getNumCqsClosedId();
  m_numCqsStoppedId = regStatType->getNumCqsStoppedId();

  m_cqServiceVsdStats->setInt(m_numCqsActiveId, 0);
  m_cqServiceVsdStats->setInt(m_numCqsCreatedId, 0);
  m_cqServiceVsdStats->setInt(m_numCqsClosedId, 0);
  m_cqServiceVsdStats->setInt(m_numCqsStoppedId, 0);
}

CqServiceVsdStats::~CqServiceVsdStats() {
  if (m_cqServiceVsdStats != NULL) {
    // Don't Delete, Already closed, Just set NULL
    // delete m_CqServiceVsdStats;
    m_cqServiceVsdStats = NULL;
  }
}

}  // namespace gemfire
