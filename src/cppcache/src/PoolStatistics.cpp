/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include "PoolStatistics.hpp"
//#include "StatisticsFactory.hpp"

#include <ace/Thread_Mutex.h>
#include <ace/Singleton.h>

////////////////////////////////////////////////////////////////////////////////

namespace gemfire {

using namespace gemfire_statistics;

////////////////////////////////////////////////////////////////////////////////

PoolStatType* PoolStatType::single = NULL;
SpinLock PoolStatType::m_singletonLock;
SpinLock PoolStatType::m_statTypeLock;

void PoolStatType::clean() {
  SpinLockGuard guard(m_singletonLock);
  if (single != NULL) {
    delete single;
    single = NULL;
  }
}

StatisticsType* PoolStatType::getStatType() {
  SpinLockGuard guard(m_statTypeLock);
  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
  GF_D_ASSERT(!!factory);

  StatisticsType* statsType = factory->findType("PoolStatistics");

  if (statsType == NULL) {
    m_stats[0] = factory->createIntGauge(
        "locators", "Current number of locators discovered", "locators");
    m_stats[1] = factory->createIntGauge(
        "servers", "Current number of servers discovered", "servers");
    m_stats[2] = factory->createIntGauge(
        "subscriptionServers",
        "Number of servers hosting this clients subscriptions", "servers");
    m_stats[3] = factory->createLongCounter(
        "locatorRequests",
        "Number of requests from this connection pool to a locator",
        "requests");
    m_stats[4] = factory->createLongCounter(
        "locatorResponses",
        "Number of responses from the locator to this connection pool",
        "responses");
    m_stats[5] = factory->createIntGauge(
        "poolConnections", "Current number of pool connections", "connections");
    m_stats[6] = factory->createIntCounter(
        "connects", "Total number of times a connection has been created.",
        "connects");
    m_stats[7] = factory->createIntCounter(
        "disconnects", "Total number of times a connection has been destroyed.",
        "disconnects");
    m_stats[8] = factory->createIntCounter(
        "minPoolSizeConnects",
        "Total number of connects done to maintain minimum pool size.",
        "connects");
    m_stats[9] = factory->createIntCounter(
        "loadConditioningConnects",
        "Total number of connects done due to load conditioning.", "connects");
    m_stats[10] = factory->createIntCounter(
        "idleDisconnects",
        "Total number of disconnects done due to idle expiration.",
        "disconnects");
    m_stats[11] = factory->createIntCounter(
        "loadConditioningDisconnects",
        "Total number of disconnects done due to load conditioning expiration.",
        "disconnects");
    m_stats[12] = factory->createIntGauge(
        "connectionWaitsInProgress",
        "Current number of threads waiting for a connection", "threads");
    m_stats[13] = factory->createIntCounter(
        "connectionWaits",
        "Total number of times a thread completed waiting for a connection (by "
        "timing out or by getting a connection).",
        "waits");
    m_stats[14] = factory->createLongCounter(
        "connectionWaitTime",
        "Total time (nanoseconds) spent waiting for a connection.",
        "nanoseconds");
    m_stats[15] = factory->createIntGauge(
        "clientOpsInProgress", "Current number of clientOps being executed",
        "clientOps");
    m_stats[16] = factory->createIntCounter(
        "clientOps", "Total number of clientOps completed successfully",
        "clientOps");
    m_stats[17] = factory->createLongCounter(
        "clientOpTime",
        "Total amount of time, in nanoseconds spent doing clientOps",
        "nanoseconds");
    m_stats[18] = factory->createIntCounter(
        "clientOpFailures",
        "Total number of clientOp attempts that have failed", "clientOps");
    m_stats[19] = factory->createIntCounter(
        "clientOpTimeouts",
        "Total number of clientOp attempts that have timed out", "clientOps");
    m_stats[20] = factory->createLongCounter(
        "receivedBytes", "Total number of bytes received from the server.",
        "bytes");
    m_stats[21] = factory->createLongCounter(
        "messagesBeingReceived",
        "Total number of message being received off the network.", "messages");
    m_stats[22] = factory->createLongCounter(
        "processedDeltaMessages",
        "Total number of delta message processed successfully", "messages");
    m_stats[23] = factory->createLongCounter(
        "deltaMessageFailures", "Total number of failures in processing delta",
        "messages");
    m_stats[24] = factory->createLongCounter(
        "processedDeltaMessagesTime", "Total time spent while processing Delta",
        "nanoseconds");
    m_stats[25] = factory->createIntCounter("queryExecutions",
                                            "Total number of queryExecutions",
                                            "queryExecutions");
    m_stats[26] = factory->createLongCounter(
        "queryExecutionTime",
        "Total time spent while processing queryExecution", "nanoseconds");

    statsType = factory->createType("PoolStatistics",
                                    "Statistics for this pool", m_stats, 27);

    m_locatorsId = statsType->nameToId("locators");
    m_serversId = statsType->nameToId("servers");
    m_subsServsId = statsType->nameToId("subscriptionServers");
    m_locReqsId = statsType->nameToId("locatorRequests");
    m_locRespsId = statsType->nameToId("locatorResponses");
    m_poolConnsId = statsType->nameToId("poolConnections");
    m_connectsId = statsType->nameToId("connects");
    m_disconnectsId = statsType->nameToId("disconnects");
    m_minPoolConnectsId = statsType->nameToId("minPoolSizeConnects");
    m_loadCondConnectsId = statsType->nameToId("loadConditioningConnects");
    m_idleDisconnectsId = statsType->nameToId("idleDisconnects");
    m_loadCondDisconnectsId =
        statsType->nameToId("loadConditioningDisconnects");
    m_waitingConnectionsId = statsType->nameToId("connectionWaitsInProgress");
    m_totalWaitingConnsId = statsType->nameToId("connectionWaits");
    m_totalWaitingConnTimeId = statsType->nameToId("connectionWaitTime");
    m_curClientOpsId = statsType->nameToId("clientOpsInProgress");
    m_clientOpsSuccessId = statsType->nameToId("clientOps");
    m_clientOpsSuccessTimeId = statsType->nameToId("clientOpTime");
    m_clientOpsFailedId = statsType->nameToId("clientOpFailures");
    m_clientOpsTimeoutId = statsType->nameToId("clientOpTimeouts");
    m_receivedBytesId = statsType->nameToId("receivedBytes");
    m_messagesBeingReceivedId = statsType->nameToId("messagesBeingReceived");
    m_processedDeltaMessagesId = statsType->nameToId("processedDeltaMessages");
    m_deltaMessageFailuresId = statsType->nameToId("deltaMessageFailures");
    m_processedDeltaMessagesTimeId =
        statsType->nameToId("processedDeltaMessagesTime");
    m_queryExecutionsId = statsType->nameToId("queryExecutions");
    m_queryExecutionTimeId = statsType->nameToId("queryExecutionTime");
  }

  return statsType;
}

PoolStatType* PoolStatType::getInstance() {
  SpinLockGuard guard(m_singletonLock);
  if (single == NULL) {
    single = new PoolStatType();
  }
  return single;
}

PoolStatType::PoolStatType()
    : m_locatorsId(0),
      m_serversId(0),
      m_subsServsId(0),
      m_locReqsId(0),
      m_locRespsId(0),
      m_poolConnsId(0),
      m_connectsId(0),
      m_disconnectsId(0),
      m_minPoolConnectsId(0),
      m_loadCondConnectsId(0),
      m_idleDisconnectsId(0),
      m_loadCondDisconnectsId(0),
      m_waitingConnectionsId(0),
      m_totalWaitingConnsId(0),
      m_totalWaitingConnTimeId(0),
      m_curClientOpsId(0),
      m_clientOpsSuccessId(0),
      m_clientOpsSuccessTimeId(0),
      m_clientOpsFailedId(0),
      m_clientOpsTimeoutId(0),
      m_receivedBytesId(0),
      m_messagesBeingReceivedId(0),
      m_processedDeltaMessagesId(0),
      m_deltaMessageFailuresId(0),
      m_processedDeltaMessagesTimeId(0),
      m_queryExecutionsId(0),
      m_queryExecutionTimeId(0) {
  memset(m_stats, 0, sizeof(m_stats));
}

////////////////////////////////////////////////////////////////////////////////

PoolStats::PoolStats(const char* poolName) {
  PoolStatType* poolStatType = PoolStatType::getInstance();

  StatisticsType* statsType = poolStatType->getStatType();

  GF_D_ASSERT(statsType != NULL);

  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();

  m_poolStats = factory->createAtomicStatistics(statsType, poolName);

  m_locatorsId = poolStatType->getLocatorsId();
  m_serversId = poolStatType->getServersId();
  m_subsServsId = poolStatType->getSubscriptionServersId();
  m_locReqsId = poolStatType->getLocatorRequestsId();
  m_locRespsId = poolStatType->getLocatorResposesId();
  m_poolConnsId = poolStatType->getPoolConnectionsId();
  m_connectsId = poolStatType->getConnectsId();
  m_disconnectsId = poolStatType->getDisconnectsId();
  m_minPoolConnectsId = poolStatType->getMinPoolSizeConnectsId();
  m_loadCondConnectsId = poolStatType->getLoadCondConnectsId();
  m_idleDisconnectsId = poolStatType->getIdleDisconnectsId();
  m_loadCondDisconnectsId = poolStatType->getLoadCondDisconnectsId();
  m_waitingConnectionsId = poolStatType->getWaitingConnectionsId();
  m_totalWaitingConnsId = poolStatType->getTotalWaitingConnsId();
  m_totalWaitingConnTimeId = poolStatType->getTotalWaitingConnTimeId();
  m_curClientOpsId = poolStatType->getCurClientOpsId();
  m_clientOpsSuccessId = poolStatType->getClientOpsSucceededId();
  m_clientOpsSuccessTimeId = poolStatType->getClientOpsSucceededTimeId();
  m_clientOpsFailedId = poolStatType->getClientOpsFailedId();
  m_clientOpsTimeoutId = poolStatType->getClientOpsTimeoutId();
  m_receivedBytesId = poolStatType->getReceivedBytesId();
  m_messagesBeingReceivedId = poolStatType->getMessagesBeingReceivedId();
  m_processedDeltaMessagesId = poolStatType->getProcessedDeltaMessagesId();
  m_deltaMessageFailuresId = poolStatType->getDeltaMessageFailuresId();
  m_processedDeltaMessagesTimeId =
      poolStatType->getProcessedDeltaMessagesTimeId();
  m_queryExecutionsId = poolStatType->getQueryExecutionId();
  m_queryExecutionTimeId = poolStatType->getQueryExecutionTimeId();
  getStats()->setInt(m_locatorsId, 0);
  getStats()->setInt(m_serversId, 0);
  getStats()->setInt(m_subsServsId, 0);
  getStats()->setLong(m_locReqsId, 0);
  getStats()->setLong(m_locRespsId, 0);
  getStats()->setInt(m_poolConnsId, 0);
  getStats()->setInt(m_connectsId, 0);
  getStats()->setInt(m_disconnectsId, 0);
  getStats()->setInt(m_minPoolConnectsId, 0);
  getStats()->setInt(m_loadCondConnectsId, 0);
  getStats()->setInt(m_idleDisconnectsId, 0);
  getStats()->setInt(m_loadCondDisconnectsId, 0);
  getStats()->setInt(m_waitingConnectionsId, 0);
  getStats()->setInt(m_totalWaitingConnsId, 0);
  getStats()->setLong(m_totalWaitingConnTimeId, 0);
  getStats()->setInt(m_curClientOpsId, 0);
  getStats()->setInt(m_clientOpsSuccessId, 0);
  getStats()->setLong(m_clientOpsSuccessTimeId, 0);
  getStats()->setInt(m_clientOpsFailedId, 0);
  getStats()->setInt(m_clientOpsTimeoutId, 0);
  getStats()->setInt(m_receivedBytesId, 0);
  getStats()->setInt(m_messagesBeingReceivedId, 0);
  getStats()->setInt(m_processedDeltaMessagesId, 0);
  getStats()->setInt(m_deltaMessageFailuresId, 0);
  getStats()->setInt(m_processedDeltaMessagesTimeId, 0);
  getStats()->setInt(m_queryExecutionsId, 0);
  getStats()->setLong(m_queryExecutionTimeId, 0);

  StatisticsManager::getExistingInstance()->forceSample();
}

PoolStats::~PoolStats() {
  if (m_poolStats != NULL) {
    m_poolStats = NULL;
  }
}

}  // namespace gemfire
