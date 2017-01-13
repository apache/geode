/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef __GEMFIRE_POOLSTATS_H__
#define __GEMFIRE_POOLSTATS_H__ 1

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include <statistics/StatisticsManager.hpp>
#include "SpinLock.hpp"

namespace gemfire {

using namespace gemfire_statistics;

class PoolStats {
 public:
  /** hold statistics for a pool.. */
  PoolStats(const char* poolName);

  /** disable stat collection for this item. */
  virtual ~PoolStats();

  void close() {
    getStats()->close();
    StatisticsManager::getExistingInstance()->forceSample();
  }

  void setLocators(int32 curVal) { getStats()->setInt(m_locatorsId, curVal); }

  void setServers(int32 curVal) { getStats()->setInt(m_serversId, curVal); }

  void setSubsServers(int32 curVal) {
    getStats()->setInt(m_subsServsId, curVal);
  }

  void incLoctorRequests() { getStats()->incLong(m_locReqsId, 1); }

  void incLoctorResposes() { getStats()->incLong(m_locRespsId, 1); }

  void setCurPoolConnections(int32 curVal) {
    getStats()->setInt(m_poolConnsId, curVal);
  }

  void incPoolConnects() { getStats()->incInt(m_connectsId, 1); }

  void incPoolDisconnects() { getStats()->incInt(m_disconnectsId, 1); }

  void incPoolDisconnects(int numConn) {
    getStats()->incInt(m_disconnectsId, numConn);
  }

  void incMinPoolSizeConnects() { getStats()->incInt(m_minPoolConnectsId, 1); }

  void incLoadCondConnects() { getStats()->incInt(m_loadCondConnectsId, 1); }

  void incIdleDisconnects() { getStats()->incInt(m_idleDisconnectsId, 1); }

  void incLoadCondDisconnects() {
    getStats()->incInt(m_loadCondDisconnectsId, 1);
  }

  void setCurWaitingConnections(int32 curVal) {
    getStats()->setInt(m_waitingConnectionsId, curVal);
  }

  void incWaitingConnections() { getStats()->incInt(m_totalWaitingConnsId, 1); }

  void setCurClientOps(int32 curVal) {
    getStats()->setInt(m_curClientOpsId, curVal);
  }

  void incSucceedClientOps() { getStats()->incInt(m_clientOpsSuccessId, 1); }

  void incFailedClientOps() { getStats()->incInt(m_clientOpsFailedId, 1); }

  void incTimeoutClientOps() { getStats()->incInt(m_clientOpsTimeoutId, 1); }

  void incReceivedBytes(int64 value) {  // counter
    getStats()->incLong(m_receivedBytesId, value);
  }

  void incMessageBeingReceived() {  // counter
    getStats()->incLong(m_messagesBeingReceivedId, 1);
  }

  void incProcessedDeltaMessages() {  // counter
    getStats()->incLong(m_processedDeltaMessagesId, 1);
  }
  void incDeltaMessageFailures() {  // counter
    getStats()->incLong(m_deltaMessageFailuresId, 1);
  }
  void incProcessedDeltaMessagesTime(int64 value) {  // counter
    getStats()->incLong(m_processedDeltaMessagesTimeId, value);
  }

  void incTotalWaitingConnTime(int64 value) {  // counter
    getStats()->incLong(m_totalWaitingConnTimeId, value);
  }
  void incClientOpsSuccessTime(int64 value) {  // counter
    getStats()->incLong(m_clientOpsSuccessTimeId, value);
  }
  void incQueryExecutionId() {  // counter
    getStats()->incInt(m_queryExecutionsId, 1);
  }
  void incQueryExecutionTimeId(int64 value) {  // counter
    getStats()->incLong(m_queryExecutionTimeId, value);
  }
  inline gemfire_statistics::Statistics* getStats() { return m_poolStats; }

 private:
  // volatile gemfire_statistics::Statistics* m_poolStats;
  gemfire_statistics::Statistics* m_poolStats;

  int32_t m_locatorsId;
  int32_t m_serversId;
  int32_t m_subsServsId;
  int32_t m_locReqsId;
  int32_t m_locRespsId;
  int32_t m_poolConnsId;
  int32_t m_connectsId;
  int32_t m_disconnectsId;
  int32_t m_minPoolConnectsId;
  int32_t m_loadCondConnectsId;
  int32_t m_idleDisconnectsId;
  int32_t m_loadCondDisconnectsId;
  int32_t m_waitingConnectionsId;
  int32_t m_totalWaitingConnsId;
  int32_t m_totalWaitingConnTimeId;
  int32_t m_curClientOpsId;
  int32_t m_clientOpsSuccessId;
  int32_t m_clientOpsSuccessTimeId;
  int32_t m_clientOpsFailedId;
  int32_t m_clientOpsTimeoutId;
  int32_t m_receivedBytesId;
  int32_t m_messagesBeingReceivedId;
  int32_t m_processedDeltaMessagesId;
  int32_t m_deltaMessageFailuresId;
  int32_t m_processedDeltaMessagesTimeId;
  int32_t m_queryExecutionsId;
  int32_t m_queryExecutionTimeId;
};

class PoolStatType {
 private:
  static PoolStatType* single;
  static SpinLock m_singletonLock;
  static SpinLock m_statTypeLock;

 public:
  static PoolStatType* getInstance();

  StatisticsType* getStatType();

  static void clean();

 private:
  PoolStatType();
  StatisticDescriptor* m_stats[27];

  int32_t m_locatorsId;
  int32_t m_serversId;
  int32_t m_subsServsId;
  int32_t m_locReqsId;
  int32_t m_locRespsId;
  int32_t m_poolConnsId;
  int32_t m_connectsId;
  int32_t m_disconnectsId;
  int32_t m_minPoolConnectsId;
  int32_t m_loadCondConnectsId;
  int32_t m_idleDisconnectsId;
  int32_t m_loadCondDisconnectsId;
  int32_t m_waitingConnectionsId;
  int32_t m_totalWaitingConnsId;
  int32_t m_totalWaitingConnTimeId;
  int32_t m_curClientOpsId;
  int32_t m_clientOpsSuccessId;
  int32_t m_clientOpsSuccessTimeId;
  int32_t m_clientOpsFailedId;
  int32_t m_clientOpsTimeoutId;
  int32_t m_receivedBytesId;
  int32_t m_messagesBeingReceivedId;
  int32_t m_processedDeltaMessagesId;
  int32_t m_deltaMessageFailuresId;
  int32_t m_processedDeltaMessagesTimeId;
  int32_t m_queryExecutionsId;
  int32_t m_queryExecutionTimeId;

 public:
  int32_t getLocatorsId() { return m_locatorsId; }

  int32_t getServersId() { return m_serversId; }

  int32_t getSubscriptionServersId() { return m_subsServsId; }

  int32_t getLocatorRequestsId() { return m_locReqsId; }

  int32_t getLocatorResposesId() { return m_locRespsId; }

  int32_t getPoolConnectionsId() { return m_poolConnsId; }

  int32_t getConnectsId() { return m_connectsId; }

  int32_t getDisconnectsId() { return m_disconnectsId; }

  int32_t getMinPoolSizeConnectsId() { return m_minPoolConnectsId; }

  int32_t getLoadCondConnectsId() { return m_loadCondConnectsId; }
  int32_t getIdleDisconnectsId() { return m_idleDisconnectsId; }
  int32_t getLoadCondDisconnectsId() { return m_loadCondDisconnectsId; }
  int32_t getWaitingConnectionsId() { return m_waitingConnectionsId; }
  int32_t getTotalWaitingConnsId() { return m_totalWaitingConnsId; }
  int32_t getTotalWaitingConnTimeId() { return m_totalWaitingConnTimeId; }
  int32_t getCurClientOpsId() { return m_curClientOpsId; }
  int32_t getClientOpsSucceededId() { return m_clientOpsSuccessId; }
  int32_t getClientOpsSucceededTimeId() { return m_clientOpsSuccessTimeId; }
  int32_t getClientOpsFailedId() { return m_clientOpsFailedId; }
  int32_t getClientOpsTimeoutId() { return m_clientOpsTimeoutId; }
  int32_t getReceivedBytesId() { return m_receivedBytesId; }
  int32_t getMessagesBeingReceivedId() { return m_messagesBeingReceivedId; }
  int32_t getProcessedDeltaMessagesId() { return m_processedDeltaMessagesId; }
  int32_t getDeltaMessageFailuresId() { return m_deltaMessageFailuresId; }
  int32_t getProcessedDeltaMessagesTimeId() {
    return m_processedDeltaMessagesTimeId;
  }
  int32_t getQueryExecutionId() { return m_queryExecutionsId; }
  int32_t getQueryExecutionTimeId() { return m_queryExecutionTimeId; }
};
}

#endif  // __GEMFIRE_POOLSTATS_H__
