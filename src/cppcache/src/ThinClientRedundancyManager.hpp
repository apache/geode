/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientRedundancyManager.hpp
 *
 *  Created on: Dec 1, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTREDUNDANCYMANAGER_HPP_
#define THINCLIENTREDUNDANCYMANAGER_HPP_

#include "TcrMessage.hpp"
#include "TcrEndpoint.hpp"
#include "ServerLocation.hpp"

#include <set>
#include <list>
#include <string>

namespace gemfire {

class TcrConnectionManager;
class TcrHADistributionManager;
class ThinClientRegion;
class ThinClientPoolHADM;

class ThinClientRedundancyManager {
 public:
  bool m_globalProcessedMarker;

  GfErrType maintainRedundancyLevel(bool init = false,
                                    const TcrMessage* request = NULL,
                                    TcrMessageReply* reply = NULL,
                                    ThinClientRegion* region = NULL);
  void initialize(int redundancyLevel);
  void close();
  void sendNotificationCloseMsgs();

  ThinClientRedundancyManager(TcrConnectionManager* theConnManager,
                              int redundencyLevel = 0,
                              ThinClientPoolHADM* poolHADM = NULL,
                              bool sentReadyForEvents = false,
                              bool globalProcessedMarker = false);
  GfErrType sendSyncRequestRegisterInterest(TcrMessage& request,
                                            TcrMessageReply& reply,
                                            bool attemptFailover,
                                            TcrEndpoint* endpoint,
                                            ThinClientBaseDM* theHADM,
                                            ThinClientRegion* region = NULL);

  GfErrType sendSyncRequestCq(TcrMessage& request, TcrMessageReply& reply,
                              ThinClientBaseDM* theHADM);
  void readyForEvents();
  void startPeriodicAck();
  bool checkDupAndAdd(EventIdPtr eventid);
  void netDown();
  void acquireRedundancyLock() { m_redundantEndpointsLock.acquire_read(); }
  void releaseRedundancyLock() { m_redundantEndpointsLock.release(); }
  volatile bool allEndPointDiscon() { return m_IsAllEpDisCon; }
  void removeCallbackConnection(TcrEndpoint*);

  ACE_Recursive_Thread_Mutex& getRedundancyLock() {
    return m_redundantEndpointsLock;
  }

  GfErrType sendRequestToPrimary(TcrMessage& request, TcrMessageReply& reply);
  bool isSentReadyForEvents() const { return m_sentReadyForEvents; }

 private:
  // for selectServers
  volatile bool m_IsAllEpDisCon;
  int m_server;
  bool m_sentReadyForEvents;
  int m_redundancyLevel;
  bool m_loggedRedundancyWarning;
  ThinClientPoolHADM* m_poolHADM;
  std::vector<TcrEndpoint*> m_redundantEndpoints;
  std::vector<TcrEndpoint*> m_nonredundantEndpoints;
  ACE_Recursive_Thread_Mutex m_redundantEndpointsLock;
  TcrConnectionManager* m_theTcrConnManager;
  CacheableStringArrayPtr m_locators;
  CacheableStringArrayPtr m_servers;

  void removeEndpointsInOrder(std::vector<TcrEndpoint*>& destVector,
                              const std::vector<TcrEndpoint*>& srcVector);
  void addEndpointsInOrder(std::vector<TcrEndpoint*>& destVector,
                           const std::vector<TcrEndpoint*>& srcVector);
  GfErrType makePrimary(TcrEndpoint* ep, const TcrMessage* request,
                        TcrMessageReply* reply);
  GfErrType makeSecondary(TcrEndpoint* ep, const TcrMessage* request,
                          TcrMessageReply* reply);
  bool sendMakePrimaryMesg(TcrEndpoint* ep, const TcrMessage* request,
                           ThinClientRegion* region);
  bool readyForEvents(TcrEndpoint* primaryCandidate);
  void moveEndpointToLast(std::vector<TcrEndpoint*>& epVector,
                          TcrEndpoint* targetEp);

  void getAllEndpoints(std::vector<TcrEndpoint*>& endpoints);
  // For 38196 Fix: Reorder End points.
  void insertEPInQueueSizeOrder(TcrEndpoint* ep,
                                std::vector<TcrEndpoint*>& endpoints);

  GfErrType createQueueEP(TcrEndpoint* ep, const TcrMessage* request,
                          TcrMessageReply* reply, bool isPrimary);
  GfErrType createPoolQueueEP(TcrEndpoint* ep, const TcrMessage* request,
                              TcrMessageReply* reply, bool isPrimary);

  inline bool isDurable();
  int processEventIdMap(const ACE_Time_Value&, const void*);
  GF_TASK_T<ThinClientRedundancyManager>* m_periodicAckTask;
  ACE_Semaphore m_periodicAckSema;
  long m_processEventIdMapTaskId;  // periodic check eventid map for notify ack
                                   // and/or expiry
  int periodicAck(volatile bool& isRunning);
  void doPeriodicAck();
  ACE_Time_Value m_nextAck;     // next ack time
  ACE_Time_Value m_nextAckInc;  // next ack time increment
  volatile bool m_HAenabled;
  EventIdMap m_eventidmap;

  std::list<ServerLocation> selectServers(int howMany,
                                          std::set<ServerLocation> exclEndPts);

  friend class TcrConnectionManager;
  static const char* NC_PerodicACK;
};
}

#endif /* THINCLIENTREDUNDANCYMANAGER_HPP_ */
