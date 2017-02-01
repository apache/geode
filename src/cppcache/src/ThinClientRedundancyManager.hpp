#pragma once

#ifndef GEODE_THINCLIENTREDUNDANCYMANAGER_H_
#define GEODE_THINCLIENTREDUNDANCYMANAGER_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * ThinClientRedundancyManager.hpp
 *
 *  Created on: Dec 1, 2008
 *      Author: abhaware
 */


#include "TcrMessage.hpp"
#include "TcrEndpoint.hpp"
#include "ServerLocation.hpp"

#include <set>
#include <list>
#include <string>

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_THINCLIENTREDUNDANCYMANAGER_H_
