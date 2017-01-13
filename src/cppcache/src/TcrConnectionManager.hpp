/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TCR_CONNECTIONMANAGER_HPP__
#define __TCR_CONNECTIONMANAGER_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include "GF_TASK_T.hpp"
#include <string>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Map_Manager.h>
#include <ace/Semaphore.h>
#include <vector>
#include <unordered_map>
#include <list>
#include "ace/config-lite.h"
#include "ace/Versioned_Namespace.h"
#include "Queue.hpp"
#include "EventIdMap.hpp"
#include "ThinClientRedundancyManager.hpp"

ACE_BEGIN_VERSIONED_NAMESPACE_DECL
class ACE_Task_Base;
ACE_END_VERSIONED_NAMESPACE_DECL

namespace gemfire {
class TcrConnection;
class TcrEndpoint;
class TcrMessage;
class CacheImpl;
class ThinClientBaseDM;
class ThinClientRegion;

/**
* @brief transport data between caches
*/
class CPPCACHE_EXPORT TcrConnectionManager {
 public:
  TcrConnectionManager(CacheImpl* cache);
  ~TcrConnectionManager();
  void init(bool isPool = false);
  void startFailoverAndCleanupThreads(bool isPool = false);
  void connect(ThinClientBaseDM* distMng, std::vector<TcrEndpoint*>& endpoints,
               const std::unordered_set<std::string>& endpointStrs);
  void disconnect(ThinClientBaseDM* distMng,
                  std::vector<TcrEndpoint*>& endpoints,
                  bool keepEndpoints = false);
  int checkConnection(const ACE_Time_Value&, const void*);
  int checkRedundancy(const ACE_Time_Value&, const void*);
  int processEventIdMap(const ACE_Time_Value&, const void*);
  long getPingTaskId();
  void close();

  void readyForEvents();

  // added netDown() and revive() for tests simulation of client crash and
  // network drop
  void netDown();
  void revive();
  void setClientCrashTEST() { TEST_DURABLE_CLIENT_CRASH = true; }
  volatile static bool isNetDown;
  volatile static bool TEST_DURABLE_CLIENT_CRASH;

  inline ACE_Map_Manager<std::string, TcrEndpoint*, ACE_Recursive_Thread_Mutex>&
  getGlobalEndpoints() {
    return m_endpoints;
  }

  void getAllEndpoints(std::vector<TcrEndpoint*>& endpoints);
  int getNumEndPoints();

  GfErrType registerInterestAllRegions(TcrEndpoint* ep,
                                       const TcrMessage* request,
                                       TcrMessageReply* reply);
  GfErrType sendSyncRequestCq(TcrMessage& request, TcrMessageReply& reply);

  void addNotificationForDeletion(GF_TASK_T<TcrEndpoint>* notifyReceiver,
                                  TcrConnection* notifyConnection,
                                  ACE_Semaphore& notifyCleanupSema);

  void processMarker();

  bool getEndpointStatus(const std::string& endpoint);

  void addPoolEndpoints(TcrEndpoint* endpoint) {
    m_poolEndpointList.push_back(endpoint);
  }

  bool isDurable() { return m_isDurable; };
  bool haEnabled() { return m_redundancyManager->m_HAenabled; };
  CacheImpl* getCacheImpl() { return m_cache; };

  GfErrType sendSyncRequestCq(TcrMessage& request, TcrMessageReply& reply,
                              TcrHADistributionManager* theHADM);
  GfErrType sendSyncRequestRegisterInterest(
      TcrMessage& request, TcrMessageReply& reply, bool attemptFailover = true,
      TcrEndpoint* endpoint = NULL, TcrHADistributionManager* theHADM = NULL,
      ThinClientRegion* region = NULL);

  inline void triggerRedundancyThread() { m_redundancySema.release(); }

  inline void acquireRedundancyLock() {
    m_redundancyManager->acquireRedundancyLock();
    m_distMngrsLock.acquire_read();
  }

  inline void releaseRedundancyLock() {
    m_redundancyManager->releaseRedundancyLock();
    m_distMngrsLock.release();
  }

  bool checkDupAndAdd(EventIdPtr eventid) {
    return m_redundancyManager->checkDupAndAdd(eventid);
  }

  ACE_Recursive_Thread_Mutex* getRedundancyLock() {
    return &m_redundancyManager->getRedundancyLock();
  }

  GfErrType sendRequestToPrimary(TcrMessage& request, TcrMessageReply& reply) {
    return m_redundancyManager->sendRequestToPrimary(request, reply);
  }

 private:
  CacheImpl* m_cache;
  volatile bool m_initGuard;
  ACE_Map_Manager<std::string, TcrEndpoint*, ACE_Recursive_Thread_Mutex>
      m_endpoints;
  std::list<TcrEndpoint*> m_poolEndpointList;

  // key is hostname:port
  std::list<ThinClientBaseDM*> m_distMngrs;
  ACE_Recursive_Thread_Mutex m_distMngrsLock;

  ACE_Semaphore m_failoverSema;
  GF_TASK_T<TcrConnectionManager>* m_failoverTask;

  bool removeRefToEndpoint(TcrEndpoint* ep, bool keepEndpoint = false);
  TcrEndpoint* addRefToTcrEndpoint(std::string endpointName,
                                   ThinClientBaseDM* dm = NULL);

  void initializeHAEndpoints(const char* endpointsStr);
  void removeHAEndpoints();

  ACE_Semaphore m_cleanupSema;
  GF_TASK_T<TcrConnectionManager>* m_cleanupTask;

  long m_pingTaskId;
  long m_servermonitorTaskId;
  Queue<GF_TASK_T<TcrEndpoint> > m_receiverReleaseList;
  Queue<TcrConnection> m_connectionReleaseList;
  Queue<ACE_Semaphore> m_notifyCleanupSemaList;

  ACE_Semaphore m_redundancySema;
  GF_TASK_T<TcrConnectionManager>* m_redundancyTask;
  ACE_Recursive_Thread_Mutex m_notificationLock;
  bool m_isDurable;

  ThinClientRedundancyManager* m_redundancyManager;

  int failover(volatile bool& isRunning);
  int redundancy(volatile bool& isRunning);

  void cleanNotificationLists();
  int cleanup(volatile bool& isRunning);

  // Disallow copy constructor and assignment operator.
  TcrConnectionManager(const TcrConnectionManager&);
  TcrConnectionManager& operator=(const TcrConnectionManager&);

  friend class ThinClientRedundancyManager;
  friend class DistManagersLockGuard;
  friend class ThinClientPoolDM;
  friend class ThinClientPoolHADM;
  static const char* NC_Redundancy;
  static const char* NC_Failover;
  static const char* NC_CleanUp;
};

// Guard class to acquire/release distManagers lock
class DistManagersLockGuard {
 private:
  TcrConnectionManager& m_tccm;

 public:
  DistManagersLockGuard(TcrConnectionManager& tccm) : m_tccm(tccm) {
    m_tccm.m_distMngrsLock.acquire();
  }

  ~DistManagersLockGuard() { m_tccm.m_distMngrsLock.release(); }
};

}  // namespace gemfire

#endif  // __TCR_CONNECTIONMANAGER_HPP__
