/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "TcrConnectionManager.hpp"
#include "TcrEndpoint.hpp"
#include "ExpiryHandler_T.hpp"
#include "CacheImpl.hpp"
#include "ExpiryTaskManager.hpp"
#include "ThinClientBaseDM.hpp"
#include "ThinClientCacheDistributionManager.hpp"
#include "ThinClientRedundancyManager.hpp"
#include "TcrHADistributionManager.hpp"
#include "Utils.hpp"
#include "ThinClientRegion.hpp"
#include "ThinClientHARegion.hpp"
#include "TcrConnection.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "RemoteQueryService.hpp"
#include "ThinClientLocatorHelper.hpp"
#include "ServerLocation.hpp"
#include <ace/INET_Addr.h>
#include <set>
using namespace gemfire;
volatile bool TcrConnectionManager::isNetDown = false;
volatile bool TcrConnectionManager::TEST_DURABLE_CLIENT_CRASH = false;

const char *TcrConnectionManager::NC_Redundancy = "NC Redundancy";
const char *TcrConnectionManager::NC_Failover = "NC Failover";
const char *TcrConnectionManager::NC_CleanUp = "NC CleanUp";

TcrConnectionManager::TcrConnectionManager(CacheImpl *cache)
    : m_cache(cache),
      m_initGuard(false),
      m_failoverSema(0),
      m_failoverTask(NULL),
      m_cleanupSema(0),
      m_cleanupTask(NULL),
      m_pingTaskId(-1),
      m_servermonitorTaskId(-1),
      // Create the queues with flag to not delete the objects
      m_notifyCleanupSemaList(false),
      m_redundancySema(0),
      m_redundancyTask(NULL),
      m_isDurable(false) {
  m_redundancyManager = new ThinClientRedundancyManager(this);
}

long TcrConnectionManager::getPingTaskId() { return m_pingTaskId; }
void TcrConnectionManager::init(bool isPool) {
  if (!m_initGuard) {
    m_initGuard = true;
  } else {
    return;
  }
  SystemProperties *props = DistributedSystem::getSystemProperties();
  m_isDurable = strlen(props->durableClientId()) > 0;
  int32_t pingInterval = (props->pingInterval() / 2);
  if (!props->isGridClient() && !isPool) {
    ACE_Event_Handler *connectionChecker =
        new ExpiryHandler_T<TcrConnectionManager>(
            this, &TcrConnectionManager::checkConnection);
    m_pingTaskId = CacheImpl::expiryTaskManager->scheduleExpiryTask(
        connectionChecker, 10, pingInterval, false);
    LOGFINE(
        "TcrConnectionManager::TcrConnectionManager Registered ping "
        "task with id = %ld, interval = %ld",
        m_pingTaskId, pingInterval);
  }

  CacheAttributesPtr cacheAttributes = m_cache->getAttributes();
  const char *endpoints;
  m_redundancyManager->m_HAenabled = false;

  if (cacheAttributes != NULLPTR &&
      (cacheAttributes->getRedundancyLevel() > 0 || m_isDurable) &&
      (endpoints = cacheAttributes->getEndpoints()) != NULL &&
      strcmp(endpoints, "none") != 0) {
    initializeHAEndpoints(endpoints);  // no distributaion manager at this point
    m_redundancyManager->initialize(cacheAttributes->getRedundancyLevel());
    //  Call maintain redundancy level, so primary is available for notification
    //  operations.
    GfErrType err = m_redundancyManager->maintainRedundancyLevel(true);
    m_redundancyManager->m_HAenabled =
        m_redundancyManager->m_HAenabled ||
        ThinClientBaseDM::isDeltaEnabledOnServer();

    ACE_Event_Handler *redundancyChecker =
        new ExpiryHandler_T<TcrConnectionManager>(
            this, &TcrConnectionManager::checkRedundancy);
    int32_t redundancyMonitorInterval = props->redundancyMonitorInterval();

    m_servermonitorTaskId = CacheImpl::expiryTaskManager->scheduleExpiryTask(
        redundancyChecker, 1, redundancyMonitorInterval, false);
    LOGFINE(
        "TcrConnectionManager::TcrConnectionManager Registered server "
        "monitor task with id = %ld, interval = %ld",
        m_servermonitorTaskId, redundancyMonitorInterval);

    if (ThinClientBaseDM::isFatalError(err)) {
      GfErrTypeToException("TcrConnectionManager::init", err);
    }

    m_redundancyTask = new GF_TASK_T<TcrConnectionManager>(
        this, &TcrConnectionManager::redundancy, NC_Redundancy);
    m_redundancyTask->start();

    m_redundancyManager->m_HAenabled = true;
  }

  if (!props->isGridClient()) {
    startFailoverAndCleanupThreads(isPool);
  }
}

void TcrConnectionManager::startFailoverAndCleanupThreads(bool isPool) {
  if (m_failoverTask == NULL || m_cleanupTask == NULL) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_distMngrsLock);
    if (m_failoverTask == NULL && !isPool) {
      m_failoverTask = new GF_TASK_T<TcrConnectionManager>(
          this, &TcrConnectionManager::failover, NC_Failover);
      m_failoverTask->start();
    }
    if (m_cleanupTask == NULL && !isPool) {
      if (m_redundancyManager->m_HAenabled && !isPool) {
        m_redundancyManager->startPeriodicAck();
      }
      m_cleanupTask = new GF_TASK_T<TcrConnectionManager>(
          this, &TcrConnectionManager::cleanup, NC_CleanUp);
      m_cleanupTask->start();
    }
  }
}

void TcrConnectionManager::close() {
  LOGFINE("TcrConnectionManager is closing");
  if (m_pingTaskId > 0) {
    CacheImpl::expiryTaskManager->cancelTask(m_pingTaskId);
  }

  if (m_failoverTask != NULL) {
    m_failoverTask->stopNoblock();
    m_failoverSema.release();
    m_failoverTask->wait();
    GF_SAFE_DELETE(m_failoverTask);
  }

  CacheAttributesPtr cacheAttributes = m_cache->getAttributes();
  if (cacheAttributes != NULLPTR &&
      (cacheAttributes->getRedundancyLevel() > 0 || m_isDurable)) {
    if (m_servermonitorTaskId > 0) {
      CacheImpl::expiryTaskManager->cancelTask(m_servermonitorTaskId);
    }
    if (m_redundancyTask != NULL) {
      m_redundancyTask->stopNoblock();
      m_redundancySema.release();
      m_redundancyTask->wait();
      // now stop cleanup task
      // stopCleanupTask();
      GF_SAFE_DELETE(m_redundancyTask);
    }

    m_redundancyManager->close();
    delete m_redundancyManager;
    m_redundancyManager = 0;

    removeHAEndpoints();
  }
  LOGFINE("TcrConnectionManager is closed");
}

void TcrConnectionManager::readyForEvents() {
  m_redundancyManager->readyForEvents();
}

TcrConnectionManager::~TcrConnectionManager() {
  if (m_cleanupTask != NULL) {
    m_cleanupTask->stopNoblock();
    m_cleanupSema.release();
    m_cleanupTask->wait();
    // Clean notification lists if something remains in there; see bug #250
    cleanNotificationLists();
    GF_SAFE_DELETE(m_cleanupTask);

    // sanity cleanup of any remaining endpoints with warning; see bug #298
    //  cleanup of endpoints, when regions are destroyed via notification
    {
      ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpoints.mutex());

      size_t numEndPoints = m_endpoints.current_size();
      if (numEndPoints > 0) {
        LOGFINE("TCCM: endpoints remain in destructor");
      }
      for (ACE_Map_Manager<std::string, TcrEndpoint *,
                           ACE_Recursive_Thread_Mutex>::iterator iter =
               m_endpoints.begin();
           iter != m_endpoints.end(); ++iter) {
        TcrEndpoint *ep = (*iter).int_id_;
        LOGFINE("TCCM: forcing endpoint delete for %d in destructor",
                ep->name().c_str());
        GF_SAFE_DELETE(ep);
      }
    }
  }
  TcrConnectionManager::TEST_DURABLE_CLIENT_CRASH = false;
}

void TcrConnectionManager::connect(
    ThinClientBaseDM *distMng, std::vector<TcrEndpoint *> &endpoints,
    const std::unordered_set<std::string> &endpointStrs) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guardDistMngrs(m_distMngrsLock);
  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpoints.mutex());
    int32_t numEndPoints = static_cast<int32_t>(endpointStrs.size());

    if (numEndPoints == 0) {
      LOGFINE(
          "TcrConnectionManager::connect(): Empty endpointstr vector "
          "passed to TCCM, will initialize endpoints list with all available "
          "endpoints (%d).",
          m_endpoints.current_size());
      for (ACE_Map_Manager<std::string, TcrEndpoint *,
                           ACE_Recursive_Thread_Mutex>::iterator currItr =
               m_endpoints.begin();
           currItr != m_endpoints.end(); ++currItr) {
        TcrEndpoint *ep = (*currItr).int_id_;
        ep->setNumRegions(ep->numRegions() + 1);
        LOGFINER(
            "TCCM 2: incremented region reference count for endpoint %s "
            "to %d",
            ep->name().c_str(), ep->numRegions());
        endpoints.push_back(ep);
      }
    } else {
      for (std::unordered_set<std::string>::const_iterator iter =
               endpointStrs.begin();
           iter != endpointStrs.end(); ++iter) {
        TcrEndpoint *ep = addRefToTcrEndpoint(*iter, distMng);
        endpoints.push_back(ep);
      }
    }
  }

  m_distMngrs.push_back(distMng);

  // If a region/DM is joining after the marker has been
  // received then trigger it's marker flag.
  if (m_redundancyManager->m_globalProcessedMarker) {
    TcrHADistributionManager *tcrHADM =
        dynamic_cast<TcrHADistributionManager *>(distMng);
    if (tcrHADM != NULL) {
      ThinClientHARegion *tcrHARegion =
          dynamic_cast<ThinClientHARegion *>(tcrHADM->m_region);
      tcrHARegion->setProcessedMarker();
    }
  }
}

TcrEndpoint *TcrConnectionManager::addRefToTcrEndpoint(std::string endpointName,
                                                       ThinClientBaseDM *dm) {
  TcrEndpoint *ep = NULL;
  /*
  endpointName = Utils::convertHostToCanonicalForm(endpointName.c_str());
  */
  if (0 != m_endpoints.find(endpointName, ep)) {
    // this endpoint does not exist
    ep = new TcrEndpoint(endpointName, m_cache, m_failoverSema, m_cleanupSema,
                         m_redundancySema, dm, false);
    GF_R_ASSERT(0 == m_endpoints.bind(endpointName, ep));
  }
  ep->setNumRegions(ep->numRegions() + 1);

  LOGFINER("TCCM: incremented region reference count for endpoint %s to %d",
           ep->name().c_str(), ep->numRegions());

  return ep;
}

void TcrConnectionManager::disconnect(ThinClientBaseDM *distMng,
                                      std::vector<TcrEndpoint *> &endpoints,
                                      bool keepEndpoints) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guardDistMngrs(m_distMngrsLock);
  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpoints.mutex());

    int32_t numEndPoints = static_cast<int32_t>(endpoints.size());
    for (int32_t i = 0; i < numEndPoints; ++i) {
      TcrEndpoint *ep = endpoints[i];
      removeRefToEndpoint(ep, keepEndpoints);
    }
  }

  m_distMngrs.remove(distMng);
}

bool TcrConnectionManager::removeRefToEndpoint(TcrEndpoint *ep,
                                               bool keepEndpoint) {
  bool hasRemovedEndpoint = false;

  if (keepEndpoint && (ep->numRegions() == 1)) {
    return false;
  }
  ep->setNumRegions(ep->numRegions() - 1);

  LOGFINER("TCCM: decremented region reference count for endpoint %s to %d",
           ep->name().c_str(), ep->numRegions());

  if (0 == ep->numRegions()) {
    // this endpoint no longer used
    GF_R_ASSERT(0 == m_endpoints.unbind(ep->name(), ep));
    LOGFINE("delete endpoint %s", ep->name().c_str());
    GF_SAFE_DELETE(ep);
    hasRemovedEndpoint = true;
  }
  return hasRemovedEndpoint;
}

int TcrConnectionManager::processEventIdMap(const ACE_Time_Value &currTime,
                                            const void *) {
  return m_redundancyManager->processEventIdMap(currTime, NULL);
}

int TcrConnectionManager::checkConnection(const ACE_Time_Value &,
                                          const void *) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpoints.mutex());
  ACE_Map_Manager<std::string, TcrEndpoint *,
                  ACE_Recursive_Thread_Mutex>::iterator currItr =
      m_endpoints.begin();
  while (currItr != m_endpoints.end()) {
    if ((*currItr).int_id_->connected() && !isNetDown) {
      (*currItr).int_id_->pingServer();
    }
    currItr++;
  }
  return 0;
}

int TcrConnectionManager::checkRedundancy(const ACE_Time_Value &,
                                          const void *) {
  m_redundancySema.release();
  return 0;
}

int TcrConnectionManager::failover(volatile bool &isRunning) {
  LOGFINE("TcrConnectionManager: starting failover thread");
  while (isRunning) {
    m_failoverSema.acquire();
    if (isRunning && !isNetDown) {
      try {
        ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_distMngrsLock);
        for (std::list<ThinClientBaseDM *>::iterator it = m_distMngrs.begin();
             it != m_distMngrs.end(); ++it) {
          (*it)->failover();
        }
        while (m_failoverSema.tryacquire() != -1) {
          ;
        }
      } catch (const Exception &e) {
        LOGERROR(e.getMessage());
      } catch (const std::exception &e) {
        LOGERROR(e.what());
      } catch (...) {
        LOGERROR(
            "Unexpected exception while failing over to a "
            "different endpoint");
      }
    }
  }
  LOGFINE("TcrConnectionManager: ending failover thread");
  return 0;
}

void TcrConnectionManager::getAllEndpoints(
    std::vector<TcrEndpoint *> &endpoints) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpoints.mutex());

  for (ACE_Map_Manager<std::string, TcrEndpoint *,
                       ACE_Recursive_Thread_Mutex>::iterator currItr =
           m_endpoints.begin();
       currItr != m_endpoints.end(); currItr++) {
    endpoints.push_back((*currItr).int_id_);
  }
}

int32_t TcrConnectionManager::getNumEndPoints() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpoints.mutex());
  return static_cast<int32_t>(m_endpoints.current_size());
}

GfErrType TcrConnectionManager::registerInterestAllRegions(
    TcrEndpoint *ep, const TcrMessage *request, TcrMessageReply *reply) {
  // Preconditions:
  // 1. m_distMngrs.size() > 1 (query distribution manager + 1 or more
  // TcrHADistributionManagers).

  GfErrType err = GF_NOERR;
  GfErrType opErr = GF_NOERR;
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_distMngrsLock);
  std::list<ThinClientBaseDM *>::iterator begin = m_distMngrs.begin();
  std::list<ThinClientBaseDM *>::iterator end = m_distMngrs.end();
  for (std::list<ThinClientBaseDM *>::iterator it = begin; it != end; ++it) {
    TcrHADistributionManager *tcrHADM =
        dynamic_cast<TcrHADistributionManager *>(*it);
    if (tcrHADM != NULL) {
      if ((opErr = tcrHADM->registerInterestForRegion(ep, request, reply)) !=
          GF_NOERR) {
        if (err == GF_NOERR) {
          err = opErr;
        }
      }
    }
  }
  return err;
}
GfErrType TcrConnectionManager::sendSyncRequestCq(TcrMessage &request,
                                                  TcrMessageReply &reply) {
  LOGDEBUG("TcrConnectionManager::sendSyncRequestCq");
  GfErrType err = GF_NOERR;
  // Preconditions:
  // 1. m_distMngrs.size() > 1 (query distribution manager + 1 or more
  // TcrHADistributionManagers).

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_distMngrsLock);
  std::list<ThinClientBaseDM *>::iterator begin = m_distMngrs.begin();
  std::list<ThinClientBaseDM *>::iterator end = m_distMngrs.end();
  for (std::list<ThinClientBaseDM *>::iterator it = begin; it != end; ++it) {
    TcrHADistributionManager *tcrHADM =
        dynamic_cast<TcrHADistributionManager *>(*it);
    if (tcrHADM != NULL) {
      return tcrHADM->sendSyncRequestCq(request, reply);
    }
  }
  return err;
}

void TcrConnectionManager::initializeHAEndpoints(const char *endpointsStr) {
  std::unordered_set<std::string> endpointsList;
  Utils::parseEndpointNamesString(endpointsStr, endpointsList);
  for (std::unordered_set<std::string>::iterator iter = endpointsList.begin();
       iter != endpointsList.end(); ++iter) {
    addRefToTcrEndpoint(*iter);
  }
  // Postconditions:
  // 1. endpointsList.size() > 0
  GF_DEV_ASSERT(endpointsList.size() > 0);
}

void TcrConnectionManager::removeHAEndpoints() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpoints.mutex());
  ACE_Map_Manager<std::string, TcrEndpoint *,
                  ACE_Recursive_Thread_Mutex>::iterator currItr =
      m_endpoints.begin();
  while (currItr != m_endpoints.end()) {
    if (removeRefToEndpoint((*currItr).int_id_)) {
      currItr = m_endpoints.begin();
    } else {
      currItr++;
    }
  }
}

void TcrConnectionManager::netDown() {
  isNetDown = true;

  //  sleep for 15 seconds to allow ping and redundancy threads to pause.
  gemfire::millisleep(15000);

  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpoints.mutex());

    for (ACE_Map_Manager<std::string, TcrEndpoint *,
                         ACE_Recursive_Thread_Mutex>::iterator currItr =
             m_endpoints.begin();
         currItr != m_endpoints.end(); currItr++) {
      (*currItr).int_id_->setConnectionStatus(false);
    }
  }

  m_redundancyManager->netDown();
}

/* Need to do a get on unknown key after calling this Fn to restablish all
 * connection */
void TcrConnectionManager::revive() {
  isNetDown = false;

  //  sleep for 15 seconds to allow redundancy thread to reestablish
  //  connections.
  gemfire::millisleep(15000);
}

int TcrConnectionManager::redundancy(volatile bool &isRunning) {
  LOGFINE("Starting subscription maintain redundancy thread.");
  while (isRunning) {
    m_redundancySema.acquire();
    if (isRunning && !isNetDown) {
      m_redundancyManager->maintainRedundancyLevel();
      while (m_redundancySema.tryacquire() != -1) {
        ;
      }
    }
  }
  LOGFINE("Ending subscription maintain redundancy thread.");
  return 0;
}

void TcrConnectionManager::addNotificationForDeletion(
    GF_TASK_T<TcrEndpoint> *notifyReceiver, TcrConnection *notifyConnection,
    ACE_Semaphore &notifyCleanupSema) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_notificationLock);
  m_connectionReleaseList.put(notifyConnection);
  m_receiverReleaseList.put(notifyReceiver);
  m_notifyCleanupSemaList.put(&notifyCleanupSema);
}

int TcrConnectionManager::cleanup(volatile bool &isRunning) {
  LOGFINE("TcrConnectionManager: starting cleanup thread");
  do {
    //  If we block on acquire, the queue must be empty (precondition).
    if (m_receiverReleaseList.size() == 0) {
      LOGDEBUG(
          "TcrConnectionManager::cleanup(): waiting to acquire cleanup "
          "semaphore.");
      m_cleanupSema.acquire();
    }
    cleanNotificationLists();

    while (m_cleanupSema.tryacquire() != -1) {
      ;
    }

  } while (isRunning);

  LOGFINE("TcrConnectionManager: ending cleanup thread");
  //  Postcondition - all notification channels should be cleaned up by the end
  //  of this function.
  GF_DEV_ASSERT(m_receiverReleaseList.size() == 0);
  return 0;
}

void TcrConnectionManager::cleanNotificationLists() {
  GF_TASK_T<TcrEndpoint> *notifyReceiver;
  TcrConnection *notifyConnection;
  ACE_Semaphore *notifyCleanupSema;

  while (true) {
    {
      ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_notificationLock);
      notifyReceiver = m_receiverReleaseList.get();
      if (!notifyReceiver) break;
      notifyConnection = m_connectionReleaseList.get();
      notifyCleanupSema = m_notifyCleanupSemaList.get();
    }
    notifyReceiver->wait();
    GF_SAFE_DELETE(notifyReceiver);
    GF_SAFE_DELETE(notifyConnection);
    notifyCleanupSema->release();
  }
}

void TcrConnectionManager::processMarker() {
  // also set the static bool m_processedMarker for makePrimary messages
  m_redundancyManager->m_globalProcessedMarker = true;
}

//  TESTING: Durable clients - return queue status of endpoing. Not thread safe.
bool TcrConnectionManager::getEndpointStatus(const std::string &endpoint) {
  for (ACE_Map_Manager<std::string, TcrEndpoint *,
                       ACE_Recursive_Thread_Mutex>::iterator currItr =
           m_endpoints.begin();
       currItr != m_endpoints.end(); currItr++) {
    TcrEndpoint *ep = (*currItr).int_id_;
    const std::string epName = ep->name();
    if (epName == endpoint) return ep->getServerQueueStatusTEST();
  }
  return false;
}

GfErrType TcrConnectionManager::sendSyncRequestCq(
    TcrMessage &request, TcrMessageReply &reply,
    TcrHADistributionManager *theHADM) {
  return m_redundancyManager->sendSyncRequestCq(request, reply, theHADM);
}

GfErrType TcrConnectionManager::sendSyncRequestRegisterInterest(
    TcrMessage &request, TcrMessageReply &reply, bool attemptFailover,
    TcrEndpoint *endpoint, TcrHADistributionManager *theHADM,
    ThinClientRegion *region) {
  return m_redundancyManager->sendSyncRequestRegisterInterest(
      request, reply, attemptFailover, endpoint, theHADM, region);
}
