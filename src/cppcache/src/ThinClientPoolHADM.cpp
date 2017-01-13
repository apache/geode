/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ThinClientPoolHADM.hpp"
#include "ExpiryHandler_T.hpp"
#include <gfcpp/SystemProperties.hpp>

using namespace gemfire;
const char* ThinClientPoolHADM::NC_Redundancy = "NC Redundancy";
ThinClientPoolHADM::ThinClientPoolHADM(const char* name,
                                       PoolAttributesPtr poolAttr,
                                       TcrConnectionManager& connManager)
    : ThinClientPoolDM(name, poolAttr, connManager),
      m_theTcrConnManager(connManager),
      m_redundancySema(0),
      m_redundancyTask(NULL),
      m_servermonitorTaskId(-1) {
  m_redundancyManager = new ThinClientRedundancyManager(
      &connManager, poolAttr->getSubscriptionRedundancy(), this);
}

void ThinClientPoolHADM::init() {
  // Pool DM should only be inited once.
  ThinClientPoolDM::init();

  startBackgroundThreads();
}

void ThinClientPoolHADM::startBackgroundThreads() {
  SystemProperties* props = DistributedSystem::getSystemProperties();

  if (props->isGridClient()) {
    LOGWARN("Starting background threads and ignoring grid-client setting");
    ThinClientPoolDM::startBackgroundThreads();
  }

  m_redundancyManager->initialize(m_attrs->getSubscriptionRedundancy());
  //  Call maintain redundancy level, so primary is available for notification
  //  operations.
  GfErrType err = m_redundancyManager->maintainRedundancyLevel(true);

  ACE_Event_Handler* redundancyChecker =
      new ExpiryHandler_T<ThinClientPoolHADM>(
          this, &ThinClientPoolHADM::checkRedundancy);
  int32_t redundancyMonitorInterval = props->redundancyMonitorInterval();

  m_servermonitorTaskId = CacheImpl::expiryTaskManager->scheduleExpiryTask(
      redundancyChecker, 1, redundancyMonitorInterval, false);
  LOGFINE(
      "ThinClientPoolHADM::ThinClientPoolHADM Registered server "
      "monitor task with id = %ld, interval = %ld",
      m_servermonitorTaskId, redundancyMonitorInterval);

  if (ThinClientBaseDM::isFatalClientError(err)) {
    if (err == GF_CACHE_LOCATOR_EXCEPTION) {
      LOGWARN(
          "No locators were available during pool initialization with "
          "subscription redundancy.");
    } else {
      GfErrTypeToException("ThinClientPoolHADM::init", err);
    }
  }

  m_redundancyManager->startPeriodicAck();
  m_redundancyTask = new GF_TASK_T<ThinClientPoolHADM>(
      this, &ThinClientPoolHADM::redundancy, NC_Redundancy);
  m_redundancyTask->start();
}

GfErrType ThinClientPoolHADM::sendSyncRequest(TcrMessage& request,
                                              TcrMessageReply& reply,
                                              bool attemptFailover,
                                              bool isBGThread) {
  GfErrType err = GF_NOERR;

  int32_t type = request.getMessageType();

  if ((type == TcrMessage::EXECUTECQ_MSG_TYPE ||
       type == TcrMessage::STOPCQ_MSG_TYPE ||
       type == TcrMessage::CLOSECQ_MSG_TYPE ||
       type == TcrMessage::CLOSECLIENTCQS_MSG_TYPE ||
       type == TcrMessage::GETCQSTATS_MSG_TYPE ||
       type == TcrMessage::MONITORCQ_MSG_TYPE ||
       type == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE ||
       type == TcrMessage::GETDURABLECQS_MSG_TYPE)) {
    if (m_destroyPending) return GF_NOERR;
    reply.setDM(this);
    err = sendSyncRequestCq(request, reply);
  } else {
    err = ThinClientPoolDM::sendSyncRequest(request, reply, attemptFailover,
                                            isBGThread);
  }
  return err;
}

bool ThinClientPoolHADM::registerInterestForHARegion(
    TcrEndpoint* ep, const TcrMessage* request, ThinClientHARegion& region) {
  return (region.registerKeys(ep, request) == GF_NOERR);
}

GfErrType ThinClientPoolHADM::sendSyncRequestRegisterInterestEP(
    TcrMessage& request, TcrMessageReply& reply, bool attemptFailover,
    TcrEndpoint* endpoint) {
  return ThinClientBaseDM::sendSyncRequestRegisterInterest(
      request, reply, attemptFailover, NULL, endpoint);
}

GfErrType ThinClientPoolHADM::sendSyncRequestRegisterInterest(
    TcrMessage& request, TcrMessageReply& reply, bool attemptFailover,
    ThinClientRegion* region, TcrEndpoint* endpoint) {
  return m_redundancyManager->sendSyncRequestRegisterInterest(
      request, reply, attemptFailover, endpoint, this, region);
}

GfErrType ThinClientPoolHADM::sendSyncRequestCq(TcrMessage& request,
                                                TcrMessageReply& reply) {
  return m_redundancyManager->sendSyncRequestCq(request, reply, this);
}

bool ThinClientPoolHADM::preFailoverAction() { return true; }

bool ThinClientPoolHADM::postFailoverAction(TcrEndpoint* endpoint) {
  m_theTcrConnManager.triggerRedundancyThread();
  return true;
}

int ThinClientPoolHADM::redundancy(volatile bool& isRunning) {
  LOGFINE("ThinClientPoolHADM: Starting maintain redundancy thread.");
  while (isRunning) {
    m_redundancySema.acquire();
    if (isRunning && !TcrConnectionManager::isNetDown) {
      m_redundancyManager->maintainRedundancyLevel();
      while (m_redundancySema.tryacquire() != -1) {
        ;
      }
    }
  }
  LOGFINE("ThinClientPoolHADM: Ending maintain redundancy thread.");
  return 0;
}

int ThinClientPoolHADM::checkRedundancy(const ACE_Time_Value&, const void*) {
  m_redundancySema.release();
  return 0;
}

void ThinClientPoolHADM::destroy(bool keepAlive) {
  LOGDEBUG("ThinClientPoolHADM::destroy");
  if (!m_isDestroyed && !m_destroyPending) {
    checkRegions();

    if (m_remoteQueryServicePtr != NULLPTR) {
      m_remoteQueryServicePtr->close();
      m_remoteQueryServicePtr = NULLPTR;
    }

    stopPingThread();

    sendNotificationCloseMsgs();

    m_redundancyManager->close();
    delete m_redundancyManager;
    m_redundancyManager = 0;

    m_destroyPendingHADM = true;
    ThinClientPoolDM::destroy(keepAlive);
  }
}

void ThinClientPoolHADM::sendNotificationCloseMsgs() {
  if (m_redundancyTask) {
    if (m_servermonitorTaskId >= 0) {
      CacheImpl::expiryTaskManager->cancelTask(m_servermonitorTaskId);
    }
    m_redundancyTask->stopNoblock();
    m_redundancySema.release();
    m_redundancyTask->wait();
    GF_SAFE_DELETE(m_redundancyTask);
    m_redundancyManager->sendNotificationCloseMsgs();
  }
}

/*
void ThinClientPoolHADM::stopNotificationThreads()
{
  ACE_Guard< ACE_Recursive_Thread_Mutex > guard( m_endpointsLock );
  for( ACE_Map_Manager< std::string, TcrEndpoint *, ACE_Recursive_Thread_Mutex
>::iterator it = m_endpoints.begin(); it != m_endpoints.end(); it++){
    ((*it).int_id_)->stopNoBlock();
  }
  for( ACE_Map_Manager< std::string, TcrEndpoint *, ACE_Recursive_Thread_Mutex
>::iterator it = m_endpoints.begin(); it != m_endpoints.end(); it++){
    ((*it).int_id_)->stopNotifyReceiverAndCleanup();
  }
}
*/

GfErrType ThinClientPoolHADM::registerInterestAllRegions(
    TcrEndpoint* ep, const TcrMessage* request, TcrMessageReply* reply) {
  GfErrType err = GF_NOERR;
  GfErrType opErr = GF_NOERR;

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_regionsLock);
  for (std::list<ThinClientRegion*>::iterator itr = m_regions.begin();
       itr != m_regions.end(); itr++) {
    if ((opErr = (*itr)->registerKeys(ep, request, reply)) != GF_NOERR) {
      if (err == GF_NOERR) {
        err = opErr;
      }
    }
  }
  return err;
}

void ThinClientPoolHADM::addRegion(ThinClientRegion* theTCR) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_regionsLock);
  m_regions.push_back(theTCR);
}
void ThinClientPoolHADM::addDisMessToQueue(ThinClientRegion* theTCR) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_regionsLock);
  if (m_redundancyManager->allEndPointDiscon()) {
    theTCR->receiveNotification(TcrMessage::getAllEPDisMess());
  }
}
void ThinClientPoolHADM::removeRegion(ThinClientRegion* theTCR) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_regionsLock);
  for (std::list<ThinClientRegion*>::iterator itr = m_regions.begin();
       itr != m_regions.end(); itr++) {
    if (*itr == theTCR) {
      m_regions.erase(itr);
      return;
    }
  }
  GF_DEV_ASSERT("Region not found in ThinClientPoolHADM list" ? false : false);
}

void ThinClientPoolHADM::readyForEvents() {
  if (!DistributedSystem::getSystemProperties()->autoReadyForEvents()) {
    init();
  }

  const char* durable =
      DistributedSystem::getSystemProperties()->durableClientId();

  if (durable != NULL && strlen(durable) > 0) {
    m_redundancyManager->readyForEvents();
  }
}

void ThinClientPoolHADM::netDown() {
  ThinClientPoolDM::netDown();

  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpointsLock);

    for (ACE_Map_Manager<std::string, TcrEndpoint*,
                         ACE_Recursive_Thread_Mutex>::iterator currItr =
             m_endpoints.begin();
         currItr != m_endpoints.end(); currItr++) {
      (*currItr).int_id_->setConnectionStatus(false);
    }
  }

  m_redundancyManager->netDown();
}

void ThinClientPoolHADM::pingServerLocal() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(
      m_redundancyManager->getRedundancyLock());
  ThinClientPoolDM::pingServerLocal();
}

void ThinClientPoolHADM::removeCallbackConnection(TcrEndpoint* ep) {
  m_redundancyManager->removeCallbackConnection(ep);
}

void ThinClientPoolHADM::sendNotConMesToAllregions() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_regionsLock);
  for (std::list<ThinClientRegion*>::iterator it = m_regions.begin();
       it != m_regions.end(); it++) {
    (*it)->receiveNotification(TcrMessage::getAllEPDisMess());
  }
}
