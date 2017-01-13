/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "TcrPoolEndPoint.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "ThinClientPoolDM.hpp"
using namespace gemfire;
#define DEFAULT_CALLBACK_CONNECTION_TIMEOUT_SECONDS 180
TcrPoolEndPoint::TcrPoolEndPoint(const std::string& name, CacheImpl* cache,
                                 ACE_Semaphore& failoverSema,
                                 ACE_Semaphore& cleanupSema,
                                 ACE_Semaphore& redundancySema,
                                 ThinClientPoolDM* dm)
    : TcrEndpoint(name, cache, failoverSema, cleanupSema, redundancySema, dm),
      m_dm(dm) {}
bool TcrPoolEndPoint::checkDupAndAdd(EventIdPtr eventid) {
  return m_dm->checkDupAndAdd(eventid);
}

void TcrPoolEndPoint::processMarker() { m_dm->processMarker(); }
QueryServicePtr TcrPoolEndPoint::getQueryService() {
  return m_dm->getQueryServiceWithoutCheck();
}
void TcrPoolEndPoint::sendRequestForChunkedResponse(const TcrMessage& request,
                                                    TcrMessageReply& reply,
                                                    TcrConnection* conn) {
  conn->sendRequestForChunkedResponse(request, request.getMsgLength(), reply,
                                      request.getTimeout(), reply.getTimeout());
}
ThinClientPoolDM* TcrPoolEndPoint::getPoolHADM() { return m_dm; }
void TcrPoolEndPoint::triggerRedundancyThread() {
  m_dm->triggerRedundancyThread();
}
void TcrPoolEndPoint::closeFailedConnection(TcrConnection*& conn) {
  if (!m_dm->getThreadLocalConnections()) closeConnection(conn);
}

bool TcrPoolEndPoint::isMultiUserMode() { return m_dm->isMultiUserMode(); }

void TcrPoolEndPoint::closeNotification() {
  LOGFINE("TcrPoolEndPoint::closeNotification..");
  m_notifyReceiver->stopNoblock();
  m_notifyConnectionList.push_back(m_notifyConnection);
  m_notifyReceiverList.push_back(m_notifyReceiver);
  m_isQueueHosted = false;
}

GfErrType TcrPoolEndPoint::registerDM(bool clientNotification, bool isSecondary,
                                      bool isActiveEndpoint,
                                      ThinClientBaseDM* distMgr) {
  GfErrType err = GF_NOERR;
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_dm->getPoolLock());
  ACE_Guard<ACE_Recursive_Thread_Mutex> guardQueueHosted(getQueueHostedMutex());

  if (!connected()) {
    TcrConnection* newConn;
    if ((err = createNewConnection(
             newConn, false, false,
             DistributedSystem::getSystemProperties()->connectTimeout(), 0,
             connected())) != GF_NOERR) {
      setConnected(false);
      return err;
    }
    m_dm->addConnection(newConn);
    // m_connected = true;
    setConnected(true);
  }

  LOGFINEST(
      "TcrEndpoint::registerPoolDM( ): registering DM and notification "
      "channel for endpoint %s",
      name().c_str());

  if (m_numRegionListener == 0) {
    if ((err = createNewConnection(
             m_notifyConnection, true, isSecondary,
             DistributedSystem::getSystemProperties()->connectTimeout() * 3,
             0)) != GF_NOERR) {
      setConnected(false);
      LOGWARN("Failed to start subscription channel for endpoint %s",
              name().c_str());
      return err;
    }
    m_notifyReceiver = new GF_TASK_T<TcrEndpoint>(
        this, &TcrEndpoint::receiveNotification, NC_Notification);
    m_notifyReceiver->start();
  }
  ++m_numRegionListener;
  LOGFINEST("Incremented notification count for endpoint %s to %d",
            name().c_str(), m_numRegionListener);

  m_isQueueHosted = true;
  setConnected(true);
  return err;
}
void TcrPoolEndPoint::unregisterDM(bool clientNotification,
                                   ThinClientBaseDM* distMgr,
                                   bool checkQueueHosted) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(getQueueHostedMutex());

  if (checkQueueHosted && !m_isQueueHosted) {
    LOGFINEST(
        "TcrEndpoint: unregistering pool DM, notification channel not present "
        "for %s",
        name().c_str());
    return;
  }

  LOGFINEST(
      "TcrEndpoint: unregistering pool DM and closing notification "
      "channel for endpoint %s",
      name().c_str());
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard2(m_notifyReceiverLock);
  if (m_numRegionListener > 0 && --m_numRegionListener == 0) {
    closeNotification();
  }
  LOGFINEST("Decremented notification count for endpoint %s to %d",
            name().c_str(), m_numRegionListener);
  LOGFINEST("TcrEndpoint: unregisterPoolDM done for endpoint %s",
            name().c_str());
}

bool TcrPoolEndPoint::handleIOException(const std::string& message,
                                        TcrConnection*& conn, bool isBgThread) {
  if (!isBgThread) {
    m_dm->setStickyNull(false);
  }
  return TcrEndpoint::handleIOException(message, conn);
}

void TcrPoolEndPoint::handleNotificationStats(int64 byteLength) {
  m_dm->getStats().incReceivedBytes(byteLength);
  m_dm->getStats().incMessageBeingReceived();
}
