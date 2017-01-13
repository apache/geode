/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include "ThinClientBaseDM.hpp"
#include "ThinClientCacheDistributionManager.hpp"
#include "TcrMessage.hpp"
#include "TcrEndpoint.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "ReadWriteLock.hpp"
#include "ThinClientRegion.hpp"
#include "RemoteQueryService.hpp"
#include "CacheImpl.hpp"
#include <gfcpp/CacheAttributes.hpp>
#include <algorithm>

using namespace gemfire;

ThinClientCacheDistributionManager::ThinClientCacheDistributionManager(
    TcrConnectionManager& connManager)
    : ThinClientDistributionManager(connManager, NULL) {}

void ThinClientCacheDistributionManager::init() {
  LOGDEBUG("ThinClientCacheDistributionManager::init");
  if (m_connManager.getNumEndPoints() == 0) {
    throw IllegalStateException("No endpoints defined for query.");
  }
  ThinClientBaseDM::init();
}

GfErrType ThinClientCacheDistributionManager::sendSyncRequestCq(
    TcrMessage& request, TcrMessageReply& reply) {
  preFailoverAction();

  reply.setDM(this);

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpointsLock);

  // Return best effort result: If CQ succeeds on ANY server return no-error
  // even if
  // other servers might fail since this method is called in non-HA with
  // failover case.

  GfErrType err = GF_NOTCON;
  GfErrType opErr = GF_NOERR;

  for (std::vector<TcrEndpoint*>::iterator ep = m_endpoints.begin();
       ep != m_endpoints.end(); ++ep) {
    if ((*ep)->connected()) {
      (*ep)->setDM(this);
      opErr = sendRequestToEP(
          request, reply,
          *ep);  // this will go to ThinClientDistributionManager
      if (opErr == GF_NOERR ||
          (ThinClientBaseDM::isFatalClientError(opErr) && err != GF_NOERR)) {
        err = opErr;
      }
    }
  }

  // This should return only either NOERR (takes precedence), NOTCON or a
  // "client fatal error" such as NotAuthorized.
  return err;
}

GfErrType ThinClientCacheDistributionManager::sendSyncRequest(
    TcrMessage& request, TcrMessageReply& reply, bool attemptFailover,
    bool isBGThread) {
  GfErrType err = GF_NOERR;
  int32_t type = request.getMessageType();
  if (m_connManager.haEnabled() &&
      (type == TcrMessage::EXECUTECQ_MSG_TYPE ||
       type == TcrMessage::STOPCQ_MSG_TYPE ||
       type == TcrMessage::CLOSECQ_MSG_TYPE ||
       type == TcrMessage::CLOSECLIENTCQS_MSG_TYPE ||
       type == TcrMessage::GETCQSTATS_MSG_TYPE ||
       type == TcrMessage::MONITORCQ_MSG_TYPE ||
       type == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE ||
       type == TcrMessage::GETDURABLECQS_MSG_TYPE)) {
    err = m_connManager.sendSyncRequestCq(request, reply);
  } else if ((type == TcrMessage::EXECUTECQ_MSG_TYPE ||
              type == TcrMessage::STOPCQ_MSG_TYPE ||
              type == TcrMessage::CLOSECQ_MSG_TYPE ||
              type == TcrMessage::CLOSECLIENTCQS_MSG_TYPE ||
              type == TcrMessage::GETCQSTATS_MSG_TYPE ||
              type == TcrMessage::MONITORCQ_MSG_TYPE ||
              type == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE ||
              type == TcrMessage::GETDURABLECQS_MSG_TYPE)) {
    err = sendSyncRequestCq(request, reply);
  } else {
    err = ThinClientDistributionManager::sendSyncRequest(request, reply,
                                                         attemptFailover);
  }
  return err;
}

GfErrType ThinClientCacheDistributionManager::sendRequestToPrimary(
    TcrMessage& request, TcrMessageReply& reply) {
  GfErrType err = GF_NOTCON;
  if (m_connManager.haEnabled()) {
    err = m_connManager.sendRequestToPrimary(request, reply);
  } else {
    //  Call sendSyncRequest() with failover enabled.
    // TODO: Must ensure that active endpoint is correctly tracked so that
    // request is sent to an endpoint
    // for which callback connection is present.
    err = ThinClientDistributionManager::sendSyncRequest(request, reply, true);
  }
  return err;
}

bool ThinClientCacheDistributionManager::preFailoverAction() {
  if (!m_initDone) {
    // nothing to be done if not initialized
    return true;
  }
  //  take the global endpoint lock so that the global endpoints list
  // does not change while we are (possibly) adding endpoint to this endpoints
  // list and incrementing the reference count of endpoint
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(
      m_connManager.getGlobalEndpoints().mutex());
  //  This method is called at the time of failover to refresh
  // the list of endpoints.
  std::vector<TcrEndpoint*> currentGlobalEndpointsList;
  m_connManager.getAllEndpoints(currentGlobalEndpointsList);

  //  Update local list with new endpoints.
  std::vector<TcrEndpoint*> newEndpointsList;
  for (std::vector<TcrEndpoint*>::iterator it =
           currentGlobalEndpointsList.begin();
       it != currentGlobalEndpointsList.end(); ++it) {
    bool found = false;
    for (std::vector<TcrEndpoint*>::iterator currIter = m_endpoints.begin();
         currIter != m_endpoints.end(); ++currIter) {
      if (*currIter == *it) {
        found = true;
        break;
      }
    }
    if (!found) newEndpointsList.push_back(*it);
  }

  for (std::vector<TcrEndpoint*>::iterator it = newEndpointsList.begin();
       it != newEndpointsList.end(); ++it) {
    TcrEndpoint* ep = *it;
    m_endpoints.push_back(ep);
    ep->setNumRegions(ep->numRegions() + 1);
    LOGFINER(
        "TCCDM: incremented region reference count for endpoint %s "
        "to %d",
        ep->name().c_str(), ep->numRegions());
  }

  return true;
}
bool ThinClientCacheDistributionManager::postFailoverAction(
    TcrEndpoint* endpoint) {
  LOGDEBUG("ThinClientCacheDistributionManager : executeAllCqs");
  if (m_connManager.haEnabled()) {
    LOGDEBUG(
        "ThinClientCacheDistributionManager : executeAllCqs HA: case, done "
        "else where");
    return true;
  }

  CacheImpl* cache = m_connManager.getCacheImpl();

  if (cache == NULL) {
    LOGERROR("Client not initialized for failover");
    return false;
  }
  try {
    RemoteQueryServicePtr rqsService =
        dynCast<RemoteQueryServicePtr>(cache->getQueryService(true));
    rqsService->executeAllCqs(true);
  } catch (const Exception& excp) {
    LOGWARN("Failed to recover CQs during failover attempt to endpoint[%s]: %s",
            endpoint->name().c_str(), excp.getMessage());
    return false;
  } catch (...) {
    LOGWARN("Failed to recover CQs during failover attempt to endpoint[%s]",
            endpoint->name().c_str());
    return false;
  }
  return true;
}
