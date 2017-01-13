/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include "TcrHADistributionManager.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "TcrMessage.hpp"
#include "Utils.hpp"
#include "ThinClientRegion.hpp"
#include "ThinClientHARegion.hpp"
#include "CacheImpl.hpp"
#include "RemoteQueryService.hpp"

using namespace gemfire;

TcrHADistributionManager::TcrHADistributionManager(
    ThinClientRegion* theRegion, TcrConnectionManager& connManager,
    CacheAttributesPtr cacheAttributes)
    : ThinClientDistributionManager(connManager, theRegion),
      m_cacheAttributes(cacheAttributes),
      m_theTcrConnManager(connManager) {
  GF_R_ASSERT(theRegion != NULL);
}

void TcrHADistributionManager::init() {
  // Calling base init().
  ThinClientDistributionManager::init();
}

bool TcrHADistributionManager::preFailoverAction() {
  return !m_region->isDestroyed();
}

bool TcrHADistributionManager::postFailoverAction(TcrEndpoint* endpoint) {
  // Trigger the redundancy thread.
  m_connManager.triggerRedundancyThread();
  return true;
}

GfErrType TcrHADistributionManager::registerInterestForRegion(
    TcrEndpoint* ep, const TcrMessage* request, TcrMessageReply* reply) {
  return m_region->registerKeys(ep, request, reply);
}

void TcrHADistributionManager::getEndpointNames(
    std::unordered_set<std::string>& endpointNames) {
  Utils::parseEndpointNamesString(m_cacheAttributes->getEndpoints(),
                                  endpointNames);
}
GfErrType TcrHADistributionManager::sendRequestToEP(const TcrMessage& request,
                                                    TcrMessageReply& reply,
                                                    TcrEndpoint* endpoint) {
  LOGDEBUG("TcrHADistributionManager::sendRequestToEP msgType[%d]",
           request.getMessageType());
  GfErrType err = GF_NOERR;
  reply.setDM(this);
  if (endpoint->connected()) {
    // err = ThinClientBaseDM::sendRequestToEndPoint( request, reply, endpoint
    // );
    err = ThinClientDistributionManager::sendRequestToEP(request, reply,
                                                         endpoint);
  } else {
    err = GF_NOTCON;
  }
  return err;
}
GfErrType TcrHADistributionManager::sendSyncRequestCq(TcrMessage& request,
                                                      TcrMessageReply& reply) {
  return m_connManager.sendSyncRequestCq(request, reply, this);
}

GfErrType TcrHADistributionManager::sendSyncRequestRegisterInterestEP(
    TcrMessage& request, TcrMessageReply& reply, bool attemptFailover,
    TcrEndpoint* endpoint) {
  return ThinClientBaseDM::sendSyncRequestRegisterInterest(
      request, reply, attemptFailover, NULL, endpoint);
}

GfErrType TcrHADistributionManager::sendSyncRequestRegisterInterest(
    TcrMessage& request, TcrMessageReply& reply, bool attemptFailover,
    ThinClientRegion* region, TcrEndpoint* endpoint) {
  return m_connManager.sendSyncRequestRegisterInterest(
      request, reply, attemptFailover, endpoint, this, m_region);
}
