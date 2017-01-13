/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include "TcrDistributionManager.hpp"
#include "ThinClientRegion.hpp"
#include "TcrEndpoint.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "Utils.hpp"
using namespace gemfire;

TcrDistributionManager::TcrDistributionManager(
    ThinClientRegion* region, TcrConnectionManager& connManager)
    : ThinClientDistributionManager(connManager, region) {
  GF_R_ASSERT(region != NULL);
  m_clientNotification =
      region->getAttributes()->getClientNotificationEnabled();
}

void TcrDistributionManager::getEndpointNames(
    std::unordered_set<std::string>& endpointNames) {
  Utils::parseEndpointNamesString(m_region->getAttributes()->getEndpoints(),
                                  endpointNames);
}

void TcrDistributionManager::destroyAction() {
  if (m_activeEndpoint >= 0) {
    m_region->unregisterKeys();
  }
}

void TcrDistributionManager::postUnregisterAction() {
  if (m_clientNotification) {
    m_region->localInvalidateFailover();
  }
}

bool TcrDistributionManager::preFailoverAction() {
  return !m_region->isDestroyed();
}

bool TcrDistributionManager::postFailoverAction(TcrEndpoint* endpoint) {
  if (m_clientNotification) {
    return (m_region->registerKeys(endpoint) == GF_NOERR);
  }
  return true;
}
