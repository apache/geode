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
#include <gfcpp/gfcpp_globals.hpp>
#include "TcrDistributionManager.hpp"
#include "ThinClientRegion.hpp"
#include "TcrEndpoint.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "Utils.hpp"
using namespace apache::geode::client;

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
