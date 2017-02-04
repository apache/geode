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
#include "TssConnectionWrapper.hpp"
#include "TcrConnection.hpp"
#include "ThinClientPoolDM.hpp"
using namespace apache::geode::client;
ACE_TSS<TssConnectionWrapper> TssConnectionWrapper::s_geodeTSSConn;
TssConnectionWrapper::TssConnectionWrapper() {
  PoolPtr p = NULLPTR;
  m_pool = p;
  m_tcrConn = NULL;
}
TssConnectionWrapper::~TssConnectionWrapper() {
  // if cache close happening during this then we should NOT call this..
  if (m_tcrConn) {
    // this should be call in lock and release connection
    // but still race-condition is there if now cache-close starts happens
    // m_tcrConn->close();
    m_pool->releaseThreadLocalConnection();
    // delete m_tcrConn; m_tcrConn = NULL;
    m_tcrConn = NULL;
  }
}

void TssConnectionWrapper::setSHConnection(TcrEndpoint* ep,
                                           TcrConnection* conn) {
  std::string pn(ep->getPoolHADM()->getName());
  poolVsEndpointConnMap::iterator iter = m_poolVsEndpointConnMap.find(pn);
  PoolWrapper* pw = NULL;
  if (iter == m_poolVsEndpointConnMap.end()) {
    pw = new PoolWrapper();
    m_poolVsEndpointConnMap[pn] = pw;
  } else {
    pw = iter->second;
  }

  pw->setSHConnection(ep, conn);
}

TcrConnection* TssConnectionWrapper::getSHConnection(TcrEndpoint* ep,
                                                     const char* poolname) {
  std::string pn(poolname);
  poolVsEndpointConnMap::iterator iter = m_poolVsEndpointConnMap.find(pn);
  PoolWrapper* pw = NULL;
  if (iter == m_poolVsEndpointConnMap.end()) {
    return NULL;
  } else {
    pw = iter->second;
  }

  return pw->getSHConnection(ep);
}

void TssConnectionWrapper::releaseSHConnections(PoolPtr pool) {
  std::string pn(pool->getName());
  poolVsEndpointConnMap::iterator iter = m_poolVsEndpointConnMap.find(pn);
  PoolWrapper* pw = NULL;
  if (iter == m_poolVsEndpointConnMap.end()) {
    return;
  } else {
    pw = iter->second;
  }

  pw->releaseSHConnections(pool);
  m_poolVsEndpointConnMap.erase(pn);
  delete pw;
}

TcrConnection* TssConnectionWrapper::getAnyConnection(const char* poolname) {
  std::string pn(poolname);
  poolVsEndpointConnMap::iterator iter = m_poolVsEndpointConnMap.find(pn);
  PoolWrapper* pw = NULL;
  if (iter == m_poolVsEndpointConnMap.end()) {
    return NULL;
  } else {
    pw = iter->second;
  }

  return pw->getAnyConnection();
}

TcrConnection* PoolWrapper::getSHConnection(TcrEndpoint* ep) {
  EpNameVsConnection::iterator iter = m_EpnameVsConnection.find(ep->name());
  if (iter != m_EpnameVsConnection.end()) {
    TcrConnection* tmp = iter->second;
    m_EpnameVsConnection.erase(iter);
    return tmp;
  }
  return NULL;
}

void PoolWrapper::setSHConnection(TcrEndpoint* ep, TcrConnection* conn) {
  m_EpnameVsConnection.insert(
      std::pair<std::string, TcrConnection*>(ep->name(), conn));
}

PoolWrapper::PoolWrapper() {}

PoolWrapper::~PoolWrapper() {}

void PoolWrapper::releaseSHConnections(PoolPtr pool) {
  for (EpNameVsConnection::iterator iter = m_EpnameVsConnection.begin();
       iter != m_EpnameVsConnection.end(); iter++) {
    TcrConnection* tmp = iter->second;
    tmp->setAndGetBeingUsed(false, false);  // now this can be used by next one
    ThinClientPoolDM* dm = dynamic_cast<ThinClientPoolDM*>(pool.ptr());
    if (dm != NULL) {
      dm->put(tmp, false);
    }
  }
  m_EpnameVsConnection.clear();
}

TcrConnection* PoolWrapper::getAnyConnection() {
  EpNameVsConnection::iterator iter = m_EpnameVsConnection.begin();
  if (iter != m_EpnameVsConnection.end()) {
    TcrConnection* tmp = iter->second;
    m_EpnameVsConnection.erase(iter);
    return tmp;
  }
  return NULL;
}
