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
#include "UserAttributes.hpp"
#include "ProxyCache.hpp"

using namespace apache::geode::client;

UserAttributes::UserAttributes(PropertiesPtr credentials, PoolPtr pool,
                               ProxyCache* proxyCache)
    : m_isUserAuthenticated(false), m_pool(pool) {
  m_credentials = credentials;

  ProxyCachePtr pcp(proxyCache);
  m_proxyCache = pcp;
}

bool UserAttributes::isCacheClosed() { return m_proxyCache->isClosed(); }

UserAttributes::~UserAttributes() {
  std::map<std::string, UserConnectionAttributes*>::iterator it;

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_listLock);
  for (it = m_connectionAttr.begin(); it != m_connectionAttr.end(); it++) {
    UserConnectionAttributes* uca = (*it).second;
    if (uca != NULL) {
      GF_SAFE_DELETE(uca);
    }
  }
}

UserConnectionAttributes* UserAttributes::getConnectionAttribute() {
  LOGDEBUG("UserConnectionAttributes* getConnectionAttribute().");
  if (m_connectionAttr.size() == 0) return NULL;

  //  std::map<std::string, UserConnectionAttributes*>::iterator it;

  // ACE_Guard< ACE_Recursive_Thread_Mutex > guard( m_listLock );
  /*for( it = m_connectionAttr.begin(); it != m_connectionAttr.end(); it++ )
  {
    UserConnectionAttributes* uca = &((*it).second);
    if (uca->isAuthenticated() && uca->getEndpoint()->connected())
      return uca;
    else
      uca->setUnAuthenticated();
  }*/
  return NULL;
}

void UserAttributes::unAuthenticateEP(TcrEndpoint* endpoint) {
  LOGDEBUG("UserAttributes::unAuthenticateEP.");
  if (m_connectionAttr.size() == 0) return;
  // TODO: it is always returning first one
  // need to take care when FE for onServers();
  // TODO: chk before returing whether endpoint is up or not
  // std::map<std::string, UserConnectionAttributes>::iterator it;

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_listLock);
  UserConnectionAttributes* uca = m_connectionAttr[endpoint->name()];
  if (uca != NULL) {
    m_connectionAttr.erase(endpoint->name());
    GF_SAFE_DELETE(uca);
  }
  /*for( it = m_connectionAttr.begin(); it != m_connectionAttr.end(); it++ )
  {
    UserConnectionAttributes* uca = &((*it).second);
    if (uca->getEndpoint() == endpoint)
      uca->setUnAuthenticated();
  }*/
}

PoolPtr UserAttributes::getPool() { return m_pool; }

UserConnectionAttributes* UserAttributes::getConnectionAttribute(
    TcrEndpoint* ep) {
  LOGDEBUG("UserConnectionAttributes* getConnectionAttribute with EP.");
  if (m_connectionAttr.size() == 0) return NULL;

  // std::map<std::string, UserConnectionAttributes>::iterator it;
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_listLock);
  /*for( it = m_connectionAttr.begin(); it != m_connectionAttr.end(); it++ )
  {
    UserConnectionAttributes* uca = &((*it).second);
    if (uca->isAuthenticated() && (uca->getEndpoint() == ep))
      return uca;
  }*/

  return m_connectionAttr[ep->name()];
}

bool UserAttributes::isEndpointAuthenticated(TcrEndpoint* ep) {
  LOGDEBUG(
      "UserAttributes::isEndpointAuthenticated: (TcrEndpoint* ep) with EP.");
  if (m_connectionAttr.size() == 0) return false;

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_listLock);
  UserConnectionAttributes* uca = m_connectionAttr[ep->name()];
  if (uca != NULL && uca->isAuthenticated() && (uca->getEndpoint() == ep)) {
    return true;
  }
  return false;
}
PropertiesPtr UserAttributes::getCredentials() {
  if (m_proxyCache->isClosed()) {
    throw IllegalStateException("User cache has been closed");
  }
  if (m_credentials == NULLPTR) {
    LOGDEBUG("getCredentials");
  } else {
    LOGDEBUG("getCredentials not null ");
  }
  return m_credentials;
}

ProxyCachePtr UserAttributes::getProxyCache() { return m_proxyCache; }

ACE_TSS<TSSUserAttributesWrapper>
    TSSUserAttributesWrapper::s_geodeTSSUserAttributes;

GuardUserAttribures::GuardUserAttribures(ProxyCachePtr proxyCache) {
  setProxyCache(proxyCache);
}

void GuardUserAttribures::setProxyCache(ProxyCachePtr proxyCache) {
  m_proxyCache = proxyCache;
  LOGDEBUG("GuardUserAttribures::GuardUserAttribures:");
  if (m_proxyCache != NULLPTR && !proxyCache->isClosed()) {
    TSSUserAttributesWrapper::s_geodeTSSUserAttributes->setUserAttributes(
        proxyCache->m_userAttributes);
  } else {
    throw CacheClosedException("User Cache has been closed");
  }
}

GuardUserAttribures::GuardUserAttribures() { m_proxyCache = NULLPTR; }

GuardUserAttribures::~GuardUserAttribures() {
  if (m_proxyCache != NULLPTR) {
    TSSUserAttributesWrapper::s_geodeTSSUserAttributes->setUserAttributes(
        NULLPTR);
  }
}
