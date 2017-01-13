/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "UserAttributes.hpp"
#include "ProxyCache.hpp"

using namespace gemfire;

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
    TSSUserAttributesWrapper::s_gemfireTSSUserAttributes;

GuardUserAttribures::GuardUserAttribures(ProxyCachePtr proxyCache) {
  setProxyCache(proxyCache);
}

void GuardUserAttribures::setProxyCache(ProxyCachePtr proxyCache) {
  m_proxyCache = proxyCache;
  LOGDEBUG("GuardUserAttribures::GuardUserAttribures:");
  if (m_proxyCache != NULLPTR && !proxyCache->isClosed()) {
    TSSUserAttributesWrapper::s_gemfireTSSUserAttributes->setUserAttributes(
        proxyCache->m_userAttributes);
  } else {
    throw CacheClosedException("User Cache has been closed");
  }
}

GuardUserAttribures::GuardUserAttribures() { m_proxyCache = NULLPTR; }

GuardUserAttribures::~GuardUserAttribures() {
  if (m_proxyCache != NULLPTR) {
    TSSUserAttributesWrapper::s_gemfireTSSUserAttributes->setUserAttributes(
        NULLPTR);
  }
}
