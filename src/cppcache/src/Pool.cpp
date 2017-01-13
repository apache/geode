/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/Pool.hpp>
#include <PoolAttributes.hpp>
#include <gfcpp/Cache.hpp>
#include <gfcpp/CacheFactory.hpp>
#include <ProxyCache.hpp>
#include <ThinClientPoolHADM.hpp>
/**
 * @file
 */

using namespace gemfire;

Pool::Pool(PoolAttributesPtr attr) : m_attrs(attr) {}
Pool::~Pool() {}

int Pool::getFreeConnectionTimeout() const {
  return m_attrs->getFreeConnectionTimeout();
}
int Pool::getLoadConditioningInterval() const {
  return m_attrs->getLoadConditioningInterval();
}
int Pool::getSocketBufferSize() const { return m_attrs->getSocketBufferSize(); }
int Pool::getReadTimeout() const { return m_attrs->getReadTimeout(); }
int Pool::getMinConnections() const { return m_attrs->getMinConnections(); }
int Pool::getMaxConnections() const { return m_attrs->getMaxConnections(); }
long Pool::getIdleTimeout() const { return m_attrs->getIdleTimeout(); }
long Pool::getPingInterval() const { return m_attrs->getPingInterval(); }
long Pool::getUpdateLocatorListInterval() const {
  return m_attrs->getUpdateLocatorListInterval();
}
int Pool::getStatisticInterval() const {
  return m_attrs->getStatisticInterval();
}
int Pool::getRetryAttempts() const { return m_attrs->getRetryAttempts(); }
// bool Pool::getThreadLocalConnections() const { return false; }
bool Pool::getSubscriptionEnabled() const {
  return m_attrs->getSubscriptionEnabled();
}
int Pool::getSubscriptionRedundancy() const {
  return m_attrs->getSubscriptionRedundancy();
}
int Pool::getSubscriptionMessageTrackingTimeout() const {
  return m_attrs->getSubscriptionMessageTrackingTimeout();
}
int Pool::getSubscriptionAckInterval() const {
  return m_attrs->getSubscriptionAckInterval();
}
const char* Pool::getServerGroup() const { return m_attrs->getServerGroup(); }
bool Pool::getThreadLocalConnections() const {
  return m_attrs->getThreadLocalConnectionSetting();
}
bool Pool::getMultiuserAuthentication() const {
  return m_attrs->getMultiuserSecureModeEnabled();
}
RegionServicePtr Pool::createSecureUserCache(PropertiesPtr credentials) {
  if (this->getMultiuserAuthentication()) {
    CachePtr realCache = CacheFactory::getAnyInstance();

    if (!(realCache != NULLPTR && realCache->m_cacheImpl != NULL)) {
      throw IllegalStateException("cache has not been created yet.");
      ;
    }

    if (realCache->isClosed()) {
      throw IllegalStateException("cache has been closed. ");
    }

    if (credentials != NULLPTR && credentials.ptr() == NULL) {
      LOGDEBUG("Pool::createSecureUserCache creds are null");
      credentials = NULLPTR;
    }

    PoolPtr tmpPool(this);
    // TODO: this will return cache with userattribtes
    ProxyCachePtr userCache(new ProxyCache(credentials, tmpPool));
    return userCache;
  }

  throw IllegalStateException(
      "This operation is only allowed when attached pool is in "
      "multiuserSecureMode");
  // return NULLPTR;
}
bool Pool::getPRSingleHopEnabled() const {
  return m_attrs->getPRSingleHopEnabled();
}
// void Pool::releaseThreadLocalConnection(){}

int Pool::getPendingEventCount() const {
  TcrConnectionManager& tccm = CacheImpl::getInstance()->tcrConnectionManager();
  if (!tccm.isDurable()) {
    LOGERROR("This operation should only be called by durable client.");
    throw IllegalStateException(
        "This operation should only be called by durable client");
  }
  PoolPtr currPool(this);
  ThinClientPoolHADMPtr poolHADM = dynCast<ThinClientPoolHADMPtr>(currPool);
  if (poolHADM->isReadyForEvent()) {
    LOGERROR("This operation should only be called before readyForEvents.");
    throw IllegalStateException(
        "This operation should only be called before readyForEvents");
  }
  return poolHADM->getPrimaryServerQueueSize();
}
