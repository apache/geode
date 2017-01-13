/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
#include <CacheImpl.hpp>
#include <gfcpp/PoolFactory.hpp>
#include <gfcpp/Pool.hpp>
#include <PoolAttributes.hpp>
#include <ThinClientPoolDM.hpp>
#include <ThinClientPoolHADM.hpp>
#include <gfcpp/SystemProperties.hpp>
#include <gfcpp/PoolManager.hpp>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/INET_Addr.h>
#include <ThinClientPoolStickyDM.hpp>
#include <ThinClientPoolStickyHADM.hpp>
using namespace gemfire;
const char* PoolFactory::DEFAULT_SERVER_GROUP = "";
extern HashMapOfPools* connectionPools;
extern ACE_Recursive_Thread_Mutex connectionPoolsLock;

PoolFactory::PoolFactory()
    : m_attrs(new PoolAttributes),
      m_isSubscriptionRedundancy(false),
      m_addedServerOrLocator(false) {}

PoolFactory::~PoolFactory() {}

void PoolFactory::setFreeConnectionTimeout(int connectionTimeout) {
  m_attrs->setFreeConnectionTimeout(connectionTimeout);
}
void PoolFactory::setLoadConditioningInterval(int loadConditioningInterval) {
  m_attrs->setLoadConditioningInterval(loadConditioningInterval);
}
void PoolFactory::setSocketBufferSize(int bufferSize) {
  m_attrs->setSocketBufferSize(bufferSize);
}
void PoolFactory::setThreadLocalConnections(bool threadLocalConnections) {
  m_attrs->setThreadLocalConnectionSetting(threadLocalConnections);
}
void PoolFactory::setReadTimeout(int timeout) {
  m_attrs->setReadTimeout(timeout);
}
void PoolFactory::setMinConnections(int minConnections) {
  m_attrs->setMinConnections(minConnections);
}
void PoolFactory::setMaxConnections(int maxConnections) {
  m_attrs->setMaxConnections(maxConnections);
}
void PoolFactory::setIdleTimeout(long idleTimeout) {
  m_attrs->setIdleTimeout(idleTimeout);
}
void PoolFactory::setRetryAttempts(int retryAttempts) {
  m_attrs->setRetryAttempts(retryAttempts);
}
void PoolFactory::setPingInterval(long pingInterval) {
  m_attrs->setPingInterval(pingInterval);
}
void PoolFactory::setUpdateLocatorListInterval(long updateLocatorListInterval) {
  m_attrs->setUpdateLocatorListInterval(updateLocatorListInterval);
}
void PoolFactory::setStatisticInterval(int statisticInterval) {
  m_attrs->setStatisticInterval(statisticInterval);
}
void PoolFactory::setServerGroup(const char* group) {
  m_attrs->setServerGroup(group);
}
void PoolFactory::addLocator(const char* host, int port) {
  addCheck(host, port);
  m_attrs->addLocator(host, port);
  m_addedServerOrLocator = true;
}
void PoolFactory::addServer(const char* host, int port) {
  addCheck(host, port);
  m_attrs->addServer(host, port);
  m_addedServerOrLocator = true;
}
void PoolFactory::setSubscriptionEnabled(bool enabled) {
  m_attrs->setSubscriptionEnabled(enabled);
}
void PoolFactory::setSubscriptionRedundancy(int redundancy) {
  m_isSubscriptionRedundancy = true;
  m_attrs->setSubscriptionRedundancy(redundancy);
}
void PoolFactory::setSubscriptionMessageTrackingTimeout(
    int messageTrackingTimeout) {
  m_attrs->setSubscriptionMessageTrackingTimeout(messageTrackingTimeout);
}
void PoolFactory::setSubscriptionAckInterval(int ackInterval) {
  m_attrs->setSubscriptionAckInterval(ackInterval);
}
void PoolFactory::setMultiuserAuthentication(bool multiuserAuthentication) {
  m_attrs->setMultiuserSecureModeEnabled(multiuserAuthentication);
}

void PoolFactory::reset() { m_attrs = PoolAttributesPtr(new PoolAttributes); }

void PoolFactory::setPRSingleHopEnabled(bool enabled) {
  m_attrs->setPRSingleHopEnabled(enabled);
}

PoolPtr PoolFactory::create(const char* name) {
  ThinClientPoolDMPtr poolDM;
  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(connectionPoolsLock);

    if (PoolManager::find(name) != NULLPTR) {
      throw IllegalStateException("Pool with the same name already exists");
    }
    // Create a clone of Attr;
    PoolAttributesPtr copyAttrs = m_attrs->clone();

    if (CacheImpl::getInstance() == NULL) {
      throw IllegalStateException("Cache has not been created.");
    }

    if (CacheImpl::getInstance()->isClosed()) {
      throw CacheClosedException("Cache is closed");
    }
    if (CacheImpl::getInstance()->getCacheMode() &&
        m_isSubscriptionRedundancy) {
      LOGWARN(
          "At least one pool has been created so ignoring cache level "
          "redundancy setting");
    }
    TcrConnectionManager& tccm =
        CacheImpl::getInstance()->tcrConnectionManager();
    LOGDEBUG("PoolFactory::create mulitusermode = %d ",
             copyAttrs->getMultiuserSecureModeEnabled());
    if (copyAttrs->getMultiuserSecureModeEnabled()) {
      if (copyAttrs->getThreadLocalConnectionSetting()) {
        LOGERROR(
            "When pool [%s] is in multiuser authentication mode then thread "
            "local connections are not supported.",
            name);
        throw IllegalArgumentException(
            "When pool is in multiuser authentication mode then thread local "
            "connections are not supported.");
      }
    }
    if (!copyAttrs->getSubscriptionEnabled() &&
        copyAttrs->getSubscriptionRedundancy() == 0 && !tccm.isDurable()) {
      if (copyAttrs
              ->getThreadLocalConnectionSetting() /*&& !copyAttrs->getPRSingleHopEnabled()*/) {
        // TODO: what should we do for sticky connections
        poolDM = new ThinClientPoolStickyDM(name, copyAttrs, tccm);
      } else {
        LOGDEBUG("ThinClientPoolDM created ");
        poolDM = new ThinClientPoolDM(name, copyAttrs, tccm);
      }
    } else {
      LOGDEBUG("ThinClientPoolHADM created ");
      if (copyAttrs
              ->getThreadLocalConnectionSetting() /*&& !copyAttrs->getPRSingleHopEnabled()*/) {
        poolDM = new ThinClientPoolStickyHADM(name, copyAttrs, tccm);
      } else {
        poolDM = new ThinClientPoolHADM(name, copyAttrs, tccm);
      }
    }

    connectionPools->insert(CacheableString::create(name),
                            staticCast<PoolPtr>(poolDM));
  }

  // TODO: poolDM->init() should not throw exceptions!
  // Pool DM should only be inited once.
  if (DistributedSystem::getSystemProperties()->autoReadyForEvents()) {
    poolDM->init();
  }

  return staticCast<PoolPtr>(poolDM);
}

void PoolFactory::addCheck(const char* host, int port) {
  if (port <= 0) {
    char buff[100];
    ACE_OS::snprintf(buff, 100, "port must be greater than 0 but was %d", port);
    throw IllegalArgumentException(buff);
  }
  ACE_INET_Addr addr(port, host);
  if (!(addr.get_ip_address())) {
    char buff[100];
    ACE_OS::snprintf(buff, 100, "Unknown host %s", host);
    throw IllegalArgumentException(buff);
  }
}
