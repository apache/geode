/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "RemoteQueryService.hpp"
#include "CacheImpl.hpp"
#include "RemoteQuery.hpp"
#include "ReadWriteLock.hpp"
#include "CqServiceVsdStats.hpp"
#include "ThinClientPoolDM.hpp"
#include "UserAttributes.hpp"

using namespace gemfire;

RemoteQueryService::RemoteQueryService(CacheImpl* cptr,
                                       ThinClientPoolDM* poolDM)
    : m_invalid(true), m_cqService(NULLPTR) {
  if (poolDM) {
    m_tccdm = poolDM;
  } else {
    m_tccdm =
        new ThinClientCacheDistributionManager(cptr->tcrConnectionManager());
  }
  // m_cqService = new CqService(m_tccdm);
  // m_tccdm->init();
  LOGFINEST("Initialized m_tccdm");
}

void RemoteQueryService::init() {
  TryWriteGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    LOGFINEST("RemoteQueryService::init: initializing TCCDM");
    if (dynamic_cast<ThinClientCacheDistributionManager*>(m_tccdm)) {
      m_tccdm->init();
    }
    m_invalid = false;
    LOGFINEST("RemoteQueryService::init: done initialization");
  }
}

QueryPtr RemoteQueryService::newQuery(const char* querystring) {
  LOGDEBUG("RemoteQueryService::newQuery: multiuserMode = %d ",
           m_tccdm->isMultiUserMode());
  if (!m_tccdm->isMultiUserMode()) {
    TryReadGuard guard(m_rwLock, m_invalid);

    if (m_invalid) {
      throw CacheClosedException(
          "QueryService::newQuery: Cache has been closed.");
    }
    LOGDEBUG("RemoteQueryService: creating a new query: %s", querystring);
    return QueryPtr(
        new RemoteQuery(querystring, RemoteQueryServicePtr(this), m_tccdm));
  } else {
    UserAttributesPtr ua = TSSUserAttributesWrapper::s_gemfireTSSUserAttributes
                               ->getUserAttributes();
    TryReadGuard guard(m_rwLock, m_invalid);

    if (m_invalid) {
      throw CacheClosedException(
          "QueryService::newQuery: Cache has been closed.");
    }
    LOGDEBUG("RemoteQueryService: creating a new query: %s", querystring);
    return QueryPtr(new RemoteQuery(querystring, RemoteQueryServicePtr(this),
                                    m_tccdm, ua->getProxyCache()));
  }
}

void RemoteQueryService::close() {
  LOGFINEST("RemoteQueryService::close: starting close");
  TryWriteGuard guard(m_rwLock, m_invalid);
  if (m_cqService != NULLPTR) {
    LOGFINEST("RemoteQueryService::close: starting CQ service close");
    m_cqService->closeCqService();
    m_cqService = NULLPTR;
    LOGFINEST("RemoteQueryService::close: completed CQ service close");
  }
  if (dynamic_cast<ThinClientCacheDistributionManager*>(m_tccdm)) {
    if (!m_invalid) {
      LOGFINEST("RemoteQueryService::close: destroying DM");
      m_tccdm->destroy();
    }
    GF_SAFE_DELETE(m_tccdm);
    m_invalid = true;
  }
  if (!m_CqPoolsConnected.empty()) {
    m_CqPoolsConnected.clear();
  }
  LOGFINEST("RemoteQueryService::close: completed");
}

/**
 * execute all cqs on the endpoint after failover
 */
GfErrType RemoteQueryService::executeAllCqs(TcrEndpoint* endpoint) {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    LOGFINE("QueryService::executeAllCqs(endpoint): Not initialized.");
    return GF_NOERR;
  }

  if (m_cqService == NULLPTR) {
    LOGFINE(
        "RemoteQueryService: no cq to execute after failover to endpoint[%s]",
        endpoint->name().c_str());
    return GF_NOERR;
  } else {
    LOGFINE(
        "RemoteQueryService: execute all cqs after failover to endpoint[%s]",
        endpoint->name().c_str());
    return m_cqService->executeAllClientCqs(endpoint);
  }
}

void RemoteQueryService::executeAllCqs(bool failover) {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    LOGFINE("QueryService::executeAllCqs: Not initialized.");
    return;
  }
  /*if cq has not been started, then failover will not start it.*/
  if (m_cqService != NULLPTR) {
    LOGFINE("RemoteQueryService: execute all cqs after failover");
    m_cqService->executeAllClientCqs(failover);
  } else {
    LOGFINE("RemoteQueryService: no cq to execute after failover");
  }
}

CqQueryPtr RemoteQueryService::newCq(const char* querystr,
                                     CqAttributesPtr& cqAttr, bool isDurable) {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    throw CacheClosedException("QueryService::newCq: Cache has been closed.");
  }
  initCqService();
  std::string qs(querystr);
  // use query string as name for now
  std::string name("_default");
  name += querystr;
  return m_cqService->newCq(name, qs, cqAttr, isDurable);
}

CqQueryPtr RemoteQueryService::newCq(const char* name, const char* querystr,
                                     CqAttributesPtr& cqAttr, bool isDurable) {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    throw CacheClosedException("QueryService::newCq: Cache has been closed.");
  }
  initCqService();
  std::string qs(querystr);
  std::string nm(name);
  return m_cqService->newCq(nm, qs, cqAttr, isDurable);
}

void RemoteQueryService::closeCqs() {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    LOGFINE("QueryService::closeCqs: Cache has been closed.");
    return;
  }
  // If cqService has not started, then no cq exists
  if (m_cqService != NULLPTR) {
    m_cqService->closeAllCqs();
  }
}

void RemoteQueryService::getCqs(VectorOfCqQuery& vec) {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    throw CacheClosedException("QueryService::getCqs: Cache has been closed.");
  }
  // If cqService has not started, then no cq exists
  if (m_cqService != NULLPTR) {
    m_cqService->getAllCqs(vec);
  }
}

CqQueryPtr RemoteQueryService::getCq(const char* name) {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    throw CacheClosedException("QueryService::getCq: Cache has been closed.");
  }
  // If cqService has not started, then no cq exists
  if (m_cqService != NULLPTR) {
    std::string nm(name);
    return m_cqService->getCq(nm);
  }
  return NULLPTR;
}

void RemoteQueryService::executeCqs() {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    throw CacheClosedException(
        "QueryService::executeCqs: Cache has been closed.");
  }
  // If cqService has not started, then no cq exists
  if (m_cqService != NULLPTR) {
    m_cqService->executeAllClientCqs();
  }
}

void RemoteQueryService::stopCqs() {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    LOGFINE("QueryService::stopCqs: Cache has been closed.");
    return;
  }
  // If cqService has not started, then no cq exists
  if (m_cqService != NULLPTR) {
    m_cqService->stopAllClientCqs();
  }
}

CqServiceStatisticsPtr RemoteQueryService::getCqServiceStatistics() {
  TryReadGuard guard(m_rwLock, m_invalid);

  if (m_invalid) {
    throw CacheClosedException(
        "QueryService::getCqServiceStatistics: Cache has been closed.");
  }
  // If cqService has not started, then no cq exists
  if (m_cqService != NULLPTR) {
    return m_cqService->getCqServiceStatistics();
  }
  CqServiceStatisticsPtr ptr(new CqServiceVsdStats());
  return ptr;
}

void RemoteQueryService::receiveNotification(TcrMessage* msg) {
  {
    TryReadGuard guard(m_rwLock, m_invalid);
    if (m_invalid) {
      //  do we need this check?
      return;
    }
    /*if cq has not been started, then  no cq exists */
    if (m_cqService == NULLPTR) {
      return;
    }
    if (!m_cqService->checkAndAcquireLock()) {
      return;
    }
  }

  m_cqService->receiveNotification(msg);
}

CacheableArrayListPtr RemoteQueryService::getAllDurableCqsFromServer() {
  TryReadGuard guard(m_rwLock, m_invalid);
  if (m_invalid) {
    throw CacheClosedException(
        "QueryService::getAllDurableCqsFromServer: Cache has been closed.");
  }
  // If cqService has not started, then no cq exists
  if (m_cqService != NULLPTR) {
    return m_cqService->getAllDurableCqsFromServer();
  } else {
    return NULLPTR;
  }
}

void RemoteQueryService::invokeCqConnectedListeners(ThinClientPoolDM* pool,
                                                    bool connected) {
  if (m_cqService == NULLPTR) {
    return;
  }
  std::string poolName;
  pool = dynamic_cast<ThinClientPoolDM*>(m_tccdm);
  if (pool != NULL) {
    poolName = pool->getName();
    CqPoolsConnected::iterator itr = m_CqPoolsConnected.find(poolName);
    if (itr != m_CqPoolsConnected.end() && itr->second == connected) {
      LOGDEBUG("Returning since pools connection status matched.");
      return;
    } else {
      LOGDEBUG("Inserting since pools connection status did not match.");
      m_CqPoolsConnected[poolName] = connected;
    }
  }
  m_cqService->invokeCqConnectedListeners(poolName, connected);
}
