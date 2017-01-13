/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ProxyRemoteQueryService.hpp"
#include "ThinClientPoolDM.hpp"
#include <gfcpp/PoolManager.hpp>
#include "CqQueryImpl.hpp"

ProxyRemoteQueryService::ProxyRemoteQueryService(ProxyCache* cptr) {
  ProxyCachePtr pcp(cptr);
  m_proxyCache = pcp;
}

QueryPtr ProxyRemoteQueryService::newQuery(const char* querystring) {
  if (!m_proxyCache->isClosed()) {
    PoolPtr userAttachedPool = m_proxyCache->m_userAttributes->getPool();
    PoolPtr pool = PoolManager::find(userAttachedPool->getName());
    if (pool != NULLPTR && pool.ptr() == userAttachedPool.ptr() &&
        !pool->isDestroyed()) {
      GuardUserAttribures gua(m_proxyCache);
      ThinClientPoolDMPtr pooDM(static_cast<ThinClientPoolDM*>(pool.ptr()));
      if (!pooDM->isDestroyed()) {
        return pooDM->getQueryServiceWithoutCheck()->newQuery(querystring);
      }
    }
    throw IllegalStateException("Pool has been closed.");
  }
  throw IllegalStateException("UserCache has been closed.");
}

void ProxyRemoteQueryService::unSupportedException(const char* operationName) {
  char msg[256] = {'\0'};
  ACE_OS::snprintf(msg, 256,
                   "%s operation is not supported when pool is in multiuser "
                   "authentication mode.",
                   operationName);
  throw UnsupportedOperationException(msg);
}

CqQueryPtr ProxyRemoteQueryService::newCq(const char* querystr,
                                          CqAttributesPtr& cqAttr,
                                          bool isDurable) {
  if (!m_proxyCache->isClosed()) {
    PoolPtr userAttachedPool = m_proxyCache->m_userAttributes->getPool();
    PoolPtr pool = PoolManager::find(userAttachedPool->getName());
    if (pool != NULLPTR && pool.ptr() == userAttachedPool.ptr() &&
        !pool->isDestroyed()) {
      GuardUserAttribures gua(m_proxyCache);
      ThinClientPoolDMPtr pooDM(static_cast<ThinClientPoolDM*>(pool.ptr()));
      if (!pooDM->isDestroyed()) {
        CqQueryPtr cqQuery = pooDM->getQueryServiceWithoutCheck()->newCq(
            querystr, cqAttr, isDurable);
        addCqQuery(cqQuery);
        return cqQuery;
      }
    }
    throw IllegalStateException("Pool has been closed.");
  }
  throw IllegalStateException("Logical Cache has been closed.");
}

void ProxyRemoteQueryService::addCqQuery(const CqQueryPtr& cqQuery) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_cqQueryListLock);
  m_cqQueries.push_back(cqQuery);
}

CqQueryPtr ProxyRemoteQueryService::newCq(const char* name,
                                          const char* querystr,
                                          CqAttributesPtr& cqAttr,
                                          bool isDurable) {
  if (!m_proxyCache->isClosed()) {
    PoolPtr userAttachedPool = m_proxyCache->m_userAttributes->getPool();
    PoolPtr pool = PoolManager::find(userAttachedPool->getName());
    if (pool != NULLPTR && pool.ptr() == userAttachedPool.ptr() &&
        !pool->isDestroyed()) {
      GuardUserAttribures gua(m_proxyCache);
      ThinClientPoolDMPtr pooDM(static_cast<ThinClientPoolDM*>(pool.ptr()));
      if (!pooDM->isDestroyed()) {
        CqQueryPtr cqQuery = pooDM->getQueryServiceWithoutCheck()->newCq(
            name, querystr, cqAttr, isDurable);
        addCqQuery(cqQuery);
        return cqQuery;
      }
    }
    throw IllegalStateException("Pool has been closed.");
  }
  throw IllegalStateException("Logical Cache has been closed.");
}

void ProxyRemoteQueryService::closeCqs() { closeCqs(false); }

void ProxyRemoteQueryService::closeCqs(bool keepAlive) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_cqQueryListLock);

  for (int32_t i = 0; i < m_cqQueries.size(); i++) {
    std::string cqName = m_cqQueries[i]->getName();
    try {
      if (!(m_cqQueries[i]->isDurable() && keepAlive)) {
        m_cqQueries[i]->close();
      } else {
        // need to just cleanup client side data structure
        CqQueryImpl* cqImpl = static_cast<CqQueryImpl*>(m_cqQueries[i].ptr());
        cqImpl->close(false);
      }
    } catch (QueryException& qe) {
      Log::fine(("Failed to close the CQ, CqName : " + cqName + " Error : " +
                 qe.getMessage())
                    .c_str());
    } catch (CqClosedException& cce) {
      Log::fine(("Failed to close the CQ, CqName : " + cqName + " Error : " +
                 cce.getMessage())
                    .c_str());
    }
  }
}

void ProxyRemoteQueryService::getCqs(VectorOfCqQuery& vec) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_cqQueryListLock);

  for (int32_t i = 0; i < m_cqQueries.size(); i++) {
    vec.push_back(m_cqQueries[i]);
  }
}

CqQueryPtr ProxyRemoteQueryService::getCq(const char* name) {
  if (!m_proxyCache->isClosed()) {
    PoolPtr userAttachedPool = m_proxyCache->m_userAttributes->getPool();
    PoolPtr pool = PoolManager::find(userAttachedPool->getName());
    if (pool != NULLPTR && pool.ptr() == userAttachedPool.ptr() &&
        !pool->isDestroyed()) {
      GuardUserAttribures gua(m_proxyCache);
      ThinClientPoolDMPtr pooDM(static_cast<ThinClientPoolDM*>(pool.ptr()));
      if (!pooDM->isDestroyed()) {
        return pooDM->getQueryServiceWithoutCheck()->getCq(name);
      }
    }
    throw IllegalStateException("Pool has been closed.");
  }
  throw IllegalStateException("Logical Cache has been closed.");
}

void ProxyRemoteQueryService::executeCqs() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_cqQueryListLock);

  for (int32_t i = 0; i < m_cqQueries.size(); i++) {
    std::string cqName = m_cqQueries[i]->getName();
    try {
      m_cqQueries[i]->execute();
    } catch (QueryException& qe) {
      Log::fine(("Failed to excecue the CQ, CqName : " + cqName + " Error : " +
                 qe.getMessage())
                    .c_str());
    } catch (CqClosedException& cce) {
      Log::fine(("Failed to excecue the CQ, CqName : " + cqName + " Error : " +
                 cce.getMessage())
                    .c_str());
    }
  }
}

void ProxyRemoteQueryService::stopCqs() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_cqQueryListLock);

  for (int32_t i = 0; i < m_cqQueries.size(); i++) {
    std::string cqName = m_cqQueries[i]->getName();
    try {
      m_cqQueries[i]->stop();
    } catch (QueryException& qe) {
      Log::fine(("Failed to stop the CQ, CqName : " + cqName + " Error : " +
                 qe.getMessage())
                    .c_str());
    } catch (CqClosedException& cce) {
      Log::fine(("Failed to stop the CQ, CqName : " + cqName + " Error : " +
                 cce.getMessage())
                    .c_str());
    }
  }
}

CqServiceStatisticsPtr ProxyRemoteQueryService::getCqServiceStatistics() {
  unSupportedException("getCqServiceStatistics()");
  return NULLPTR;
}

CacheableArrayListPtr ProxyRemoteQueryService::getAllDurableCqsFromServer() {
  unSupportedException("getAllDurableCqsFromServer()");
  return NULLPTR;
}
