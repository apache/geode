/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>

#include <gfcpp/DistributedSystem.hpp>
#include "DistributedSystemImpl.hpp"
#include "CacheXmlParser.hpp"
#include "CacheRegionHelper.hpp"
#include <gfcpp/Cache.hpp>
#include "CacheImpl.hpp"
#include "UserAttributes.hpp"
#include "ProxyRegion.hpp"
#include <gfcpp/FunctionService.hpp>
#include "ProxyRemoteQueryService.hpp"
#include "FunctionServiceImpl.hpp"
#include "ProxyCache.hpp"
#include <string.h>
#include <gfcpp/PoolManager.hpp>
#include "ThinClientPoolDM.hpp"
#include "PdxInstanceFactoryImpl.hpp"

using namespace gemfire;

/**
 * Indicates if this cache has been closed.
 * After a new cache object is created, this method returns false;
 * After the close is called on this cache object, this method
 * returns true.
 *
 * @return true, if this cache is closed; false, otherwise
 */
bool ProxyCache::isClosed() const { return m_isProxyCacheClosed; }

/**
 * Terminates this object cache and releases all the local resources.
 * After this cache is closed, any further
 * method call on this cache or any region object will throw
 * <code>CacheClosedException</code>, unless otherwise noted.
 * @param keepalive whether to keep the durable client's queue
 * @throws CacheClosedException,  if the cache is already closed.
 */
void ProxyCache::close() {
  LOGDEBUG("ProxyCache::close: isProxyCacheClosed = %d", m_isProxyCacheClosed);
  if (!m_isProxyCacheClosed) {
    if (m_remoteQueryService != NULLPTR) {
      ProxyRemoteQueryService* prqs =
          static_cast<ProxyRemoteQueryService*>(m_remoteQueryService.ptr());
      prqs->closeCqs(false);
    }

    ProxyCachePtr pcp(this);
    GuardUserAttribures gua(pcp);
    m_isProxyCacheClosed = true;
    m_userAttributes->unSetCredentials();
    // send message to server
    PoolPtr userAttachedPool = m_userAttributes->getPool();
    PoolPtr pool = PoolManager::find(userAttachedPool->getName());
    if (pool != NULLPTR && pool.ptr() == userAttachedPool.ptr()) {
      ThinClientPoolDMPtr poolDM(static_cast<ThinClientPoolDM*>(pool.ptr()));
      if (!poolDM->isDestroyed()) {
        poolDM->sendUserCacheCloseMessage(false);
      }
    }
    return;
  }
  throw IllegalStateException("User cache has been closed.");
}

RegionPtr ProxyCache::getRegion(const char* path) {
  LOGDEBUG("ProxyCache::getRegion:");

  if (!m_isProxyCacheClosed) {
    RegionPtr result;
    CachePtr realCache = CacheFactory::getAnyInstance();

    if (realCache != NULLPTR && !realCache->isClosed()) {
      CacheRegionHelper::getCacheImpl(realCache.ptr())->getRegion(path, result);
    }

    if (result != NULLPTR) {
      PoolPtr userAttachedPool = m_userAttributes->getPool();
      PoolPtr pool = PoolManager::find(result->getAttributes()->getPoolName());
      if (pool != NULLPTR && pool.ptr() == userAttachedPool.ptr() &&
          !pool->isDestroyed()) {
        ProxyRegionPtr pRegion(new ProxyRegion(this, result));
        return pRegion;
      }
      throw IllegalArgumentException(
          "The Region argument is not attached with the pool, which used to "
          "create this user cache.");
    }

    return result;
  }
  throw IllegalStateException("User cache has been closed.");
}

/**
 * Returns a set of root regions in the cache. Does not cause any
 * shared regions to be mapped into the cache. This set is a snapshot and
 * is not backed by the Cache. The regions passed in are cleared.
 *
 * @param regions the region collection object containing the returned set of
 * regions when the function returns
 */

QueryServicePtr ProxyCache::getQueryService() {
  if (!m_isProxyCacheClosed) {
    if (m_remoteQueryService != NULLPTR) return m_remoteQueryService;
    QueryServicePtr prqsPtr(new ProxyRemoteQueryService(this));
    m_remoteQueryService = prqsPtr;
    return prqsPtr;
  }
  throw IllegalStateException("User cache has been closed.");
}

void ProxyCache::rootRegions(VectorOfRegion& regions) {
  LOGDEBUG("ProxyCache::rootRegions:");

  if (!m_isProxyCacheClosed) {
    RegionPtr result;
    CachePtr realCache = CacheFactory::getAnyInstance();

    if (realCache != NULLPTR && !realCache->isClosed()) {
      VectorOfRegion tmp;
      // this can cause issue when pool attached with region in multiuserSecure
      // mode
      realCache->rootRegions(tmp);

      if (tmp.size() > 0) {
        for (int32_t i = 0; i < tmp.size(); i++) {
          RegionPtr reg = tmp.at(i);
          if (strcmp(m_userAttributes->getPool()->getName(),
                     reg->getAttributes()->getPoolName()) == 0) {
            ProxyRegionPtr pRegion(new ProxyRegion(this, reg));
            regions.push_back(pRegion);
          }
        }
      }
    }
  }
}

ProxyCache::ProxyCache(PropertiesPtr credentials, PoolPtr pool) {
  m_remoteQueryService = NULLPTR;
  m_isProxyCacheClosed = false;
  UserAttributesPtr userAttr;
  userAttr = new UserAttributes(credentials, pool, this);
  m_userAttributes = userAttr;
}

ProxyCache::~ProxyCache() {}

PdxInstanceFactoryPtr ProxyCache::createPdxInstanceFactory(
    const char* className) {
  PdxInstanceFactoryPtr pIFPtr(new PdxInstanceFactoryImpl(className));
  return pIFPtr;
}
