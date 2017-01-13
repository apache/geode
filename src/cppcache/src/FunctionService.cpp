/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/FunctionService.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <ExecutionImpl.hpp>
#include <ProxyRegion.hpp>
#include <UserAttributes.hpp>
#include <ProxyCache.hpp>
#include <gfcpp/PoolManager.hpp>
#include <CacheRegionHelper.hpp>
#include <gfcpp/TypeHelper.hpp>

using namespace gemfire;

ExecutionPtr FunctionService::onRegion(RegionPtr region) {
  LOGDEBUG("FunctionService::onRegion(RegionPtr region)");
  if (region == NULLPTR) {
    throw NullPointerException("FunctionService::onRegion: region is null");
  }

  const PoolPtr& pool = region->getPool();

  if (pool == NULLPTR) {
    throw IllegalArgumentException("Pool attached with region is closed.");
  }
  ProxyCachePtr proxyCache = NULLPTR;

  if (pool->getMultiuserAuthentication()) {
    ProxyRegion* pr = dynamic_cast<ProxyRegion*>(region.ptr());
    if (pr != NULL) {
      LOGDEBUG("FunctionService::onRegion(RegionPtr region) proxy cache");
      // it is in multiuser mode
      proxyCache = pr->m_proxyCache;
      PoolPtr userAttachedPool = proxyCache->m_userAttributes->getPool();
      PoolPtr pool = PoolManager::find(userAttachedPool->getName());
      if (!(pool != NULLPTR && pool.ptr() == userAttachedPool.ptr() &&
            !pool->isDestroyed())) {
        throw IllegalStateException(
            "Pool has been closed with attached Logical Cache.");
      }
      RegionPtr tmpRegion;
      tmpRegion = NULLPTR;
      // getting real region to execute function on region
      if (!CacheFactory::getAnyInstance()->isClosed()) {
        CacheRegionHelper::getCacheImpl(CacheFactory::getAnyInstance().ptr())
            ->getRegion(region->getName(), tmpRegion);
      } else {
        throw IllegalStateException("Cache has been closed");
      }

      if (tmpRegion == NULLPTR) {
        throw IllegalStateException("Real region has been closed.");
      }
      region = tmpRegion;
    } else {
      throw IllegalArgumentException(
          "onRegion() argument region should have get from RegionService.");
    }
  }

  ExecutionPtr ptr(new ExecutionImpl(region, proxyCache, pool));
  return ptr;
}

ExecutionPtr FunctionService::onServerWithPool(const PoolPtr& pool) {
  if (pool == NULLPTR) {
    throw NullPointerException("FunctionService::onServer: pool is null");
  }
  if (pool->getMultiuserAuthentication()) {
    throw UnsupportedOperationException(
        "This API is not supported in multiuser mode. "
        "Please use FunctionService::onServer(RegionService) API.");
  }
  ExecutionPtr ptr(new ExecutionImpl(pool));
  return ptr;
}

ExecutionPtr FunctionService::onServersWithPool(const PoolPtr& pool) {
  if (pool == NULLPTR) {
    throw NullPointerException("FunctionService::onServers: pool is null");
  }
  if (pool->getMultiuserAuthentication()) {
    throw UnsupportedOperationException(
        "This API is not supported in multiuser mode. "
        "Please use FunctionService::onServers(RegionService) API.");
  }

  ExecutionPtr ptr(new ExecutionImpl(pool, true));
  return ptr;
}

ExecutionPtr FunctionService::onServerWithCache(const RegionServicePtr& cache) {
  if (cache->isClosed()) {
    throw IllegalStateException("Cache has been closed");
  }

  ProxyCache* pc = dynamic_cast<ProxyCache*>(cache.ptr());

  LOGDEBUG("FunctionService::onServer:");
  if (pc != NULL) {
    PoolPtr userAttachedPool = pc->m_userAttributes->getPool();
    PoolPtr pool = PoolManager::find(userAttachedPool->getName());
    if (pool != NULLPTR && pool.ptr() == userAttachedPool.ptr() &&
        !pool->isDestroyed()) {
      ExecutionPtr ptr(new ExecutionImpl(pool, false, cache));
      return ptr;
    }
    throw IllegalStateException(
        "Pool has been close to execute function on server");
  } else {
    CachePtr realcache = staticCast<CachePtr>(cache);
    return FunctionService::onServer(realcache->m_cacheImpl->getDefaultPool());
  }
}

ExecutionPtr FunctionService::onServersWithCache(
    const RegionServicePtr& cache) {
  if (cache->isClosed()) {
    throw IllegalStateException("Cache has been closed");
  }

  ProxyCache* pc = dynamic_cast<ProxyCache*>(cache.ptr());

  LOGDEBUG("FunctionService::onServers:");
  if (pc != NULL && !cache->isClosed()) {
    PoolPtr userAttachedPool = pc->m_userAttributes->getPool();
    PoolPtr pool = PoolManager::find(userAttachedPool->getName());
    if (pool != NULLPTR && pool.ptr() == userAttachedPool.ptr() &&
        !pool->isDestroyed()) {
      ExecutionPtr ptr(new ExecutionImpl(pool, true, cache));
      return ptr;
    }
    throw IllegalStateException(
        "Pool has been close to execute function on server");
  } else {
    CachePtr realcache = staticCast<CachePtr>(cache);
    return FunctionService::onServers(realcache->m_cacheImpl->getDefaultPool());
  }
}
