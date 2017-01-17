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
