#pragma once

#ifndef GEODE_PROXYCACHE_H_
#define GEODE_PROXYCACHE_H_

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
#include <gfcpp/gf_types.hpp>
#include <gfcpp/Region.hpp>
#include <gfcpp/DistributedSystem.hpp>
#include <gfcpp/QueryService.hpp>
#include <gfcpp/PoolFactory.hpp>
#include "UserAttributes.hpp"
#include <gfcpp/RegionService.hpp>

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

class FunctionServiceImpl;

/**
 * @class Cache Cache.hpp
 *
 * Caches are obtained from static methods on the {@link CacheFactory} class
 * <p>
 * When a cache is created a {@link DistributedSystem} must be specified.
 * <p>
 * When a cache will no longer be used, it should be {@link #close closed}.
 * Once it {@link Cache::isClosed is closed} any attempt to use it
 * will cause a <code>CacheClosedException</code> to be thrown.
 *
 * <p>A cache can have multiple root regions, each with a different name.
 *
 */
class CPPCACHE_EXPORT ProxyCache : public RegionService {
  /**
   * @brief public methods
   */
 public:
  /**
   * Indicates if this cache has been closed.
   * After a new cache object is created, this method returns false;
   * After the close is called on this cache object, this method
   * returns true.
   *
   * @return true, if this cache is closed; false, otherwise
   */
  virtual bool isClosed() const;

  /**
   * Terminates this object cache and releases all the local resources.
   * After this cache is closed, any further
   * method call on this cache or any region object will throw
   * <code>CacheClosedException</code>, unless otherwise noted.
   * @param keepalive whether to keep a durable client's queue alive
   * @throws CacheClosedException,  if the cache is already closed.
   */
  virtual void close();

  /** Look up a region with the full path from root.
   * @param path the region's path, such as <code>RootA/Sub1/Sub1A</code>.
   * @returns region, or NULLPTR if no such region exists.
   */
  virtual RegionPtr getRegion(const char* path);

  /**
  * Gets the QueryService from which a new Query can be obtained.
  *
  * @returns A smart pointer to the QueryService.
  */
  virtual QueryServicePtr getQueryService();

  /**
  * Returns a set of root regions in the cache. This set is a snapshot and
  * is not backed by the Cache. The vector passed in is cleared and the
  * regions are added to it.
  *
  * @param regions the returned set of
  * regions
  */
  virtual void rootRegions(VectorOfRegion& regions);

  /**
    * @brief destructor
    */
  virtual ~ProxyCache();

  ProxyCache(PropertiesPtr credentials, PoolPtr pool);

  /**
   * Returns a factory that can create a {@link PdxInstance}.
   * @param className the fully qualified class name that the PdxInstance will
   * become
   * when it is fully deserialized.
   * @return the factory
   */
  virtual PdxInstanceFactoryPtr createPdxInstanceFactory(const char* className);

 private:
  /**
   * @brief constructors
   */

  UserAttributesPtr m_userAttributes;
  bool m_isProxyCacheClosed;
  QueryServicePtr m_remoteQueryService;
  friend class Pool;
  friend class ProxyRegion;
  friend class ProxyRemoteQueryService;
  friend class RemoteQuery;
  friend class ExecutionImpl;
  friend class FunctionServiceImpl;
  friend class FunctionService;
  friend class GuardUserAttribures;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_PROXYCACHE_H_
