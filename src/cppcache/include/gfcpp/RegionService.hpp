#pragma once

#ifndef GEODE_GFCPP_REGIONSERVICE_H_
#define GEODE_GFCPP_REGIONSERVICE_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "VectorT.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

class Region;
class QueryService;

/**
 * A RegionService provides access to existing {@link Region regions} that exist
 * in a {@link GeodeCache Geode cache}.
 * Regions can be obtained using {@link #getRegion}
 * and queried using {@link #getQueryService}.
 * The service should be {@link #close closed} to free up resources
 * once it is no longer needed.
 * Once it {@link #isClosed is closed} any attempt to use it or any {@link
 * Region regions}
 * obtained from it will cause a {@link CacheClosedException} to be thrown.
 * <p>
 * Instances of the interface are created using one of the following methods:
 * <ul>
 * <li> {@link CacheFactory#create()} creates a client instance of {@link
 * Cache}.
 * <li> {@link Cache#createAuthenticatedView(Properties)} creates a client
 * multiuser authenticated cache view.
 * </ul>
 * <p>
 *
 */

class CPPCACHE_EXPORT RegionService : public SharedBase {
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
  virtual bool isClosed() const = 0;

  /**
   * Terminates this object cache and releases all the local resources.
   * After this cache is closed, any further
   * method call on this cache or any region object will throw
   * <code>CacheClosedException</code>, unless otherwise noted.
   * If RegionService is created from {@link Cache#createAuthenticatedView" },
   * then it clears user related security data.
   * @param keepalive whether to keep a durable CQ kept alive for this user.
   * @throws CacheClosedException,  if the cache is already closed.
   */
  virtual void close() = 0;

  /** Look up a region with the name.
   *
   * @param name the region's name, such as <code>root</code>.
   * @returns region, or NULLPTR if no such region exists.
   */
  virtual RegionPtr getRegion(const char* name) = 0;

  /**
  * Gets the QueryService from which a new Query can be obtained.
  * @returns A smart pointer to the QueryService.
  */
  virtual QueryServicePtr getQueryService() = 0;

  /**
   * Returns a set of root regions in the cache. This set is a snapshot and
   * is not backed by the Cache. The vector passed in is cleared and the
   * regions are added to it.
   *
   * @param regions the returned set of
   * regions
   */
  virtual void rootRegions(VectorOfRegion& regions) = 0;

  /**
  * Returns a factory that can create a {@link PdxInstance}.
  * @param className the fully qualified class name that the PdxInstance will
  * become
  * when it is fully deserialized.
  * @return the factory
  */
  virtual PdxInstanceFactoryPtr createPdxInstanceFactory(
      const char* className) = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_REGIONSERVICE_H_
