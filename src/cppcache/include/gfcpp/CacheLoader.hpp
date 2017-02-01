#pragma once

#ifndef GEODE_GFCPP_CACHELOADER_H_
#define GEODE_GFCPP_CACHELOADER_H_

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

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CacheableKey.hpp"
#include "Cacheable.hpp"
#include "UserData.hpp"

namespace apache {
namespace geode {
namespace client {

/**
 * @class CacheLoader CacheLoader.hpp
 * An application plug-in that can be installed on a region. Loaders
 * facilitate loading of data into the cache. When an application does a
 * lookup for a key in a region and it does not exist, the system checks to
 * see if any loaders are available for the region in the system and
 * invokes them to get the value for the key into the cache.
 * Allows data to be loaded from a 3rd party data source and placed
 * into the region
 * When {@link Region::get} is called for a region
 * entry that has a <code>NULLPTR</code> value, the
 * {@link CacheLoader::load} method of the
 * region's cache loader is invoked.  The <code>load</code> method
 * creates the value for the desired key by performing an operation such
 * as a database query.
 *
 * @see AttributesFactory::setCacheLoader
 * @see RegionAttributes::getCacheLoader
 */
class CPPCACHE_EXPORT CacheLoader : public SharedBase {
 public:
  /**Loads a value. Application writers should implement this
   * method to customize the loading of a value. This method is called
   * by the caching service when the requested value is not in the cache.
   * Any exception thrown by this method is propagated back to and thrown
   * by the invocation of {@link Region::get} that triggered this load.
   * @param rp a Region Pointer for which this is called.
   * @param key the key for the cacheable
   * @param helper any related user data, or NULLPTR
   * @return the value supplied for this key, or NULLPTR if no value can be
   * supplied.
   *
   *@see Region::get .
   */
  virtual CacheablePtr load(const RegionPtr& rp, const CacheableKeyPtr& key,
                            const UserDataPtr& aCallbackArgument) = 0;

  /** Called when the region containing this callback is destroyed, when
   * the cache is closed.
   *
   * <p>Implementations should clean up any external
   * resources, such as database connections. Any runtime exceptions this method
   * throws will be logged.
   *
   * <p>It is possible for this method to be called multiple times on a single
   * callback instance, so implementations must be tolerant of this.
   *
   * @param rp the region pointer
   *
   * @see Cache::close
   * @see Region::destroyRegion
   */
  virtual void close(const RegionPtr& rp);

  virtual ~CacheLoader();

 protected:
  CacheLoader();

 protected:
  // never implemented.
  CacheLoader(const CacheLoader& other);
  void operator=(const CacheLoader& other);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CACHELOADER_H_
