#pragma once

#ifndef GEODE_GFCPP_REGIONFACTORY_H_
#define GEODE_GFCPP_REGIONFACTORY_H_

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
#include "AttributesFactory.hpp"
/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {
class CacheImpl;
class CPPCACHE_EXPORT RegionFactory : public SharedBase {
 public:
  /*
   * To create the (@link Region}.
   * @param name
   *        the name of the Region.
   * @throws RegionExistsException if a region is already in
   * this cache
   * @throws CacheClosedException if the cache is closed
   */
  RegionPtr create(const char* name);

  /** Sets the cache loader for the next <code>RegionAttributes</code> created.
   * @param cacheLoader the cache loader or NULLPTR if no loader
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setCacheLoader(const CacheLoaderPtr& cacheLoader);

  /** Sets the cache writer for the next <code>RegionAttributes</code> created.
   * @param cacheWriter the cache writer or NULLPTR if no cache writer
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setCacheWriter(const CacheWriterPtr& cacheWriter);

  /** Sets the CacheListener for the next <code>RegionAttributes</code> created.
   * @param aListener a user defined CacheListener, NULLPTR if no listener
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setCacheListener(const CacheListenerPtr& aListener);

  /** Sets the PartitionResolver for the next <code>RegionAttributes</code>
   * created.
   * @param aResolver a user defined PartitionResolver, NULLPTR if no resolver
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setPartitionResolver(const PartitionResolverPtr& aResolver);

  /**
   * Sets the library path for the library that will be invoked for the loader
   * of the region.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setCacheLoader(const char* libpath,
                                  const char* factoryFuncName);

  /**
   * Sets the library path for the library that will be invoked for the writer
   * of the region.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setCacheWriter(const char* libpath,
                                  const char* factoryFuncName);

  /**
   * Sets the library path for the library that will be invoked for the listener
   * of the region.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setCacheListener(const char* libpath,
                                    const char* factoryFuncName);

  /**
   * Sets the library path for the library that will be invoked for the
   * partition resolver of the region.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setPartitionResolver(const char* libpath,
                                        const char* factoryFuncName);

  // EXPIRATION ATTRIBUTES

  /** Sets the idleTimeout expiration attributes for region entries for the next
   * <code>RegionAttributes</code> created.
   * @param action the expiration action for entries in this region.
   * @param idleTimeout the idleTimeout in seconds for entries in this region.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setEntryIdleTimeout(ExpirationAction::Action action,
                                       int32_t idleTimeout);

  /** Sets the timeToLive expiration attributes for region entries for the next
   * <code>RegionAttributes</code> created.
   * @param action the expiration action for entries in this region.
   * @param timeToLive the timeToLive in seconds for entries in this region.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setEntryTimeToLive(ExpirationAction::Action action,
                                      int32_t timeToLive);

  /** Sets the idleTimeout expiration attributes for the region itself for the
   * next <code>RegionAttributes</code> created.
   * @param action the expiration action for entries in this region.
   * @param idleTimeout the idleTimeout in seconds for the region as a whole.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setRegionIdleTimeout(ExpirationAction::Action action,
                                        int32_t idleTimeout);

  /** Sets the timeToLive expiration attributes for the region itself for the
   * next <code>RegionAttributes</code> created.
   * @param action the expiration action for entries in this region.
   * @param timeToLive the timeToLive in seconds for the region as a whole.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setRegionTimeToLive(ExpirationAction::Action action,
                                       int32_t timeToLive);

  // PERSISTENCE
  /**
   * Sets the library path for the library that will be invoked for the
   * persistence of the region.
   * If the region is being created from a client on a server, or on a server
   * directly, then
   * this must be used to set the PersistenceManager.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setPersistenceManager(const char* libpath,
                                         const char* factoryFuncName,
                                         const PropertiesPtr& config = NULLPTR);

  /** Sets the PersistenceManager for the next <code>RegionAttributes</code>
  * created.
  * @param persistenceManager a user defined PersistenceManager, NULLPTR if no
  * persistenceManager
  * @return a reference to <code>this</code>
  */
  RegionFactoryPtr setPersistenceManager(
      const PersistenceManagerPtr& persistenceManager,
      const PropertiesPtr& config = NULLPTR);

  // MAP ATTRIBUTES
  /** Sets the entry initial capacity for the next <code>RegionAttributes</code>
   * created. This value
   * is used in initializing the map that holds the entries.
   * @param initialCapacity the initial capacity of the entry map
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if initialCapacity is negative.
   */
  RegionFactoryPtr setInitialCapacity(int initialCapacity);

  /** Sets the entry load factor for the next <code>RegionAttributes</code>
   * created. This value is
   * used in initializing the map that holds the entries.
   * @param loadFactor the load factor of the entry map
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if loadFactor is nonpositive
   */
  RegionFactoryPtr setLoadFactor(float loadFactor);

  /** Sets the concurrency level tof the next <code>RegionAttributes</code>
   * created. This value is used in initializing the map that holds the entries.
   * @param concurrencyLevel the concurrency level of the entry map
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if concurrencyLevel is nonpositive
   */
  RegionFactoryPtr setConcurrencyLevel(uint8_t concurrencyLevel);

  /**
   * Sets a limit on the number of entries that will be held in the cache.
   * If a new entry is added while at the limit, the cache will evict the
   * least recently used entry. Defaults to 0, meaning no LRU actions will
   * used.
   * @param entriesLimit number of enteries to keep in region
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setLruEntriesLimit(const uint32_t entriesLimit);

  /** Sets the Disk policy type for the next <code>RegionAttributes</code>
   * created.
   * @param diskPolicy the type of disk policy to use for the region
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if diskPolicyType is Invalid
   */
  RegionFactoryPtr setDiskPolicy(const DiskPolicyType::PolicyType diskPolicy);

  /**
   * Set caching enabled flag for this region. If set to false, then no data is
   * stored
   * in the local process, but events and distributions will still occur, and
   * the region can still be used to put and remove, etc...
   * The default if not set is 'true', 'false' is illegal for regions of 'local'
   * scope.
   * This also requires that interestLists are turned off for the region.
   * @param cachingEnabled if true, cache data for this region in this process.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setCachingEnabled(bool cachingEnabled);

  /*
   * Set the PoolName to attach the Region with that Pool.
   * Use only when Cache ha more than one Pool
   * @param name
   *        the name of the Pool to which region will be attached.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setPoolName(const char* name);

  /*
   * Set boolean to enable/disable cloning while applying delta.
   * @param isClonable whether to enable cloning or not.
   * @return a reference to <code>this</code>
   */
  RegionFactoryPtr setCloningEnabled(bool isClonable);

  /**
  * Enables or disables concurrent modification checks
  * @since 7.0
  * @param concurrencyChecksEnabled whether to perform concurrency checks on
  * operations
  * @return a reference to <code>this</code>
  */
  RegionFactoryPtr setConcurrencyChecksEnabled(bool enable);

  /**
  * Sets time out for tombstones
  * @since 7.0
  * @param tombstoneTimeoutInMSec tombstone timeout in milli second
  * @return a reference to <code>this</code>
  */
  RegionFactoryPtr setTombstoneTimeout(uint32_t tombstoneTimeoutInMSec);

 private:
  RegionFactory(apache::geode::client::RegionShortcut preDefinedRegion);

  RegionShortcut m_preDefinedRegion;

  AttributesFactoryPtr m_attributeFactory;

  void setRegionShortcut();

  ~RegionFactory();
  friend class CacheImpl;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_REGIONFACTORY_H_
