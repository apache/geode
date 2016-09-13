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
package com.gemstone.gemfire.modules.hibernate;

import java.util.Map;

import org.hibernate.cache.Cache;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.Timestamper;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.internal.cache.LocalRegion;

public class GemFireCache implements Cache {
  private Region region;

  private boolean clientRegion = false;

  private final DistributedLockService distributedLockService;

  public GemFireCache(Region region, DistributedLockService lockService) {
    this.region = region;
    this.distributedLockService = lockService;
    this.clientRegion = isClient(region);
  }

  private boolean isClient(Region region) {
    return region.getAttributes().getPoolName() != null;
  }

  /**
   * Clear the cache
   */
  public void clear() throws CacheException {
    GemFireCacheProvider.getLogger().info("GemFireCache: clear called");
    region.clear();
  }

  /**
   * Clean up
   */
  public void destroy() throws CacheException {
    GemFireCacheProvider.getLogger().info("GemFireCache: destroy called");
    region.localDestroyRegion();
  }

  /**
   * Get an item from the cache
   * 
   * @param key
   * @return the cached object or <tt>null</tt>
   * @throws CacheException
   */
  public Object get(Object key) throws CacheException {
    GemFireCacheProvider.getLogger().debug(
        "GemFireCache: get called for: " + key);
    try {
      Object value = region.get(key);
      GemFireCacheProvider.getLogger().debug(
          "GemFireCache: retrieved: " + key + "-->" + value);
      return value;
    }
    catch (com.gemstone.gemfire.cache.CacheException e) {
      throw new CacheException(e);
    }
  }

  /**
   * The count of entries currently contained in the regions in-memory store.
   * 
   * @return The count of entries in memory; -1 if unknown or unsupported.
   */
  public long getElementCountInMemory() {
    return ((LocalRegion)region).entryCount();
  }

  /**
   * The count of entries currently contained in the regions disk store.
   * 
   * @return The count of entries on disk; -1 if unknown or unsupported.
   */
  public long getElementCountOnDisk() {
    return -1;
  }

  /**
   * Get the name of the cache region
   */
  public String getRegionName() {
    return region.getName();
  }

  /**
   * The number of bytes is this cache region currently consuming in memory.
   * 
   * @return The number of bytes consumed by this region; -1 if unknown or
   *         unsupported.
   */
  public long getSizeInMemory() {
    return -1;
  }

  /**
   * Return the lock timeout for this cache.
   */
  public int getTimeout() {
    GemFireCacheProvider.getLogger().debug("GemFireCache: getTimeout");
    return Timestamper.ONE_MS * 60000;
  }

  /**
   * If this is a clustered cache, lock the item
   */
  public void lock(Object key) throws CacheException {
    GemFireCacheProvider.getLogger().info(
        "GemFireCache: lock called for: " + key);

    if (!clientRegion) {
      // If we're using GLOBAL scope, we don't have to worry about
      // locking.
      if (!Scope.GLOBAL.equals(region.getAttributes().getScope())) {
        this.distributedLockService.lock(key, -1, -1);
      }
    }
    else {
      // We assume the server region is GLOBAL for now. Else, use command
      // pattern to acquire lock on the server
      GemFireCacheProvider.getLogger().info(
          "GemFireCache: client region, ignoring lock : " + key);
    }
  }

  /**
   * Generate the next timestamp
   */
  public long nextTimestamp() {
    GemFireCacheProvider.getLogger().debug("GemFireCache: nextTimestamp called");
    // TODO : Need a counter, cache-wide
    return Timestamper.next();
  }

  /**
   * Add an item to the cache
   * 
   * @param key
   * @param value
   * @throws CacheException
   */
  public void put(Object key, Object value) throws CacheException {
    GemFireCacheProvider.getLogger().debug(
        "GemFireCache: put called for key: " + key + "value: " + value);
    try {
      region.put(key, value);
      GemFireCacheProvider.getLogger().debug(
          "GemFireCache: put " + key + "-->" + value);
    }
    catch (com.gemstone.gemfire.cache.CacheException e) {
      throw new CacheException(e);
    }
  }

  public Object read(Object key) throws CacheException {
    GemFireCacheProvider.getLogger().info(
        "GemFireCache: read called for: " + key);
    return region.get(key);
  }

  /**
   * Remove an item from the cache
   */
  public void remove(Object key) throws CacheException {
    GemFireCacheProvider.getLogger().debug(
        "GemFireCache: remove called for: " + key);
    try {
      region.destroy(key);
      GemFireCacheProvider.getLogger().debug("GemFireCache: removed: " + key);
    }
    catch (EntryNotFoundException e) {
      // We can silently ignore this
    }
    catch (com.gemstone.gemfire.cache.CacheException e) {
      throw new CacheException(e);
    }
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Hibernate cache on GemFire region: ");
    buffer.append(region);
    return buffer.toString();
  }

  /**
   * If this is a clustered cache, unlock the item
   */
  public void unlock(Object key) throws CacheException {
    GemFireCacheProvider.getLogger().info(
        "GemFireCache: unlock called for: " + key);

    if (!clientRegion) {
      // If we're using GLOBAL scope, we don't have to worry about locking.
      if (!Scope.GLOBAL.equals(region.getAttributes().getScope())) {
        this.distributedLockService.unlock(key);
      }
    }
    else {
      GemFireCacheProvider.getLogger().info(
          "GemFireCache: client region, ignoring lock : " + key);
    }
  }

  public void update(Object key, Object value) throws CacheException {
    GemFireCacheProvider.getLogger().info(
        "GemFireCache: update called for: " + key);
    this.region.put(key, value);
  }

  public Map<?, ?> toMap() {
    return null;
  }
}
