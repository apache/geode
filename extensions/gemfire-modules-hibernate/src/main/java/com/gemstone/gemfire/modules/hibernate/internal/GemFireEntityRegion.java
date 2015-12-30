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
package com.gemstone.gemfire.modules.hibernate.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.EntityRegion;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cache.access.EntityRegionAccessStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.modules.hibernate.GemFireRegionFactory;
import com.gemstone.gemfire.modules.util.ModuleStatistics;

public class GemFireEntityRegion extends GemFireBaseRegion implements EntityRegion {

  private final Logger log = LoggerFactory.getLogger(getClass());
  
  private final boolean USE_JTA = Boolean.getBoolean("gemfiremodules.useJTA");
  
  /**
   * keys for which interest has been registered already
   */
  private ConcurrentMap<Object, Boolean> registeredKeys = new ConcurrentHashMap<Object, Boolean>();

  /**
   * map to store the entries that were pre-fetched when the underlying region has no local storage
   */
  protected ConcurrentMap<Object, EntityWrapper> preFetchMap = new ConcurrentHashMap<Object, EntityWrapper>();
  
  public GemFireEntityRegion(Region<Object, EntityWrapper> region,
      boolean isClient, CacheDataDescription metadata, GemFireRegionFactory regionFactory) {
    super(region, isClient, metadata, regionFactory);
  }

  @Override
  public boolean isTransactionAware() {
    // there are no colocation guarantees while using hibernate
    // so return false for a PartitionedRegion for now
    if (USE_JTA) {
      return true;
    }
    return false;
  }

  @Override
  public CacheDataDescription getCacheDataDescription() {
    return this.metadata;
  }

  @Override
  public EntityRegionAccessStrategy buildAccessStrategy(AccessType accessType)
      throws CacheException {
    if (AccessType.READ_ONLY.equals(accessType)) {
      log.info("creating read-only access for region: " + this.getName());
      return new ReadOnlyAccess(this);
    }
    else if (AccessType.NONSTRICT_READ_WRITE.equals(accessType)) {
      log.info("creating nonstrict-read-write access for region: "
          + this.getName());
      return new NonStrictReadWriteAccess(this);
    }
    else if (AccessType.READ_WRITE.equals(accessType)) {
    	log.info("creating read-write access for region: "
    	          + this.getName());
      return new ReadWriteAccess(this);
    }
    else if (AccessType.TRANSACTIONAL.equals(accessType)) {
    	log.info("creating transactional access for region: "
    	          + this.getName());
      return new TransactionalAccess(this);
    }
    throw new UnsupportedOperationException("Unknown access type: "
        + accessType);
  }

  /**
   * Should this region should register interest in keys.
   * @return true for client regions with storage
   */
  public boolean isRegisterInterestRequired() {
    return this.isClientRegion && this.region.getAttributes().getDataPolicy().withStorage();
  }
  
  /**
   * register interest in this key, if not already registered
   * @param key
   */
  public void registerInterest(Object key) {
    if (!this.registeredKeys.containsKey(key)) {
      this.region.registerInterest(key);
      this.registeredKeys.put(key, Boolean.TRUE);
      log.debug("registered interest in key{}", key);
    }
  }
  
  public void registerInterest(Collection<?> list) {
    // build a list of keys for which interest is not
    // already registered
    List<Object> interestList = new ArrayList<Object>();
    for (Object o : list) {
      if (!this.registeredKeys.containsKey(o)) {
        interestList.add(o);
      }
    }
    // register interest in this list
    this.region.registerInterest(interestList);
    log.debug("registered interest in {} keys", interestList.size());
  }
  
  /**
   * wraps the keys in {@link KeyWrapper} and calls getAll
   * on the underlying GemFire region. When the underlying region
   * is a proxy region, the fetched entries are stored in a local
   * map.
   * @param keys
   */
  public void getAll(Collection<?> keys) {
    Set<KeyWrapper> wrappedKeys = new HashSet<KeyWrapper>();
    for (Object o : keys) {
      wrappedKeys.add(new KeyWrapper(o));
    }
    if (isRegisterInterestRequired()) {
      registerInterest(wrappedKeys);
    } else {
      Map<Object, EntityWrapper> retVal = this.region.getAll(wrappedKeys);
      putInLocalMap(retVal);
    }
  }

  /**
   * if the underlying gemfire region does not have local storage, put
   * the pre-fetched entries in {@link #preFetchMap}
   * @param map map of prefetched entries
   */
  private void putInLocalMap(Map<Object, EntityWrapper> map) {
    if (!this.region.getAttributes().getDataPolicy().withStorage()) {
      // if the value is null, do not cache in preFetchMap
      for (Entry<Object, EntityWrapper> e : map.entrySet()) {
        if (e.getValue() != null) {
          this.preFetchMap.put(e.getKey(), e.getValue());
          log.debug("putting key: {} value: {} in local map", e.getKey(), e.getValue());
        }
      }
    }
  }

  /**
   * If this key was pre-fetched, get the entity.
   * @param key
   * @return the prefetched entity
   */
  public EntityWrapper get(Object key) {
    return this.preFetchMap.remove(key);
  }
}

