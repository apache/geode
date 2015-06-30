/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.hibernate.internal;

import java.util.Collections;
import java.util.Map;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.QueryResultsRegion;
import org.hibernate.cache.Timestamper;
import org.hibernate.cache.TimestampsRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;

public class GemFireQueryResultsRegion implements QueryResultsRegion, TimestampsRegion {

  private final Region region;
  
  private Logger log = LoggerFactory.getLogger(getClass());
  
  public GemFireQueryResultsRegion(Region region) {
    this.region = region;
  }
  
  @Override
  public Object get(Object key) throws CacheException {
    log.debug("get query results for {} ", key);
    return this.region.get(key);
  }

  @Override
  public void put(Object key, Object value) throws CacheException {
    log.debug("For key {} putting query results {} ", key, value);
    this.region.put(key, value);
  }

  @Override
  public void evict(Object key) throws CacheException {
    log.debug("removing query results for key {}", key);
    this.region.remove(key);
  }

  @Override
  public void evictAll() throws CacheException {
    log.debug("clearing the query cache");
    this.region.clear();
  }

  @Override
  public String getName() {
    return this.region.getName();
  }

  @Override
  public void destroy() throws CacheException {
    if (!this.region.isDestroyed()) {
      this.region.destroyRegion();
    }
  }

  @Override
  public boolean contains(Object key) {
    return this.region.containsKey(key);
  }

  @Override
  public long getSizeInMemory() {
    return -1;
  }

  @Override
  public long getElementCountInMemory() {
    return this.region.size();
  }

  @Override
  public long getElementCountOnDisk() {
    // TODO make this an overflow region
    return -1;
  }

  @Override
  public Map toMap() {
    return Collections.unmodifiableMap(this.region);
  }

  @Override
  public long nextTimestamp() {
    return Timestamper.next();
  }

  @Override
  public int getTimeout() {
    return 60*1000; // all other cache providers have same value
  }
}
