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
