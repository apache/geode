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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.Region;
import org.hibernate.cache.Timestamper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.modules.hibernate.GemFireRegionFactory;
import com.gemstone.gemfire.modules.util.ModuleStatistics;

public class GemFireBaseRegion implements Region {

  /**
   * the backing region
   */
  protected final com.gemstone.gemfire.cache.Region<Object, EntityWrapper> region;

  /**
   * to determine if the operation should be forwarded to server
   */
  protected final boolean isClientRegion;

  protected final CacheDataDescription metadata;

  private final Logger log = LoggerFactory.getLogger(getClass());

  protected final GemFireRegionFactory regionFactory;
  
  protected final ModuleStatistics stats;
  
  public GemFireBaseRegion(com.gemstone.gemfire.cache.Region<Object, EntityWrapper> region,
      boolean isClient, CacheDataDescription metadata, GemFireRegionFactory regionFactory) {
    this.region = region;
    this.isClientRegion = isClient;
    this.metadata = metadata;
    this.regionFactory = regionFactory;
    DistributedSystem system = ((LocalRegion)region).getSystem();
    this.stats = ModuleStatistics.getInstance(system);

  }

  public com.gemstone.gemfire.cache.Region<Object, EntityWrapper> getGemFireRegion() {
    return this.region;
  }

  public ModuleStatistics getStats() {
    return this.stats;
  }
  
  public ExecutorService getExecutorService() {
    return this.regionFactory.getExecutorService();
  }

  @Override
  public String getName() {
    return this.region.getName();
  }

  @Override
  public void destroy() throws CacheException {
    if (!this.region.isDestroyed()) {
      this.region.localDestroyRegion();
    }
  }

  /*
   * I did not see any useful callers from hibernate-core
   */
  @Override
  public boolean contains(Object key) {
    log.debug("contains key called for :" + key);
    if (isClientRegion) {
      // TODO should this be done?
      return this.region.containsKeyOnServer(key);
    }
    return this.region.containsKey(key);
  }

  @Override
  public long getSizeInMemory() {
    return 0;
  }

  @Override
  public long getElementCountInMemory() {
    return this.region.size();
  }

  @Override
  public long getElementCountOnDisk() {
    LocalRegion lr = (LocalRegion)this.region;
    if (lr.getDiskRegion() != null) {
      return lr.getDiskRegion().getNumOverflowOnDisk();
    }
    return 0;
  }

  @Override
  public Map<Object, EntityWrapper> toMap() {
    return Collections.unmodifiableMap(this.region);
  }

  /*
   * only used by updateTimestamps cache
   */
  @Override
  public long nextTimestamp() {
    log.debug("nextTimestamp called");
    return Timestamper.next();
  }
  
  /*
   * this is used by updateTimestamps cache only
   */
  @Override
  public int getTimeout() {
    return 60*1000; // all other cache providers have same value
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GemFireBaseRegion) {
      GemFireBaseRegion other = (GemFireBaseRegion)obj;
      if (this.region.getName().equals(other.region.getName())
          && this.isClientRegion == other.isClientRegion) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.region.hashCode() + (this.isClientRegion ? 1 : 0);
  }
  
}
