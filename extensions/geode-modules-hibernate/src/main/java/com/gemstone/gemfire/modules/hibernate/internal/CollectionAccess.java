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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.CollectionRegion;
import org.hibernate.cache.access.CollectionRegionAccessStrategy;
import org.hibernate.cache.access.SoftLock;
import org.hibernate.cache.entry.CollectionCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ServerOperationException;

public class CollectionAccess implements
    CollectionRegionAccessStrategy {

  private final GemFireCollectionRegion region;
  
  private Logger log = LoggerFactory.getLogger(getClass());
  
  /**
   * if we know the entity whose ids are stored in this
   * collection, we can prefetch those entities using
   * getAll. This field stores that child entity name.
   */
  private String childEntityName;
  
  public CollectionAccess(GemFireCollectionRegion region) {
    this.region = region;
    String regionName = this.region.getGemFireRegion().getName().trim();
    regionName = regionName.replace("\\/", "");
    int lastPeriod = regionName.lastIndexOf('.');
    if (lastPeriod < 0) {
      log.info("Eager prefetching disabled for region: {}", this.region.getName());
      return;
    }
    String entityName = regionName.substring(0, lastPeriod);
    String collectionFieldName = regionName.substring(lastPeriod+1);
    log.debug("entity name: {}, collectionFieldName: {}", entityName, collectionFieldName);
    try {
      Class parentClass = Class.forName(entityName);
      Field[] fields = parentClass.getDeclaredFields();
      for (Field field : fields) {
        log.debug("genericType: {}", field.getGenericType());
        if (field.getName().equals(collectionFieldName)) {
          String genericString = field.toGenericString();
          log.debug("genericType: for required field name: {}", field.toGenericString());
          int startDependentEntityIndex = genericString.indexOf("<");
          if (startDependentEntityIndex != -1 &&
              genericString.indexOf("<", startDependentEntityIndex+1) == -1) {
            int childDependentEntityIndex = genericString.indexOf(">");
            this.childEntityName = genericString.substring(startDependentEntityIndex+1, childDependentEntityIndex);
            log.debug("For Collection {} using child entity: {}", this.region.getGemFireRegion().getName(), this.childEntityName);
          }
        }
      }
    }
    catch (ClassNotFoundException e) {
      //ok to ignore, we will not use pre-fetching
    }
    if (this.childEntityName == null) {
      log.info("Eager prefetching disabled for region: {}", this.region.getName());
    }
  }
  
  @Override
  public CollectionRegion getRegion() {
    return this.region;
  }

  @Override
  public Object get(Object key, long txTimestamp) throws CacheException {
    EntityWrapper wrapper = this.region.getGemFireRegion().get(key);
    if (wrapper == null) {
      this.region.getStats().incCacheMiss();
      log.debug("Cache miss for {} ts: {}",key, txTimestamp);
      return null;
    } else {
      this.region.getStats().incCacheHit();
      log.debug("cache hit {} count: {} ", key, this.region.getStats().getCacheHits());
      // do pre-fetching
      if (isPrefetchPossible()) {
        log.debug("for key: {} prefetching entries: {}", key, wrapper.getEntity());
        prefetchKeys((CollectionCacheEntry)wrapper.getEntity());
      }
    }
    return wrapper.getEntity();
  }

  private void prefetchKeys(CollectionCacheEntry entry) {
    StringBuilder builder = new StringBuilder(this.childEntityName+"#");
    Serializable[] childEntityKeys = entry.getState();
    Set<String> getAllSet = new HashSet<String>();
    for (Serializable id : childEntityKeys) {
      String key = builder.append(id).toString();
      log.debug("adding key {} to getAll set", key);
      getAllSet.add(key);
    }
    GemFireEntityRegion childRegion = this.region.regionFactory.getEntityRegion(this.childEntityName);
    log.debug("prefetching {} keys", getAllSet.size());
    if (!getAllSet.isEmpty() && childRegion != null) {
      childRegion.getAll(getAllSet);
    }
  }

  private boolean isPrefetchPossible() {
    return this.childEntityName != null;
  }

  private void printRegionContents(Region<Object, EntityWrapper> r) {
    log.debug("printing contents of {} ",r);
    for (Object k : r.keySet()) {
      log.debug("key {} value {} ",k,r.get(k));
    }
  }
  
  @Override
  public boolean putFromLoad(Object key, Object value, long txTimestamp,
      Object version) throws CacheException {
    return putFromLoad(key, value, txTimestamp, version, true);
  }

  @Override
  public boolean putFromLoad(Object key, Object value, long txTimestamp,
      Object version, boolean minimalPutOverride) throws CacheException {
    EntityWrapper wrapper = new EntityWrapper(value, 1L);
    log.debug("putting a new collection entry from load {} value: {}",key, wrapper);
    boolean remove = false;
    try {
      this.region.getGemFireRegion().create(key, wrapper);
    } catch (EntryExistsException ee) {
      log.debug("key {} exists in the cache already, destroying", key);
      remove = true;
    } catch (CacheWriterException writerEx) {
      this.region.getStats().incHibernateDestroyJobsScheduled();
      log.debug("caught a CacheWriterException {} ",writerEx.getMessage());
      remove = true;
    } catch (ServerOperationException serverEx) {
      if (serverEx.getCause() instanceof CacheWriterException) {
        this.region.getStats().incHibernateDestroyJobsScheduled();
        log.debug("caught a ServerOperationException caused by CacheWriterException {} ",serverEx.getMessage());
      } else {
        throw serverEx;
      }
      remove = true;
    }
    if (remove) {
      this.region.getGemFireRegion().remove(key);
      return false;
    }
    return true;
  }

  @Override
  public SoftLock lockItem(Object key, Object version) throws CacheException {
    // there are no updates to the collectionCache,
    // so no need to lock/version
    return null;
  }

  @Override
  public SoftLock lockRegion() throws CacheException {
    return null;
  }

  @Override
  public void unlockItem(Object key, SoftLock lock) throws CacheException {
  }

  @Override
  public void unlockRegion(SoftLock lock) throws CacheException {
  }

  @Override
  public void remove(Object key) throws CacheException {
    log.debug("removing key {}",key);
    this.region.getGemFireRegion().remove(key);
  }

  @Override
  public void removeAll() throws CacheException {
    log.debug("removing all keys");
    this.region.getGemFireRegion().clear();
  }

  @Override
  public void evict(Object key) throws CacheException {
    // TODO we should implement a method on Region to evict
    // a particular entry, destroying is inefficient
    log.debug("removing key {}", key);
    this.region.getGemFireRegion().remove(key);
  }

  @Override
  public void evictAll() throws CacheException {
    log.debug("removing all keys");
    this.region.getGemFireRegion().clear();
  }

}
