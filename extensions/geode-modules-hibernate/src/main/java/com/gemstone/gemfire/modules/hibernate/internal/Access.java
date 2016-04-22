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

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.EntityRegion;
import org.hibernate.cache.access.EntityRegionAccessStrategy;
import org.hibernate.cache.access.SoftLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class Access implements EntityRegionAccessStrategy {

  private final GemFireEntityRegion region;

  /**Thread local to remember the status of insert, which can be returned in afterInsert*/
  private ThreadLocal<Map<Object, Boolean>> createStatus = new ThreadLocal<Map<Object, Boolean>>() {
    @Override
    protected Map<Object, Boolean> initialValue() {
      return new HashMap<Object, Boolean>();
    }
  };

  private Logger log = LoggerFactory.getLogger(getClass());

  public Access(GemFireEntityRegion region) {
    this.region = region;
  }

  @Override
  public EntityRegion getRegion() {
    return this.region;
  }

  @Override
  public Object get(Object key, long txTimestamp) throws CacheException {
    KeyWrapper wKey = getWrappedKey(key);
    if (this.region.isRegisterInterestRequired()) {
      this.region.registerInterest(wKey);
    }
    // first check to see if we have pre-fetched this entity
    EntityWrapper wrapper = this.region.get(wKey);
    if (wrapper == null) {
      wrapper = this.region.getGemFireRegion().get(wKey);
    }
    if (wrapper == null) {
      this.region.getStats().incCacheMiss();
      log.debug("Cache miss for {} count: {}",wKey, this.region.getStats().getCacheMiss());
      return null;
    } else {
      this.region.getStats().incCacheHit();
      log.debug("cache hit {} count: {} ", wKey, this.region.getStats().getCacheHits());
    }
    return wrapper.getEntity();
  }

  @Override
  public boolean putFromLoad(Object key, Object value, long txTimestamp,
      Object version) throws CacheException {
    return putFromLoad(key, value, txTimestamp, version, true);
  }

  @Override
  public boolean putFromLoad(Object key, Object value, long txTimestamp,
      Object version, boolean minimalPutOverride) throws CacheException {
    return create(key, value);
  }

  private boolean create(Object key, Object value) {
    KeyWrapper wKey = getWrappedKey(key);
    EntityWrapper wrapper = new EntityWrapper(value, 1L);
    log.debug("putting a new entry from load {} value: {}",wKey, wrapper);
    boolean remove = false;
    try {
      this.region.getGemFireRegion().create(wKey, wrapper);
    } catch (EntryExistsException ee) {
      log.debug("key {} exists in the cache already, destroying", wKey);
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
      this.region.getGemFireRegion().remove(wKey);
      return false;
    }
    return true;
  }

  @Override
  public SoftLock lockItem(Object key, Object version) throws CacheException {
    KeyWrapper wKey = getWrappedKey(key);
    EntityWrapper wrapper = this.region.getGemFireRegion().get(wKey);
    Long ver = wrapper == null ? 0L : wrapper.getVersion();
    log.debug("lockItem:key: {} entityVersion: {}", new Object[] { wKey, ver });
    return new EntityVersionImpl(ver);
  }

  @Override
  public SoftLock lockRegion() throws CacheException {
    return null;
  }

  @Override
  public void unlockItem(Object key, SoftLock lock) throws CacheException {
    log.debug("unlockItem:key:" + key + " lock:" + lock);
  }

  @Override
  public void unlockRegion(SoftLock lock) throws CacheException {
  }

  @Override
  public boolean insert(Object key, Object value, Object version)
      throws CacheException {
    log.debug("insert:key:{} value:{} version:{}",
        new Object[]{key, value, version});
    boolean retVal = create(key, value);
    createStatus.get().put(key, retVal);
    return retVal;
  }

  @Override
  public boolean afterInsert(Object key, Object value, Object version)
      throws CacheException {
    log.info("afterInsert:key:{} value:{} version:{}",
        new Object[]{key, value, version});
    return createStatus.get().remove(key);
  }

  @Override
  public boolean update(Object key, Object value, Object currentVersion,
      Object previousVersion) throws CacheException {
    KeyWrapper wKey = getWrappedKey(key);
    EntityWrapper oldWrapper = this.region.getGemFireRegion().get(wKey);
    Long version = oldWrapper == null ? 1L : oldWrapper.getVersion() + 1;
    EntityWrapper wrapper = new EntityWrapper(value, version);
    log.debug("put:key:{} value:{} version:{}", new Object[] { wKey, value,
        version });
    boolean remove = false;
    try {
      if (oldWrapper == null) {
        remove = this.region.getGemFireRegion().putIfAbsent(wKey, wrapper) != null;
      } else {
        remove = !this.region.getGemFireRegion().replace(wKey, oldWrapper, wrapper);
      }
    } catch (CacheWriterException writerEx) {
      this.region.getStats().incHibernateDestroyJobsScheduled();
      log.debug("caught a CacheWriterException {} ",writerEx.getMessage());
      remove = true;
    } catch (ServerOperationException serverEx) {
      if (serverEx.getCause() instanceof CacheWriterException) {
        this.region.getStats().incHibernateDestroyJobsScheduled();
        log.debug("caught a ServerOperationException caused by CacheWriterException {} ",serverEx.getMessage());
        remove = true;
      } else {
        throw serverEx;
      }
    }
    if (remove) {
      this.region.getGemFireRegion().remove(wKey);
      return false;
    }
    log.debug("put for key {} succeded", wKey);
    return true;
  }

  @Override
  public boolean afterUpdate(Object key, Object value, Object currentVersion,
      Object previousVersion, SoftLock lock) throws CacheException {
    log.debug("afterUpdate:key:{} value:{} currVersion:{} previousVersion:{}",
        new Object[] {key, value, currentVersion, previousVersion});
    KeyWrapper wKey = getWrappedKey(key);
    EntityWrapper wrapper = this.region.getGemFireRegion().get(wKey);
    if (wrapper == null) {
      // this entry was destroyed during update
      return false;
    }
    Long version = wrapper.getVersion();
    Long expectedVersion = ((EntityVersion)lock).getVersion() + 1;
    log.debug("afterPut:key:{} value:{} version:{} expected: {}",
        new Object[] { wKey, value, version, expectedVersion });
    if (wrapper.getVersion() != expectedVersion) {
      log.debug(
          "for key {} expected version to be {} but was {}, so destroying the key",
          new Object[] { wKey, expectedVersion, version });
      this.region.getGemFireRegion().remove(wKey);
      return false;
    }
    return true;
  }

  @Override
  public void remove(Object key) throws CacheException {
    log.debug("removing key {} ",key);
    this.region.getGemFireRegion().remove(getWrappedKey(key));
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
    log.debug("removing key {} ",key);
    this.region.getGemFireRegion().remove(getWrappedKey(key));
  }

  @Override
  public void evictAll() throws CacheException {
    log.debug("removing all keys");
    this.region.getGemFireRegion().clear();
  }

  protected Region<Object, EntityWrapper> getGemFireRegion() {
    return this.region.getGemFireRegion();
  }
  
  protected KeyWrapper getWrappedKey(Object key) {
    return new KeyWrapper(key);
  }
}

