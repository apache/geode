/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.client.internal;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.Objects;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.UserSpecifiedRegionAttributes;

/**
 * The distributed system will always default to a loner on a client.
 *
 * @since GemFire 6.5
 */

public class ClientRegionFactoryImpl<K, V> implements ClientRegionFactory<K, V> {
  private final AttributesFactory<K, V> attrsFactory;
  private final InternalClientCache cache;

  /**
   * Constructs a ClientRegionFactory by creating a DistributedSystem and a Cache. If no
   * DistributedSystem exists it creates a loner DistributedSystem, otherwise it uses the existing
   * DistributedSystem. A default pool will be used unless ... The Region configuration is
   * initialized using the given region shortcut.
   *
   * @param pra the region shortcut to use
   */
  public ClientRegionFactoryImpl(InternalClientCache cache, ClientRegionShortcut pra) {
    this.cache = cache;
    RegionAttributes ra = cache.getRegionAttributes(pra.toString());
    if (ra == null) {
      throw new IllegalStateException(
          "The region shortcut " + pra + " has been removed from " + cache.listRegionAttributes());
    }
    this.attrsFactory = new AttributesFactory<K, V>(ra);
    initAttributeFactoryDefaults();
  }

  /**
   * Constructs a ClientRegionFactory by creating a DistributedSystem and a Cache. If no
   * DistributedSystem exists it creates a loner DistributedSystem, otherwise it uses the existing
   * DistributedSystem. A default pool will be used unless ... The region configuration is
   * initialized using a region attributes whose name was given as the refid.
   *
   * @param refid the name of the region attributes to use
   */
  public ClientRegionFactoryImpl(InternalClientCache cache, String refid) {
    this.cache = cache;
    RegionAttributes ra = cache.getRegionAttributes(refid);
    if (ra == null) {
      throw new IllegalStateException(
          "The named region attributes \"" + refid + "\" has not been defined.");
    }
    this.attrsFactory = new AttributesFactory<K, V>(ra);
    initAttributeFactoryDefaults();
  }

  private void initAttributeFactoryDefaults() {
    this.attrsFactory.setScope(Scope.LOCAL);
    this.attrsFactory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
  }

  /**
   * Returns the cache used by this factory.
   */
  private InternalClientCache getCache() {
    return this.cache;
  }

  private Pool getDefaultPool() {
    return getCache().getDefaultPool();
  }

  @Override
  public ClientRegionFactory<K, V> addCacheListener(CacheListener<K, V> aListener) {
    this.attrsFactory.addCacheListener(aListener);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> initCacheListeners(CacheListener<K, V>[] newListeners) {
    this.attrsFactory.initCacheListeners(newListeners);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setEvictionAttributes(EvictionAttributes evictionAttributes) {
    this.attrsFactory.setEvictionAttributes(evictionAttributes);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setEntryIdleTimeout(ExpirationAttributes idleTimeout) {
    this.attrsFactory.setEntryIdleTimeout(idleTimeout);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setCustomEntryIdleTimeout(CustomExpiry<K, V> custom) {
    this.attrsFactory.setCustomEntryIdleTimeout(custom);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setEntryTimeToLive(ExpirationAttributes timeToLive) {
    this.attrsFactory.setEntryTimeToLive(timeToLive);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setCustomEntryTimeToLive(CustomExpiry<K, V> custom) {
    this.attrsFactory.setCustomEntryTimeToLive(custom);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setRegionIdleTimeout(ExpirationAttributes idleTimeout) {
    this.attrsFactory.setRegionIdleTimeout(idleTimeout);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setRegionTimeToLive(ExpirationAttributes timeToLive) {
    this.attrsFactory.setRegionTimeToLive(timeToLive);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setKeyConstraint(Class<K> keyConstraint) {
    this.attrsFactory.setKeyConstraint(keyConstraint);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setValueConstraint(Class<V> valueConstraint) {
    this.attrsFactory.setValueConstraint(valueConstraint);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setInitialCapacity(int initialCapacity) {
    this.attrsFactory.setInitialCapacity(initialCapacity);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setLoadFactor(float loadFactor) {
    this.attrsFactory.setLoadFactor(loadFactor);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setConcurrencyLevel(int concurrencyLevel) {
    this.attrsFactory.setConcurrencyLevel(concurrencyLevel);
    return this;
  }

  @Override
  public void setConcurrencyChecksEnabled(boolean concurrencyChecksEnabled) {
    this.attrsFactory.setConcurrencyChecksEnabled(concurrencyChecksEnabled);
  }

  @Override
  public ClientRegionFactory<K, V> setDiskStoreName(String name) {
    this.attrsFactory.setDiskStoreName(name);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setDiskSynchronous(boolean isSynchronous) {
    this.attrsFactory.setDiskSynchronous(isSynchronous);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setStatisticsEnabled(boolean statisticsEnabled) {
    this.attrsFactory.setStatisticsEnabled(statisticsEnabled);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setCloningEnabled(boolean cloningEnable) {
    this.attrsFactory.setCloningEnabled(cloningEnable);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setPoolName(String poolName) {
    this.attrsFactory.setPoolName(poolName);
    return this;
  }

  @Override
  public ClientRegionFactory<K, V> setCompressor(Compressor compressor) {
    this.attrsFactory.setCompressor(compressor);
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Region<K, V> create(String name) throws RegionExistsException {
    return getCache().basicCreateRegion(name, createRegionAttributes());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Region<K, V> createSubregion(Region<?, ?> parent, String name)
      throws RegionExistsException {
    return ((InternalRegion) parent).createSubregion(name, createRegionAttributes());
  }

  @SuppressWarnings("deprecation")
  private RegionAttributes<K, V> createRegionAttributes() {
    RegionAttributes<K, V> ra = this.attrsFactory.create();
    if (isEmpty(ra.getPoolName())
        || Objects.equals(GemFireCacheImpl.DEFAULT_POOL_NAME, ra.getPoolName())) {
      UserSpecifiedRegionAttributes<K, V> ura = (UserSpecifiedRegionAttributes<K, V>) ra;
      if (ura.requiresPoolName) {
        Pool dp = getDefaultPool();
        if (dp != null) {
          this.attrsFactory.setPoolName(dp.getName());
          ra = this.attrsFactory.create();
        } else {
          throw new IllegalStateException("The poolName must be set on a client.");
        }
      }
    }
    return ra;
  }
}
