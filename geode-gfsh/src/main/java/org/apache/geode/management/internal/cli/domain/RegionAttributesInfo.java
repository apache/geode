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
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.management.internal.cli.util.RegionAttributesDefault;
import org.apache.geode.management.internal.cli.util.RegionAttributesNames;


public class RegionAttributesInfo implements Serializable {

  private static final long serialVersionUID = 336184564012988487L;
  private Scope scope = AbstractRegion.DEFAULT_SCOPE;

  private boolean cloningEnabled = false;
  private boolean concurrencyChecksEnabled = false;
  private int concurrencyLevel = 16;
  private DataPolicy dataPolicy = DataPolicy.DEFAULT;
  private String diskStoreName = "";
  private boolean enableAsyncConflation = false;
  private boolean enableSubscriptionConflation = false;
  private boolean ignoreJTA = false;
  private boolean indexMaintenanceSynchronous = true;
  private int initialCapacity = 16;
  private float loadFactor = 0.75f;
  private boolean multicastEnabled = false;
  private String poolName = "";

  private boolean statisticsEnabled = false;
  private boolean isLockGrantor = false;

  private List<String> cacheListenerClassNames;
  private String cacheLoaderClassName = "";
  private String cacheWriterClassName = "";

  private String compressorClassName = null;

  private Set<String> asyncEventQueueIDs = new HashSet<>();
  private Set<String> gatewaySenderIDs = new HashSet<>();

  private PartitionAttributesInfo partitionAttributesInfo = null;
  private EvictionAttributesInfo evictionAttributesInfo = null;
  private int regionTimeToLive = 0;
  private int regionIdleTimeout = 0;
  private int entryIdleTimeout = 0;
  private int entryTimeToLive = 0;
  private String entryTimeToLiveAction = ExpirationAction.INVALIDATE.toString();
  private String regionTimeToLiveAction = ExpirationAction.INVALIDATE.toString();
  private String entryIdleTimeoutAction = ExpirationAction.INVALIDATE.toString();
  private String regionIdleTimeoutAction = ExpirationAction.INVALIDATE.toString();

  private final boolean offHeap;
  private String customExpiryIdleTimeoutClass = null;
  private String customExpiryTTLClass = null;

  /***
   * Non-default-attribute map in the constructor
   */
  Map<String, String> nonDefaultAttributes;

  public RegionAttributesInfo(RegionAttributes<?, ?> ra) {

    cloningEnabled = ra.getCloningEnabled();
    concurrencyChecksEnabled = ra.getConcurrencyChecksEnabled();
    concurrencyLevel = ra.getConcurrencyLevel();
    dataPolicy = ra.getDataPolicy();
    diskStoreName = ra.getDiskStoreName();
    enableAsyncConflation = ra.getEnableAsyncConflation();
    enableSubscriptionConflation = ra.getEnableSubscriptionConflation();
    ignoreJTA = ra.getIgnoreJTA();
    indexMaintenanceSynchronous = ra.getIndexMaintenanceSynchronous();
    initialCapacity = ra.getInitialCapacity();
    loadFactor = ra.getLoadFactor();
    multicastEnabled = ra.getMulticastEnabled();
    poolName = ra.getPoolName();
    scope = ra.getScope();
    statisticsEnabled = ra.getStatisticsEnabled();
    entryTimeToLive = ra.getEntryTimeToLive().getTimeout();
    isLockGrantor = ra.isLockGrantor();
    entryIdleTimeout = ra.getEntryIdleTimeout().getTimeout();
    regionIdleTimeout = ra.getRegionIdleTimeout().getTimeout();
    regionTimeToLive = ra.getRegionTimeToLive().getTimeout();

    Compressor compressor = ra.getCompressor();
    if (compressor != null) {
      compressorClassName = compressor.getClass().getCanonicalName();
    }

    ExpirationAction expAction = ra.getEntryIdleTimeout().getAction();
    if (expAction != null) {
      entryIdleTimeoutAction = expAction.toString();
    }

    expAction = ra.getEntryTimeToLive().getAction();
    if (expAction != null) {
      entryTimeToLiveAction = expAction.toString();
    }

    expAction = ra.getRegionTimeToLive().getAction();

    if (expAction != null) {
      regionTimeToLiveAction = expAction.toString();
    }

    expAction = ra.getRegionIdleTimeout().getAction();
    if (expAction != null) {
      regionIdleTimeoutAction = expAction.toString();
    }

    asyncEventQueueIDs = ra.getAsyncEventQueueIds();
    gatewaySenderIDs = ra.getGatewaySenderIds();

    // Collecting information about all the CacheListeners, CacheWriters, CacheLoaders
    CacheListener<?, ?>[] cacheListeners = ra.getCacheListeners();

    // TODO: The cacheListeners should be added one by one by delimited by a "\n"
    if (cacheListeners.length > 0) {
      cacheListenerClassNames = new ArrayList<String>();
      for (CacheListener<?, ?> cacheListener : cacheListeners) {
        cacheListenerClassNames.add(cacheListener.getClass().getCanonicalName());
      }
      Collections.sort(cacheListenerClassNames);
    }

    // CacheLoader
    CacheLoader<?, ?> cacheLoader = ra.getCacheLoader();

    if (cacheLoader != null) {
      cacheLoaderClassName = cacheLoader.getClass().getCanonicalName();
    }

    // CacheWriter
    CacheWriter<?, ?> cacheWriter = ra.getCacheWriter();

    if (cacheWriter != null) {
      cacheWriterClassName = cacheWriter.getClass().getCanonicalName();
    }

    // Setting the Partition Attributes and Eviction Attributes
    PartitionAttributes<?, ?> partitionAttributes = ra.getPartitionAttributes();
    EvictionAttributes evictionAttributes = ra.getEvictionAttributes();

    if (partitionAttributes != null) {
      partitionAttributesInfo = new PartitionAttributesInfo(partitionAttributes);
    }

    if (evictionAttributes != null) {
      evictionAttributesInfo = new EvictionAttributesInfo(evictionAttributes);

    }
    offHeap = ra.getOffHeap();

    CustomExpiry<?, ?> customIdleTimeout = ra.getCustomEntryIdleTimeout();
    if (customIdleTimeout != null) {
      customExpiryIdleTimeoutClass = customIdleTimeout.getClass().getName();
    }

    CustomExpiry<?, ?> customTTL = ra.getCustomEntryTimeToLive();
    if (customTTL != null) {
      customExpiryTTLClass = customTTL.getClass().getName();
    }
  }


  public PartitionAttributesInfo getPartitionAttributesInfo() {
    return partitionAttributesInfo;
  }

  public EvictionAttributesInfo getEvictionAttributesInfo() {
    return evictionAttributesInfo;
  }

  public int getEntryIdleTimeout() {
    return entryIdleTimeout;
  }

  public String getEntryIdleTimeoutAction() {
    return entryIdleTimeoutAction;
  }

  public int getEntryTimeToLive() {
    return entryTimeToLive;
  }

  public String getEntryTimeToLiveAction() {
    return entryTimeToLiveAction;
  }

  public int getRegionIdleTimeout() {
    return regionIdleTimeout;
  }

  public String getRegionIdleTimeoutAction() {
    return regionIdleTimeoutAction;
  }

  public boolean getCloningEnabled() {
    return cloningEnabled;
  }

  public String getCompressorClassName() {
    return compressorClassName;
  }

  public boolean getConcurrencyChecksEnabled() {
    return concurrencyChecksEnabled;
  }

  public int getConcurrencyLevel() {
    return concurrencyLevel;
  }

  public DataPolicy getDataPolicy() {
    return dataPolicy;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public boolean getEnableSyncConflation() {
    return enableAsyncConflation;
  }

  public boolean getEnableSubscriptionConflation() {
    return enableSubscriptionConflation;
  }

  public boolean getIgnoreJTA() {
    return ignoreJTA;
  }

  public boolean getIndexMaintenanceSynchronous() {
    return indexMaintenanceSynchronous;
  }

  public int getInitialCapacity() {
    return initialCapacity;
  }

  public Set<String> getAsyncEventQueueIDs() {
    return asyncEventQueueIDs;
  }

  public Set<String> getGatewaySenderIDs() {
    return gatewaySenderIDs;
  }

  public float getLoadFactor() {
    return loadFactor;
  }

  public boolean getMulticastEnabled() {
    return multicastEnabled;
  }

  public String getPoolName() {
    return poolName;
  }

  public Scope getScope() {
    return scope;
  }

  public boolean getStatisticsEnabled() {
    return statisticsEnabled;
  }

  public boolean isLockGrantor() {
    return isLockGrantor;
  }

  public List<String> getCacheListenerClassNames() {
    return cacheListenerClassNames;
  }


  public String getCacheLoaderClassName() {
    return cacheLoaderClassName;
  }


  public String getCacheWriterClassName() {
    return cacheWriterClassName;
  }

  public int getRegionTimeToLive() {
    return regionTimeToLive;
  }

  public String getRegionTimeToLiveAction() {
    return regionTimeToLiveAction;
  }

  public boolean getOffHeap() {
    return offHeap;
  }

  public String getCustomExpiryIdleTimeoutClass() {
    return customExpiryIdleTimeoutClass;
  }

  public String getCustomExpiryTTLClass() {
    return customExpiryTTLClass;
  }

  @Override
  public boolean equals(Object arg0) {
    return super.equals(arg0);
  }

  public int hashCode() {
    return 42; // any arbitrary constant will do

  }

  /***
   * Returns Map of the non default Attributes and its values
   */
  public Map<String, String> getNonDefaultAttributes() {

    if (nonDefaultAttributes != null) {
      return nonDefaultAttributes;
    }

    nonDefaultAttributes = new HashMap<>();

    if (cloningEnabled != RegionAttributesDefault.CLONING_ENABLED) {
      nonDefaultAttributes.put(RegionAttributesNames.CLONING_ENABLED,
          Boolean.toString(cloningEnabled));
    }

    if (!StringUtils.equals(RegionAttributesDefault.COMPRESSOR_CLASS_NAME, compressorClassName)) {
      nonDefaultAttributes.put(RegionAttributesNames.COMPRESSOR, compressorClassName);
    }

    if (concurrencyChecksEnabled != RegionAttributesDefault.CONCURRENCY_CHECK_ENABLED) {
      nonDefaultAttributes.put(RegionAttributesNames.CONCURRENCY_CHECK_ENABLED,
          Boolean.toString(concurrencyChecksEnabled));
    }

    if (concurrencyLevel != RegionAttributesDefault.CONCURRENCY_LEVEL) {
      nonDefaultAttributes.put(RegionAttributesNames.CONCURRENCY_LEVEL,
          Integer.toString(concurrencyLevel));
    }

    if (!dataPolicy.equals(RegionAttributesDefault.DATA_POLICY)) {
      nonDefaultAttributes.put(RegionAttributesNames.DATA_POLICY, dataPolicy.toString());
    }

    if (diskStoreName != null && !diskStoreName.equals(RegionAttributesDefault.DISK_STORE_NAME)) {
      nonDefaultAttributes.put(RegionAttributesNames.DISK_STORE_NAME, diskStoreName);
    }

    if (enableAsyncConflation != RegionAttributesDefault.ENABLE_ASYNC_CONFLATION) {
      nonDefaultAttributes.put(RegionAttributesNames.ENABLE_ASYNC_CONFLATION,
          Boolean.toString(enableAsyncConflation));
    }

    if (enableSubscriptionConflation != RegionAttributesDefault.ENABLE_SUBSCRIPTION_CONFLATION) {
      nonDefaultAttributes.put(RegionAttributesNames.ENABLE_SUBSCRIPTION_CONFLATION,
          Boolean.toString(enableSubscriptionConflation));
    }

    if (entryIdleTimeout != RegionAttributesDefault.ENTRY_IDLE_TIMEOUT) {
      nonDefaultAttributes.put(RegionAttributesNames.ENTRY_IDLE_TIMEOUT,
          Integer.toString(entryIdleTimeout));
    }

    if (ignoreJTA != RegionAttributesDefault.IGNORE_JTA) {
      nonDefaultAttributes.put(RegionAttributesNames.IGNORE_JTA, Boolean.toString(ignoreJTA));
    }

    if (indexMaintenanceSynchronous != RegionAttributesDefault.INDEX_MAINTENANCE_SYNCHRONOUS) {
      nonDefaultAttributes.put(RegionAttributesNames.INDEX_MAINTENANCE_SYNCHRONOUS,
          Boolean.toString(indexMaintenanceSynchronous));
    }

    if (initialCapacity != RegionAttributesDefault.INITIAL_CAPACITY) {
      nonDefaultAttributes.put(RegionAttributesNames.INITIAL_CAPACITY,
          Integer.toString(initialCapacity));
    }

    if (loadFactor != RegionAttributesDefault.LOAD_FACTOR) {
      nonDefaultAttributes.put(RegionAttributesNames.LOAD_FACTOR, Float.toString(loadFactor));
    }

    if (multicastEnabled != RegionAttributesDefault.MULTICAST_ENABLED) {
      nonDefaultAttributes.put(RegionAttributesNames.MULTICAST_ENABLED,
          Boolean.toString(multicastEnabled));
    }

    if (poolName != null && !poolName.equals(RegionAttributesDefault.POOL_NAME)) {
      nonDefaultAttributes.put(RegionAttributesNames.POOL_NAME, poolName);
    }

    if (!scope.equals(RegionAttributesDefault.SCOPE)) {
      nonDefaultAttributes.put(RegionAttributesNames.SCOPE, scope.toString());
    }

    if (statisticsEnabled != RegionAttributesDefault.STATISTICS_ENABLED) {
      nonDefaultAttributes.put(RegionAttributesNames.STATISTICS_ENABLED,
          Boolean.toString(statisticsEnabled));
    }

    if (isLockGrantor != RegionAttributesDefault.IS_LOCK_GRANTOR) {
      nonDefaultAttributes.put(RegionAttributesNames.IS_LOCK_GRANTOR,
          Boolean.toString(isLockGrantor));
    }

    if (entryIdleTimeout != RegionAttributesDefault.ENTRY_IDLE_TIMEOUT) {
      nonDefaultAttributes.put(RegionAttributesNames.ENTRY_IDLE_TIMEOUT,
          Integer.toString(entryIdleTimeout));
    }

    if (entryIdleTimeoutAction != null
        && !entryIdleTimeoutAction.equals(RegionAttributesDefault.ENTRY_IDLE_TIMEOUT_ACTION)) {
      nonDefaultAttributes.put(RegionAttributesNames.ENTRY_IDLE_TIMEOUT_ACTION,
          entryIdleTimeoutAction);
    }

    if (entryTimeToLive != RegionAttributesDefault.ENTRY_TIME_TO_LIVE) {
      nonDefaultAttributes.put(RegionAttributesNames.ENTRY_TIME_TO_LIVE,
          Integer.toString(entryTimeToLive));
    }

    if (entryTimeToLiveAction != null
        && !entryTimeToLiveAction.equals(RegionAttributesDefault.ENTRY_TIME_TO_LIVE_ACTION)) {
      nonDefaultAttributes.put(RegionAttributesNames.ENTRY_TIME_TO_LIVE_ACTION,
          entryTimeToLiveAction);
    }

    if (regionIdleTimeout != RegionAttributesDefault.REGION_IDLE_TIMEOUT) {
      nonDefaultAttributes.put(RegionAttributesNames.REGION_IDLE_TIMEOUT,
          Integer.toString(regionIdleTimeout));
    }

    if (regionIdleTimeoutAction != null
        && !regionIdleTimeoutAction.equals(RegionAttributesDefault.REGION_IDLE_TIMEOUT_ACTION)) {
      nonDefaultAttributes.put(RegionAttributesNames.REGION_IDLE_TIMEOUT_ACTION,
          regionIdleTimeoutAction);
    }

    if (regionTimeToLive != RegionAttributesDefault.REGION_TIME_TO_LIVE) {
      nonDefaultAttributes.put(RegionAttributesNames.REGION_TIME_TO_LIVE,
          Integer.toString(regionTimeToLive));
    }

    if (regionTimeToLiveAction != null
        && !regionTimeToLiveAction.equals(RegionAttributesDefault.REGION_TIME_TO_LIVE_ACTION)) {
      nonDefaultAttributes.put(RegionAttributesNames.REGION_TIME_TO_LIVE_ACTION,
          regionTimeToLiveAction);
    }

    if (cacheListenerClassNames != null && !cacheListenerClassNames.isEmpty()) {
      nonDefaultAttributes.put(RegionAttributesNames.CACHE_LISTENERS,
          StringUtils.join(cacheListenerClassNames, ","));
    }

    if (cacheLoaderClassName != null && !cacheLoaderClassName.isEmpty()) {
      nonDefaultAttributes.put(RegionAttributesNames.CACHE_LOADER, cacheLoaderClassName);
    }

    if (cacheWriterClassName != null && !cacheWriterClassName.isEmpty()) {
      nonDefaultAttributes.put(RegionAttributesNames.CACHE_WRITER, cacheWriterClassName);
    }

    if (offHeap != RegionAttributesDefault.OFF_HEAP) {
      nonDefaultAttributes.put(RegionAttributesNames.OFF_HEAP, Boolean.toString(offHeap));
    }

    if (!asyncEventQueueIDs.isEmpty()) {
      nonDefaultAttributes.put(RegionAttributesNames.ASYNC_EVENT_QUEUE_ID,
          String.join(",", asyncEventQueueIDs));
    }

    if (!gatewaySenderIDs.isEmpty()) {
      nonDefaultAttributes.put(RegionAttributesNames.GATEWAY_SENDER_ID,
          String.join(",", gatewaySenderIDs));
    }

    if (StringUtils.isNotEmpty(customExpiryIdleTimeoutClass)) {
      nonDefaultAttributes.put(RegionAttributesNames.ENTRY_IDLE_TIME_CUSTOM_EXPIRY,
          customExpiryIdleTimeoutClass);
    }

    if (StringUtils.isNotEmpty(customExpiryTTLClass)) {
      nonDefaultAttributes.put(RegionAttributesNames.ENTRY_TIME_TO_LIVE_CUSTOM_EXPIRY,
          customExpiryTTLClass);
    }

    return nonDefaultAttributes;
  }
}
