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
package org.apache.geode.management;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Set;

import org.apache.geode.cache.Region;

/**
 * Composite data type used to distribute attributes for a {@link Region}.
 *
 * @since GemFire 7.0
 */
public class RegionAttributesData {

  private final String cacheLoaderClassName;
  private final String cacheWriterClassName;
  private final String keyConstraintClassName;
  private final String[] cacheListeners;
  private final String valueConstraintClassName;
  private final int regionTimeToLive;
  private final int regionIdleTimeout;
  private final int entryTimeToLive;
  private final int entryIdleTimeout;
  private final String customEntryTimeToLive;
  private final String customEntryIdleTimeout;
  private final boolean ignoreJTA;
  private final String dataPolicy;
  private final String scope;
  private final int initialCapacity;
  private final float loadFactor;
  private final boolean lockGrantor;
  private final boolean multicastEnabled;
  private final int concurrencyLevel;
  private final boolean indexMaintenanceSynchronous;
  private final boolean statisticsEnabled;
  private final boolean subscriptionConflationEnabled;
  private final boolean asyncConflationEnabled;
  private final String poolName;
  private final boolean cloningEnabled;
  private final String diskStoreName;
  private final String interestPolicy;
  private final boolean diskSynchronous;
  private final String compressorClassName;
  private final boolean offHeap;
  private final Set<String> asyncEventQueueIds;
  private final Set<String> gatewaySenderIds;

  /**
   *
   * This constructor is to be used by internal JMX framework only. User should not try to create an
   * instance of this class.
   */
  @ConstructorProperties({"cacheLoaderClassName", "cacheWriterClassName", "keyConstraintClassName",
      "valueConstraintClassName", "regionTimeToLive", "regionIdleTimeout", "entryTimeToLive",
      "entryIdleTimeout", "customEntryTimeToLive", "customEntryIdleTimeout", "ignoreJTA",
      "dataPolicy", "scope", "initialCapacity", "loadFactor", "lockGrantor", "multicastEnabled",
      "concurrencyLevel", "indexMaintenanceSynchronous", "statisticsEnabled",
      "subscriptionConflationEnabled", "asyncConflationEnabled", "poolName", "cloningEnabled",
      "diskStoreName", "interestPolicy", "diskSynchronous", "cacheListeners", "compressorClassName",
      "offHeap", "asyncEventQueueIds", "gatewaySenderIds"})


  public RegionAttributesData(String cacheLoaderClassName, String cacheWriterClassName,
      String keyConstraintClassName, String valueConstraintClassName, int regionTimeToLive,
      int regionIdleTimeout, int entryTimeToLive, int entryIdleTimeout,
      String customEntryTimeToLive, String customEntryIdleTimeout, boolean ignoreJTA,
      String dataPolicy, String scope, int initialCapacity, float loadFactor, boolean lockGrantor,
      boolean multicastEnabled, int concurrencyLevel, boolean indexMaintenanceSynchronous,
      boolean statisticsEnabled, boolean subscriptionConflationEnabled,
      boolean asyncConflationEnabled, String poolName, boolean cloningEnabled, String diskStoreName,
      String interestPolicy, boolean diskSynchronous, String[] cacheListeners,
      String compressorClassName, boolean offHeap, Set<String> asyncEventQueueIds,
      Set<String> gatewaySenderIds) {

    this.cacheLoaderClassName = cacheLoaderClassName;
    this.cacheWriterClassName = cacheWriterClassName;
    this.keyConstraintClassName = keyConstraintClassName;
    this.valueConstraintClassName = valueConstraintClassName;
    this.regionTimeToLive = regionTimeToLive;
    this.regionIdleTimeout = regionIdleTimeout;
    this.entryTimeToLive = entryTimeToLive;
    this.entryIdleTimeout = entryIdleTimeout;
    this.customEntryTimeToLive = customEntryTimeToLive;
    this.customEntryIdleTimeout = customEntryIdleTimeout;
    this.ignoreJTA = ignoreJTA;
    this.dataPolicy = dataPolicy;
    this.scope = scope;
    this.initialCapacity = initialCapacity;
    this.loadFactor = loadFactor;
    this.lockGrantor = lockGrantor;
    this.multicastEnabled = multicastEnabled;
    this.concurrencyLevel = concurrencyLevel;
    this.indexMaintenanceSynchronous = indexMaintenanceSynchronous;
    this.statisticsEnabled = statisticsEnabled;
    this.subscriptionConflationEnabled = subscriptionConflationEnabled;
    this.asyncConflationEnabled = asyncConflationEnabled;
    this.poolName = poolName;
    this.cloningEnabled = cloningEnabled;
    this.diskStoreName = diskStoreName;
    this.interestPolicy = interestPolicy;
    this.diskSynchronous = diskSynchronous;
    this.cacheListeners = cacheListeners;
    this.compressorClassName = compressorClassName;
    this.offHeap = offHeap;
    this.asyncEventQueueIds = asyncEventQueueIds;
    this.gatewaySenderIds = gatewaySenderIds;
  }

  /**
   * Returns the Class of cache loader associated with this region.
   */
  public String getCacheLoaderClassName() {
    return cacheLoaderClassName;
  }

  /**
   * Returns the Class of the cache writer associated with this region.
   */
  public String getCacheWriterClassName() {
    return cacheWriterClassName;
  }

  /**
   * Returns the Class that the keys in this region are constrained to (must be an instance of).
   */
  public String getKeyConstraintClassName() {
    return keyConstraintClassName;
  }

  /**
   * Returns the Class that the values in this region are constrained to (must be an instance of).
   */
  public String getValueConstraintClassName() {
    return valueConstraintClassName;
  }

  /**
   * Returns the time to live expiration for the Region.
   */
  public int getRegionTimeToLive() {
    return regionTimeToLive;
  }

  /**
   * Returns the idle timeout expiration for the Region.
   */
  public int getRegionIdleTimeout() {
    return regionIdleTimeout;
  }

  /**
   * Returns the time to live expiration for entries in the Region.
   */
  public int getEntryTimeToLive() {
    return entryTimeToLive;
  }

  /**
   * Returns the idle timeout expiration for entries in the Region.
   */
  public int getEntryIdleTimeout() {
    return entryIdleTimeout;
  }

  /**
   * Returns the custom time to live expiration for entries in the Region, if one exists.
   */
  public String getCustomEntryTimeToLive() {
    return customEntryTimeToLive;
  }

  /**
   * Returns the custom idle timeout expiration for entries in the Region, if one exists.
   */
  public String getCustomEntryIdleTimeout() {
    return customEntryIdleTimeout;
  }

  /**
   * Returns whether JTA transactions are being ignored.
   *
   * @return True if JTA transactions are being ignored, false otherwise.
   */
  public boolean isIgnoreJTA() {
    return ignoreJTA;
  }

  /**
   * Returns the data policy.
   */
  public String getDataPolicy() {
    return dataPolicy;
  }

  /**
   * Returns the scope.
   */
  public String getScope() {
    return scope;
  }

  /**
   * Returns the initial capacity of entries in the Region.
   */
  public int getInitialCapacity() {
    return initialCapacity;
  }

  /**
   * Returns the load factor of entries in the Region.
   */
  public float getLoadFactor() {
    return loadFactor;
  }

  /**
   * Returns whether this member is configured to become the lock granter when the Region is
   * created. It does not indicate whether this member is currently the lock granter for the Region.
   *
   * @return True if this member is configured to start the Region as the lock granter, false
   *         otherwise. Always returns false if the scope of the Region is not
   *         <code>Scope.GLOBAL</code>
   */
  public boolean isLockGrantor() {
    return lockGrantor;
  }

  /**
   * Returns whether multicast communication is enabled for the Region.
   *
   * @return True if multicast communication is enabled, false otherwise.
   */
  public boolean isMulticastEnabled() {
    return multicastEnabled;
  }

  /**
   * Returns the concurrency level for entries in the Region.
   */
  public int getConcurrencyLevel() {
    return concurrencyLevel;
  }

  /**
   * Returns whether query service index maintenance will be done synchronously.
   *
   * @return True if query service index maintenance will be done synchronously or false if it will
   *         be done asynchronously.
   */
  public boolean isIndexMaintenanceSynchronous() {
    return indexMaintenanceSynchronous;
  }

  /**
   * Returns whether statistic collection is enabled for the Region and its entries.
   *
   * @return True if statistic collection is enabled, false otherwise.
   */
  public boolean isStatisticsEnabled() {
    return statisticsEnabled;
  }

  /**
   * Returns whether conflation is enabled for sending messages from a cache server to its clients.
   * This value only has meaning for client to server communication and is not relevant for peer to
   * peer communication.
   *
   * @return True if conflation is enabled, false otherwise.
   */
  public boolean isSubscriptionConflationEnabled() {
    return subscriptionConflationEnabled;
  }

  /**
   * Returns whether asynchronous conflation is enabled for sending messages to peers.
   *
   * @return True if asynchronous conflation is enabled, false otherwise.
   */
  public boolean isAsyncConflationEnabled() {
    return asyncConflationEnabled;
  }

  /**
   * Returns the name of the Pool that this Region will use to communicate with servers, if any.
   *
   * @return The name of the Pool used to communicate with servers or null if the host member
   *         communicates with peers.
   */
  public String getPoolName() {
    return poolName;
  }

  /**
   * Returns whether cloning is enabled.
   *
   * @return True if cloning is enabled, false otherwise.
   */
  public boolean isCloningEnabled() {
    return cloningEnabled;
  }

  /**
   * Returns the name of the DiskStore associated with the Region.
   */
  public String getDiskStoreName() {
    return diskStoreName;
  }

  /**
   * Returns the subscriber's interest policy.
   */
  public String getInterestPolicy() {
    return interestPolicy;
  }

  /**
   * Returns whether disk writes are synchronous.
   *
   * @return True if disk writes are synchronous, false otherwise.
   */
  public boolean isDiskSynchronous() {
    return diskSynchronous;
  }

  /**
   * Returns a list of CacheListeners for the Region. An empty array if no listener is specified.
   */
  public String[] getCacheListeners() {
    return cacheListeners;
  }

  /**
   * Returns the compressor class name used by the region.
   *
   * @return null if no compression is used.
   */
  public String getCompressorClassName() {
    return compressorClassName;
  }

  /**
   * Returns true if the region uses off-heap memory.
   *
   * @return false if the region does not use off-heap memory.
   */
  public boolean getOffHeap() {
    return offHeap;
  }

  /**
   * Returns the set of async event queue IDs.
   *
   * @return a set of ids.
   */
  public Set<String> getAsyncEventQueueIds() {
    return asyncEventQueueIds;
  }

  /**
   * Returns the set of gateway sender IDs.
   *
   * @return a set of ids.
   */
  public Set<String> getGatewaySenderIds() {
    return gatewaySenderIds;
  }

  /**
   * String representation of RegionAttributesData
   */
  @Override
  public String toString() {
    return "RegionAttributesData [asyncConflationEnabled=" + asyncConflationEnabled
        + ", asyncEventQueueIds=" + asyncEventQueueIds + ", cacheListeners="
        + Arrays.toString(cacheListeners) + ", cacheLoaderClassName=" + cacheLoaderClassName
        + ", cacheWriterClassName=" + cacheWriterClassName + ", cloningEnabled=" + cloningEnabled
        + ", compressorClassName=" + compressorClassName + ", concurrencyLevel=" + concurrencyLevel
        + ", customEntryIdleTimeout=" + customEntryIdleTimeout + ", customEntryTimeToLive="
        + customEntryTimeToLive + ", dataPolicy=" + dataPolicy + ", diskStoreName=" + diskStoreName
        + ", diskSynchronous=" + diskSynchronous + ", entryIdleTimeout=" + entryIdleTimeout
        + ", entryTimeToLive=" + entryTimeToLive + ", gatewaySenderIds=" + gatewaySenderIds
        + ", ignoreJTA=" + ignoreJTA + ", indexMaintenanceSynchronous="
        + indexMaintenanceSynchronous + ", initialCapacity=" + initialCapacity + ", interestPolicy="
        + interestPolicy + ", keyConstraintClassName=" + keyConstraintClassName + ", loadFactor="
        + loadFactor + ", lockGrantor=" + lockGrantor + ", multicastEnabled=" + multicastEnabled
        + ", offHeap=" + offHeap + ", poolName=" + poolName + ", regionIdleTimeout="
        + regionIdleTimeout + ", regionTimeToLive=" + regionTimeToLive + ", scope=" + scope
        + ", statisticsEnabled=" + statisticsEnabled + ", subscriptionConflationEnabled="
        + subscriptionConflationEnabled + ", valueConstraintClassName=" + valueConstraintClassName
        + "]";
  }

}
