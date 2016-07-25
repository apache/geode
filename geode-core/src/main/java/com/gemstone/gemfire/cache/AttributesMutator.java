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

package com.gemstone.gemfire.cache;


/**
 * Supports modification of certain region attributes after the region has been
 * created. It is recommended that the attributes be completely initialized
 * using an {@link AttributesFactory} before creating the region instead of
 * using an <code>AttributesMutator</code> after region creation. This will
 * avoid a potential performance penalty due to the additional
 * network traffic.
 *<p>
 * The setter methods all return the previous value of the attribute. 
 *
 *
 *
 * @see Region#getAttributesMutator
 * @see RegionAttributes
 * @see AttributesFactory
 * @since GemFire 3.0
 */
public interface AttributesMutator<K,V> {
  
  /** Returns the Region whose attributes this mutator affects.
   * @return the Region this mutator affects
   */
  public Region<K,V> getRegion();
  
  /** Changes the timeToLive expiration attributes for the region as a whole
   *
   * @param timeToLive the expiration attributes for the region timeToLive
   * @return the previous value of region timeToLive
   * @throws IllegalArgumentException if timeToLive is null or if the
   * ExpirationAction is LOCAL_INVALIDATE and the region is
   * {@link DataPolicy#withReplication replicated}
   * @throws IllegalStateException if statistics are disabled for this region.
   */
  public ExpirationAttributes setRegionTimeToLive(ExpirationAttributes timeToLive);
  
  /** Changes the idleTimeout expiration attributes for the region as a whole.
   * Resets the {@link CacheStatistics#getLastAccessedTime} for the region.
   *
   * @param idleTimeout the ExpirationAttributes for this region idleTimeout
   * @return the previous value of region idleTimeout
   * @throws IllegalArgumentException if idleTimeout is null or if the
   * ExpirationAction is LOCAL_INVALIDATE and the region is
   * {@link DataPolicy#withReplication replicated}
   * @throws IllegalStateException if statistics are disabled for this region.
   */
  public ExpirationAttributes setRegionIdleTimeout(ExpirationAttributes idleTimeout);
    
  /** Changes the timeToLive expiration attributes for values in this region.
   *
   * @param timeToLive the timeToLive expiration attributes for entries
   * @return the previous value of entry timeToLive
   * @throws IllegalArgumentException if timeToLive is null or if the
   * ExpirationAction is LOCAL_DESTROY and the region is {@link DataPolicy#withReplication replicated} or if 
   * the ExpirationAction is LOCAL_INVALIDATE and the region is 
   * {@link DataPolicy#withReplication replicated}
   * @throws IllegalStateException if statistics are disabled for this region.
   */
  public ExpirationAttributes setEntryTimeToLive(ExpirationAttributes timeToLive);
  
  /**
   * Changes the custom timeToLive for values in this region
   * @param custom the new CustomExpiry
   * @return the old CustomExpiry
   */
  public CustomExpiry<K,V> setCustomEntryTimeToLive(CustomExpiry<K,V> custom);
  
  /** Changes the idleTimeout expiration attributes for values in the region.
   *
   * @param idleTimeout the idleTimeout expiration attributes for entries
   * @return the previous value of entry idleTimeout
   * @throws IllegalArgumentException if idleTimeout is null or if the
   * ExpirationAction is LOCAL_DESTROY and the region is
   * {@link DataPolicy#withReplication replicated}
   * or if the the ExpirationAction is LOCAL_INVALIDATE and the region is 
   * {@link DataPolicy#withReplication replicated}
   * @see AttributesFactory#setStatisticsEnabled
   * @throws IllegalStateException if statistics are disabled for this region.
   */
  public ExpirationAttributes setEntryIdleTimeout(ExpirationAttributes idleTimeout);
  
  /** Changes the CustomExpiry for idleTimeout for values in the region
   * 
   * @param custom the new CustomExpiry
   * @return the old CustomExpiry
   */
  public CustomExpiry<K,V> setCustomEntryIdleTimeout(CustomExpiry<K,V> custom);
  
  /** Changes the CacheListener for the region.
   * Removes listeners already added and calls {@link CacheCallback#close} on each of them.
   * @param aListener a user defined cache listener
   * @return the previous CacheListener if a single one exists; otherwise null.
   * @throws IllegalStateException if more than one cache listener has already been added
   * @deprecated as of GemFire 5.0, use {@link #addCacheListener} or {@link #initCacheListeners} instead.
   */
  @Deprecated
  public CacheListener<K,V> setCacheListener(CacheListener<K,V> aListener);
  /**
   * Adds a cache listener to the end of the list of cache listeners on this region.
   * @param aListener the user defined cache listener to add to the region.
   * @throws IllegalArgumentException if <code>aListener</code> is null
   * @since GemFire 5.0
   */
  public void addCacheListener(CacheListener<K,V> aListener);
  /**
   * Removes a cache listener from the list of cache listeners on this region.
   * Does nothing if the specified listener has not been added.
   * If the specified listener has been added then {@link CacheCallback#close} will
   * be called on it; otherwise does nothing.
   * @param aListener the cache listener to remove from the region.
   * @throws IllegalArgumentException if <code>aListener</code> is null
   * @since GemFire 5.0
   */
  public void removeCacheListener(CacheListener<K,V> aListener);
  /**
   * Removes all cache listeners, calling {@link CacheCallback#close} on each of them, and then adds each listener in the specified array.
   * @param newListeners a possibly null or empty array of listeners to add to this region.
   * @throws IllegalArgumentException if the <code>newListeners</code> array has a null element
   * @since GemFire 5.0
   */
  public void initCacheListeners(CacheListener<K,V>[] newListeners);
  
  /** Changes the cache writer for the region.
   * @param cacheWriter the cache writer
   * @return the previous CacheWriter
   */
  public CacheWriter<K,V> setCacheWriter(CacheWriter<K,V> cacheWriter);
  
  /**
   * Changes the cache loader for the region.
   * 
   * Changing the cache loader for partitioned regions is not recommended due to
   * the fact that it can result in an inconsistent cache loader configuration.
   * This feature may be removed in future releases.
   * 
   * @param cacheLoader
   *          the cache loader
   * @return the previous CacheLoader
   */
  public CacheLoader<K,V> setCacheLoader(CacheLoader<K,V> cacheLoader);
  

  /** Allows changing the eviction controller attributes for the region.
   * 
   * @return the {@link EvictionAttributesMutator} used to change the EvictionAttributes
   */
  public EvictionAttributesMutator getEvictionAttributesMutator();

  /**
   * Sets cloning on region
   * Note: off-heap regions always behave as if cloning is enabled.
   * @param cloningEnable
   * @since GemFire 6.1
   */
  public void setCloningEnabled(boolean cloningEnable);
  /**
   * Returns whether or not cloning is enabled on region
   *
   * @return True if cloning is enabled (default);
   *         false cloning is not enabled.
   *
   * @since GemFire 6.1
   */
  public boolean getCloningEnabled();
  
  /**
   * Adds GatewaySenderId to the list of GatewaySenderIds of the region.
   * If the GatewaySenderId is not present on this VM then it will try to send it to other VM's
   * 
   * @param gatewaySenderId
   */
   public void addGatewaySenderId(String gatewaySenderId);
   
   /**
    * Removes GatewaySenderId from the list of GatewaySenderIds of the region.
    * @param gatewaySenderId 
    */
   public void removeGatewaySenderId(String gatewaySenderId);
   
   /**
    * Adds AsyncEventQueueId to the list of AsyncEventQueueId of the region.
    * @param asyncEventQueueId 
    */
   public void addAsyncEventQueueId(String asyncEventQueueId);
   
   /**
    * Removes AsyncEventQueueId from the list of AsyncEventQueuesId of the region.
    * @param asyncEventQueueId 
    */
   public void removeAsyncEventQueueId(String asyncEventQueueId);
}
