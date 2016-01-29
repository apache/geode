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

import java.io.File;
import java.util.Properties;

import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.distributed.LeaseExpiredException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * <code>RegionFactory</code> is used to create {@link Region regions}
 * in a {@link Cache cache}.
 * Instances of this interface can be created:
<ul>
<li>using a {@link RegionShortcut shortcut} by calling {@link Cache#createRegionFactory(RegionShortcut)} which will initialize the factory with the shortcut's region attributes
<li>using a named region attribute by calling {@link Cache#createRegionFactory(String)} which will initialize the factory the named region attributes
<li>using a region attribute instance by calling {@link Cache#createRegionFactory(RegionAttributes)} which will initialize the factory with the given region attributes
<li>by calling {@link Cache#createRegionFactory()} which will initialize the factory with defaults
</ul>
Once the factory has been created it can be customized with its setter methods.
<p>
The final step is to produce a {@link Region} by calling {@link #create(String)}.
<p>Example: Create a replicate region with a CacheListener
<PRE>
  Cache c = new CacheFactory().create();
  // Create replicate region.
  // Add a cache listener before creating region
  Region r = c.createRegionFactory(REPLICATE)
    .addCacheListener(myListener)
    .create("replicate");
</PRE>
<p>Example: Create a partition region that has redundancy
<PRE>
  Cache c = new CacheFactory().create();
  // Create replicate region.
  // Add a cache listener before creating region
  Region r = c.createRegionFactory(PARTITION_REDUNDANT)
    .create("partition");
</PRE>
 *
 * @author Mitch Thomas
 * @since 5.0
 */

public class RegionFactory<K,V>
{
  private final AttributesFactory<K,V> attrsFactory;
  private final GemFireCacheImpl cache;

  /**
   * For internal use only.
   * @since 6.5
   */
  protected RegionFactory(GemFireCacheImpl cache) {
    this.cache = cache;
    this.attrsFactory = new AttributesFactory<K,V>();
  }

  /**
   * For internal use only.
   * @since 6.5
   */
  protected RegionFactory(GemFireCacheImpl cache, RegionShortcut pra) {
    this.cache = cache;
    RegionAttributes ra = cache.getRegionAttributes(pra.toString());
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + pra
                                      + " has been removed.");
    }
    this.attrsFactory = new AttributesFactory<K,V>(ra);
  }

  /**
   * For internal use only.
   * @since 6.5
   */
  protected RegionFactory(GemFireCacheImpl cache, RegionAttributes ra) {
    this.cache = cache;
    this.attrsFactory = new AttributesFactory<K,V>(ra);
  }

  /**
   * For internal use only.
   * @since 6.5
   */
  protected RegionFactory(GemFireCacheImpl cache, String regionAttributesId) {
    this.cache = cache;
    RegionAttributes<K,V> ra = getCache().getRegionAttributes(regionAttributesId);
    if (ra == null) {
      throw new IllegalStateException(LocalizedStrings.RegionFactory_NO_ATTRIBUTES_ASSOCIATED_WITH_0.toLocalizedString(regionAttributesId));
    }
    this.attrsFactory = new AttributesFactory<K,V>(ra);
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If
   * no DistributedSystem exists it creates a DistributedSystem with default
   * configuration, otherwise it uses the existing DistributedSystem. The
   * default Region configuration is used.
   *
   * @throws CacheException
   *           if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link Cache#createRegionFactory()} instead.
   */
  @Deprecated
  public RegionFactory() throws CacheWriterException, RegionExistsException,
    TimeoutException {
    this((GemFireCacheImpl)new CacheFactory().create());
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If
   * no DistributedSystem exists it creates a DistributedSystem with default
   * configuration, otherwise it uses the existing DistributedSystem. The Region
   * configuration is initialized using the provided RegionAttributes.
   *
   * @throws CacheException
   *           if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link Cache#createRegionFactory(RegionAttributes)} instead.
   */
  @Deprecated
  public RegionFactory(RegionAttributes<K,V> regionAttributes)
    throws CacheWriterException, RegionExistsException, TimeoutException {
    this((GemFireCacheImpl)new CacheFactory().create(), regionAttributes);
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If
   * no DistributedSystem exists it creates a DistributedSystem with default
   * configuration, otherwise it uses the existing DistributedSystem. The Region
   * configuration is initialized using the RegionAttributes identified in the
   * cache.xml file by the provided identifier.
   *
   * @param regionAttributesId
   *          that identifies a set of RegionAttributes in the cache-xml file.
   * @see Cache#getRegionAttributes
   * @throws IllegalArgumentException
   *           if there are no attributes associated with the id
   * @throws CacheException
   *           if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link Cache#createRegionFactory(String)} instead.
   */
  @Deprecated
  public RegionFactory(String regionAttributesId) throws CacheWriterException,
               RegionExistsException, TimeoutException {
    this((GemFireCacheImpl)new CacheFactory().create(), regionAttributesId);
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If
   * a DistributedSystem already exists with the same properties it uses that
   * DistributedSystem, otherwise a DistributedSystem is created using the
   * provided properties. The default Region configuration is used.
   *
   * @param distributedSystemProperties
   *          an instance of Properties containing
   *          <code>DistributedSystem</code configuration
   * @throws CacheException if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link CacheFactory#CacheFactory(Properties)} and {@link Cache#createRegionFactory()} instead.
   */
  @Deprecated
  public RegionFactory(Properties distributedSystemProperties)
    throws CacheWriterException, RegionExistsException, TimeoutException {
    this((GemFireCacheImpl)new CacheFactory(distributedSystemProperties).create());
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If
   * a DistributedSystem already exists with the same properties it uses that
   * DistributedSystem, otherwise a DistributedSystem is created using the
   * provided properties. The initial Region configuration is set using the
   * RegionAttributes provided.
   *
   * @param distributedSystemProperties
   *          properties used to either find or create a DistributedSystem.
   * @param regionAttributes
   *          the initial Region configuration for this RegionFactory.
   * @throws CacheException
   *           if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link CacheFactory#CacheFactory(Properties)} and {@link Cache#createRegionFactory(RegionAttributes)} instead.
   */
  @Deprecated
  public RegionFactory(Properties distributedSystemProperties,
      RegionAttributes<K,V> regionAttributes) throws CacheWriterException,
      RegionExistsException, TimeoutException {
    this((GemFireCacheImpl)new CacheFactory(distributedSystemProperties).create(), regionAttributes);
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If
   * a DistributedSystem already exists whose properties match those provied, it
   * uses that DistributedSystem. The Region configuration is initialized using
   * the RegionAttributes identified in the cache.xml file by the provided
   * identifier.
   *
   * @param distributedSystemProperties
   *          properties used to either find or create a DistributedSystem.
   * @param regionAttributesId
   *          the identifier for set of RegionAttributes in the cache.xml file
   *          used as the initial Region configuration for this RegionFactory.
   * @throws IllegalArgumentException
   *           if there are no attributes associated with the id
   *
   * @throws CacheException
   *           if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link CacheFactory#CacheFactory(Properties)} and {@link Cache#createRegionFactory(String)} instead.
   *
   */
  @Deprecated
  public RegionFactory(Properties distributedSystemProperties,
      String regionAttributesId) throws CacheWriterException,
      RegionExistsException, TimeoutException {
    this((GemFireCacheImpl)new CacheFactory(distributedSystemProperties).create(), regionAttributesId);
  }

  /**
   * Returns the cache used by this factory.
   */
  private synchronized GemFireCacheImpl getCache() {
    return this.cache;
  }
  
  /**
   * Sets the cache loader for the next <code>RegionAttributes</code> created.
   *
   * @param cacheLoader
   *          the cache loader or null if no loader
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setCacheLoader
   *
   */
  public RegionFactory<K,V> setCacheLoader(CacheLoader<K,V> cacheLoader)
  {
    this.attrsFactory.setCacheLoader(cacheLoader);
    return this;
  }

  /**
   * Sets the cache writer for the next <code>RegionAttributes</code> created.
   *
   * @param cacheWriter
   *          the cache writer or null if no cache writer
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setCacheWriter
   */
  public RegionFactory<K,V> setCacheWriter(CacheWriter<K,V> cacheWriter)
  {
    this.attrsFactory.setCacheWriter(cacheWriter);
    return this;
  }

  /**
   * Adds a cache listener to the end of the list of cache listeners on this factory.
   * @param aListener the cache listener to add
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if <code>aListener</code> is null
   * @see AttributesFactory#addCacheListener
   */
  public RegionFactory<K,V> addCacheListener(CacheListener<K,V> aListener)
  {
    this.attrsFactory.addCacheListener(aListener);
    return this;
  }

  /**
   * Removes all cache listeners and then adds each listener in the specified array.
   * for the next <code>RegionAttributes</code> created.
   * @param newListeners a possibly null or empty array of listeners to add to this factory.
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if the <code>newListeners</code> array has a null element
   * @see AttributesFactory#initCacheListeners
   */
  public RegionFactory<K,V> initCacheListeners(CacheListener<K,V>[] newListeners)
  {
    this.attrsFactory.initCacheListeners(newListeners);
    return this;
  }

  /**
   * Sets the eviction attributes that controls growth of the Region to be created.
   *
   * @param evictionAttributes for the Region to create
   * @return a reference to this RegionFactory object
   */
  public RegionFactory<K,V> setEvictionAttributes(EvictionAttributes evictionAttributes) {
    this.attrsFactory.setEvictionAttributes(evictionAttributes);
    return this;
  }

  /**
   * Sets the idleTimeout expiration attributes for region entries for the next
   * <code>RegionAttributes</code> created.
   * Note that the XML element that corresponds to this method "entry-idle-time", does not include "out" in its name.
   *
   * @param idleTimeout
   *          the idleTimeout ExpirationAttributes for entries in this region
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if idleTimeout is null
   * @see AttributesFactory#setEntryIdleTimeout
   */
  public RegionFactory<K,V> setEntryIdleTimeout(ExpirationAttributes idleTimeout)
  {
    this.attrsFactory.setEntryIdleTimeout(idleTimeout);
    return this;
  }

  /**
   * Sets the custom idleTimeout for the next <code>RegionAttributes</code>
   * created.
   * 
   * @param custom the custom method
   * @return the receiver
   * @see AttributesFactory#setCustomEntryIdleTimeout(CustomExpiry)
   */
  public RegionFactory<K,V> setCustomEntryIdleTimeout(CustomExpiry<K,V> custom) {
    this.attrsFactory.setCustomEntryIdleTimeout(custom);
    return this;
  }
  
  /**
   * Sets the timeToLive expiration attributes for region entries for the next
   * <code>RegionAttributes</code> created.
   *
   * @param timeToLive
   *          the timeToLive ExpirationAttributes for entries in this region
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if timeToLive is null
   * @see AttributesFactory#setEntryTimeToLive
   */
  public RegionFactory<K,V> setEntryTimeToLive(ExpirationAttributes timeToLive)
  {
    this.attrsFactory.setEntryTimeToLive(timeToLive);
    return this;
  }

  /**
   * Sets the custom timeToLive expiration method for the next 
   * <code>RegionAttributes</code> created.
   * @param custom the custom method
   * @return the receiver
   * @see AttributesFactory#setCustomEntryTimeToLive(CustomExpiry)
   */
  public RegionFactory<K,V> setCustomEntryTimeToLive(CustomExpiry<K,V> custom) {
    this.attrsFactory.setCustomEntryTimeToLive(custom);
    return this;
  }
  
  /**
   * Sets the idleTimeout expiration attributes for the region itself for the
   * next <code>RegionAttributes</code> created.
   * Note that the XML element that corresponds to this method "region-idle-time", does not include "out" in its name.
   *
   * @param idleTimeout
   *          the ExpirationAttributes for this region idleTimeout
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if idleTimeout is null
   * @see AttributesFactory#setRegionIdleTimeout
   */
  public RegionFactory<K,V> setRegionIdleTimeout(ExpirationAttributes idleTimeout)
  {
    this.attrsFactory.setRegionIdleTimeout(idleTimeout);
    return this;
  }

  /**
   * Sets the timeToLive expiration attributes for the region itself for the
   * next <code>RegionAttributes</code> created.
   *
   * @param timeToLive
   *          the ExpirationAttributes for this region timeToLive
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if timeToLive is null
   * @see AttributesFactory#setRegionTimeToLive
   */
  public RegionFactory<K,V> setRegionTimeToLive(ExpirationAttributes timeToLive)
  {
    this.attrsFactory.setRegionTimeToLive(timeToLive);
    return this;
  }

  /**
   * Set custom {@link EvictionCriteria} for the region with start time and
   * interval of evictor task to be run in milliseconds, or evict incoming rows
   * in case both start and frequency are specified as zero.
   * 
   * @param criteria
   *          an {@link EvictionCriteria} to be used for eviction for HDFS
   *          persistent regions
   * @param start
   *          the start time at which periodic evictor task should be first
   *          fired to apply the provided {@link EvictionCriteria}; if this is
   *          zero then current time is used for the first invocation of evictor
   * @param interval
   *          the periodic frequency at which to run the evictor task after the
   *          initial start; if this is if both start and frequency are zero
   *          then {@link EvictionCriteria} is applied on incoming insert/update
   *          to determine whether it is to be retained
   */
  public RegionFactory<K, V> setCustomEvictionAttributes(
      EvictionCriteria<K, V> criteria, long start, long interval) {
    this.attrsFactory.setCustomEvictionAttributes(criteria, start, interval);
    return this;
  }

  /**
   * Sets the scope for the next <code>RegionAttributes</code> created.
   *
   * @param scopeType
   *          the type of Scope to use for the region
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if scopeType is null
   * @see AttributesFactory#setScope
   */
  public RegionFactory<K,V> setScope(Scope scopeType)
  {
    this.attrsFactory.setScope(scopeType);
    return this;
  }

  /**
   * Sets the data policy for the next <code>RegionAttributes</code> created.
   *
   * @param dataPolicy
   *          The type of mirroring to use for the region
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if dataPolicy is null
   * @see AttributesFactory#setDataPolicy
   */
  public RegionFactory<K,V> setDataPolicy(DataPolicy dataPolicy)
  {
    this.attrsFactory.setDataPolicy(dataPolicy);
    return this;
  }

  /**
   * Sets for this region whether or not acks are sent after an update is processed.
   *
   * @param earlyAck set to true for the acknowledgement to be sent prior to processing the update
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setEarlyAck(boolean)
   * @deprecated As of 6.5 this setting no longer has any effect.
   */
  @Deprecated
  public RegionFactory<K,V> setEarlyAck(boolean earlyAck) {
    this.attrsFactory.setEarlyAck(earlyAck);
    return this;
  }

  /**
   * Sets whether distributed operations on this region should attempt to use multicast.
   *
   * @since 5.0
   * @param value true to enable multicast
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setMulticastEnabled(boolean)
   */
  public RegionFactory<K,V> setMulticastEnabled(boolean value) {
    this.attrsFactory.setMulticastEnabled(value);
    return this;
  }
  
  /**
   * Sets the pool name attribute.
   * This causes regions that use these attributes
   * to be a client region which communicates with the
   * servers that the connection pool communicates with.
   * <p>If this attribute is set to <code>null</code> or <code>""</code>
   * then the connection pool is disabled causing regions that use these attributes
   * to be communicate with peers instead of servers.
   * <p>The named connection pool must exist on the cache at the time these
   * attributes are used to create a region. See {@link PoolManager#createFactory}
   * for how to create a connection pool.
   * @param poolName the name of the connection pool to use; if <code>null</code>
   * or <code>""</code> then the connection pool attribute is disabled for regions
   * using these attributes.
   * @return a reference to this RegionFactory object
   * @throws IllegalStateException if a cache loader or cache writer has already
   * been set.
   * @since 5.7
   */
  public RegionFactory<K,V> setPoolName(String poolName) {
    this.attrsFactory.setPoolName(poolName);
    return this;
  }

  /**
   * Sets whether or not this region should be considered a publisher.
   * @since 5.0
   * @deprecated as of 6.5
   */
  @Deprecated
  public void setPublisher(boolean v) {
//    this.attrsFactory.setPublisher(v);
  }
  /**
   * Sets whether or not conflation is enabled for sending messages
   * to async peers.
   * @param value true to enable async conflation
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setEnableAsyncConflation(boolean)
   */
  public RegionFactory<K,V> setEnableAsyncConflation(boolean value) {
    this.attrsFactory.setEnableAsyncConflation(value);
    return this;
  }

  /**
   * Sets whether or not conflation is enabled for sending messages
   * from a cache server to its clients.
   * @param value true to enable subscription conflation
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setEnableSubscriptionConflation(boolean)
   */
  public RegionFactory<K,V> setEnableSubscriptionConflation(boolean value) {
    this.attrsFactory.setEnableSubscriptionConflation(value);
    return this;
  }

  /**
   * Sets the key constraint for the next <code>RegionAttributes</code>
   * created. Keys in the region will be constrained to this class (or
   * subclass). Any attempt to store a key of an incompatible type in the region
   * will cause a <code>ClassCastException</code> to be thrown.
   *
   * @param keyConstraint
   *          The Class to constrain the keys to, or null if no constraint
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if <code>keyConstraint</code> is a class denoting a primitive
   *           type
   * @see AttributesFactory#setKeyConstraint
   */
  public RegionFactory<K,V> setKeyConstraint(Class<K> keyConstraint)
  {
    this.attrsFactory.setKeyConstraint(keyConstraint);
    return this;
  }

  /**
   * Sets the value constraint for the next <code>RegionAttributes</code>
   * created. Values in the region will be constrained to this class (or
   * subclass). Any attempt to store a value of an incompatible type in the
   * region will cause a <code>ClassCastException</code> to be thrown.
   *
   * @param valueConstraint
   *          The Class to constrain the values to, or null if no constraint
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if <code>valueConstraint</code> is a class denoting a primitive
   *           type
   * @see AttributesFactory#setValueConstraint
   */
  public RegionFactory<K,V> setValueConstraint(Class<V> valueConstraint)
  {
    this.attrsFactory.setValueConstraint(valueConstraint);
    return this;
  }

  /**
   * Sets the entry initial capacity for the next <code>RegionAttributes</code>
   * created. This value is used in initializing the map that holds the entries.
   *
   * @param initialCapacity
   *          the initial capacity of the entry map
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if initialCapacity is negative.
   * @see java.util.HashMap
   * @see AttributesFactory#setInitialCapacity
   */
  public RegionFactory<K,V> setInitialCapacity(int initialCapacity)
  {
    this.attrsFactory.setInitialCapacity(initialCapacity);
    return this;
  }

  /**
   * Sets the entry load factor for the next <code>RegionAttributes</code>
   * created. This value is used in initializing the map that holds the entries.
   *
   * @param loadFactor
   *          the load factor of the entry map
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if loadFactor is nonpositive
   * @see java.util.HashMap
   * @see AttributesFactory#setLoadFactor
   */
  public RegionFactory<K,V> setLoadFactor(float loadFactor)
  {
    this.attrsFactory.setLoadFactor(loadFactor);
    return this;
  }

  /**
   * Sets the concurrency level tof the next <code>RegionAttributes</code>
   * created. This value is used in initializing the map that holds the entries.
   *
   * @param concurrencyLevel
   *          the concurrency level of the entry map
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException
   *           if concurrencyLevel is nonpositive
   * @see AttributesFactory#setConcurrencyLevel
   */
  public RegionFactory<K,V> setConcurrencyLevel(int concurrencyLevel)
  {
    this.attrsFactory.setConcurrencyLevel(concurrencyLevel);
    return this;
  }

  /**
   * Enables a versioning system that detects concurrent modifications and
   * ensures that region contents are consistent across the distributed
   * system.  This setting must be the same in each member having the region.
   *
   * @since 7.0
   * @param enabled whether concurrency checks should be enabled for the region
   * @see AttributesFactory#setConcurrencyChecksEnabled
   */
  public RegionFactory<K,V> setConcurrencyChecksEnabled(boolean enabled)
  {
    this.attrsFactory.setConcurrencyChecksEnabled(enabled);
    return this;
  }

  /**
   * Returns whether or not disk writes are asynchronous.
   *
   * @return a reference to this RegionFactory object
   * @see Region#writeToDisk
   * @see AttributesFactory#setDiskWriteAttributes
   * @deprecated as of 6.5 use {@link #setDiskStoreName} instead
   *
   */
  @Deprecated
  public RegionFactory<K,V> setDiskWriteAttributes(DiskWriteAttributes attrs)
  {
    this.attrsFactory.setDiskWriteAttributes(attrs);
    return this;
  }

  /**
   * Sets the DiskStore name attribute.
   * This causes the region to belong to the DiskStore.
   * @param name the name of the diskstore
   * @return a reference to this RegionFactory object
   * @since 6.5 
   * 
   * @see AttributesFactory#setDiskStoreName
   */
  public RegionFactory<K,V> setDiskStoreName(String name) {
    this.attrsFactory.setDiskStoreName(name);
    return this;
  }
  
  /**
   * Sets whether or not the writing to the disk is synchronous.
   * 
   * @param isSynchronous
   *          boolean if true indicates synchronous writes
   * @return a reference to this RegionFactory object
   * @since 6.5 
   */
  public RegionFactory<K,V> setDiskSynchronous(boolean isSynchronous)
  {
    this.attrsFactory.setDiskSynchronous(isSynchronous);
    return this;
  }

  /**
   * Sets the directories to which the region's data are written. If multiple
   * directories are used, GemFire will attempt to distribute the data evenly
   * amongst them.
   *
   * @return a reference to this RegionFactory object
   *
   * @see AttributesFactory#setDiskDirs
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setDiskDirs} instead
   */
  @Deprecated
  public RegionFactory<K,V> setDiskDirs(File[] diskDirs)
  {
    this.attrsFactory.setDiskDirs(diskDirs);
    return this;
  }

  /**
   * Sets the directories to which the region's data are written and also set their sizes in megabytes
   *  
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if length of the size array
   * does not match to the length of the dir array
   *   
   * @since 5.1
   * @see AttributesFactory#setDiskDirsAndSizes
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setDiskDirsAndSizes} instead
   */
  @Deprecated
  public RegionFactory<K,V> setDiskDirsAndSizes(File[] diskDirs,int[] diskSizes) {
    this.attrsFactory.setDiskDirsAndSizes(diskDirs, diskSizes);
    return this;
  }

  /**
   * Sets the <code>PartitionAttributes</code> that describe how the region is
   * partitioned among members of the distributed system.
   *
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setPartitionAttributes
   */
  public RegionFactory<K,V> setPartitionAttributes(PartitionAttributes partition)
  {
    this.attrsFactory.setPartitionAttributes(partition);
    return this;
  }

  /**
   * Sets the <code>MembershipAttributes</code> that describe the membership
   * roles required for reliable access to the region.
   *
   * @param ra the MembershipAttributes to use
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setMembershipAttributes
   */
  public RegionFactory<K,V> setMembershipAttributes(MembershipAttributes ra) {
    this.attrsFactory.setMembershipAttributes(ra);
    return this;
  }

  /**
   * Sets how indexes on this region are kept current.
   *
   * @param synchronous
   *          whether indexes are maintained in a synchronized fashion
   * @return a reference to this RegionFactory object
   */
  public RegionFactory<K,V> setIndexMaintenanceSynchronous(boolean synchronous)
  {
    this.attrsFactory.setIndexMaintenanceSynchronous(synchronous);
    return this;
  }

  /**
   * Sets whether statistics are enabled for this region and its entries.
   *
   * @param statisticsEnabled
   *          whether statistics are enabled
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setStatisticsEnabled
   */
  public RegionFactory<K,V> setStatisticsEnabled(boolean statisticsEnabled)
  {
    this.attrsFactory.setStatisticsEnabled(statisticsEnabled);
    return this;
  }

  /**
   * Sets whether operations on this region should be controlled by
   * JTA transactions or not
   * @since 5.0
   */
  public RegionFactory<K,V> setIgnoreJTA(boolean flag) {
    this.attrsFactory.setIgnoreJTA(flag);
    return this;
  }

  /**
   * Sets whether this region should become lock grantor.
   *
   * @param isLockGrantor
   *          whether this region should become lock grantor
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setLockGrantor
   */
  public RegionFactory<K,V> setLockGrantor(boolean isLockGrantor)
  {
    this.attrsFactory.setLockGrantor(isLockGrantor);
    return this;
  }

  /**
   * Sets the kind of interest this region has in events occuring in other caches that define
   * the region by the same name.
   * @param sa the attributes decribing the interest
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setSubscriptionAttributes(SubscriptionAttributes)
   */
  public RegionFactory<K,V> setSubscriptionAttributes(SubscriptionAttributes sa) {
    this.attrsFactory.setSubscriptionAttributes(sa);
    return this;
  }

  /**
   * Creates a region with the given name in this factory's {@link Cache}
   * using the configuration contained in this factory. Validation of the
   * provided attributes may cause exceptions to be thrown if there are problems
   * with the configuration data.
   *
   * @param name
   *          the name of the region to create
   *
   * @return the region object
   * @throws LeaseExpiredException
   *           if lease expired on distributed lock for Scope.GLOBAL
   * @throws RegionExistsException
   *           if a region, shared or unshared, is already in this cache
   * @throws TimeoutException
   *           if timed out getting distributed lock for Scope.GLOBAL
   * @throws CacheClosedException
   *           if the cache is closed
   * @throws IllegalStateException
   *           if the supplied RegionAttributes are incompatible with this region
   *           in another cache in the distributed system (see
   *           {@link AttributesFactory} for compatibility rules)
   */
  @SuppressWarnings("unchecked")
  public Region<K,V> create(String name) throws CacheExistsException,
             RegionExistsException, CacheWriterException, TimeoutException
  {
    @SuppressWarnings("deprecation")
    RegionAttributes<K,V> ra = this.attrsFactory.create();
    return getCache().createRegion(name, ra);
  }
  /**
   * Creates a sub-region in the {@link Cache} using
   * the configuration contained in this RegionFactory. Validation of the
   * provided attributes may cause exceptions to be thrown if there are problems
   * with the configuration data.
   *
   * @param parent
   *          the existing region that will contain the created sub-region
   * @param name
   *          the name of the region to create
   *
   * @return the region object
   * @throws RegionExistsException
   *           if a region with the given name already exists in this cache
   * @throws RegionDestroyedException
   *           if the parent region has been closed or destroyed
   * @throws CacheClosedException
   *           if the cache is closed
   * @since 7.0
   */
  @SuppressWarnings("unchecked")
  public Region<K,V> createSubregion(Region<?,?> parent, String name) throws RegionExistsException {
    @SuppressWarnings("deprecation")
    RegionAttributes<K,V> ra = this.attrsFactory.create();
    return ((LocalRegion)parent).createSubregion(name, ra);
  }
  
  /**
   * Sets cloning on region
   * @param cloningEnable
   * @return a reference to this RegionFactory object
   * @since 6.1
   * @see AttributesFactory#setCloningEnabled
   */
  public RegionFactory<K,V> setCloningEnabled(boolean cloningEnable) {
    this.attrsFactory.setCloningEnabled(cloningEnable);
    return this;
  }

  /**
   * Adds a gatewaySenderId to the RegionAttributes
   * @param gatewaySenderId
   * @return a reference to this RegionFactory object
   * @since 7.0
   * @see AttributesFactory#addGatewaySenderId(String) 
   */
  public RegionFactory<K,V> addGatewaySenderId(String gatewaySenderId)
  {
    this.attrsFactory.addGatewaySenderId(gatewaySenderId);
    return this;
  } 
  
  /**
   * Adds a asyncEventQueueId to the RegionAttributes
   * 
   * @param asyncEventQueueId id of AsyncEventQueue 
   * @return a reference to this RegionFactory instance
   * @since 7.0
   */
  public RegionFactory<K,V> addAsyncEventQueueId(String asyncEventQueueId) {
    this.attrsFactory.addAsyncEventQueueId(asyncEventQueueId);
    return this;
  }

  /**
   * Set the compressor to be used by this region for compressing
   * region entry values.
   * @param compressor a compressor
   * @return a reference to this RegionFactory instance
   * @since 8.0
   */
  public RegionFactory<K,V> setCompressor(Compressor compressor) {
    this.attrsFactory.setCompressor(compressor);
    return this;
  }
  
  /**
   * Enables this region's usage of off-heap memory if true.
   * @param offHeap boolean flag to enable off-heap memory
   * @since 9.0
   */
  public RegionFactory<K,V> setOffHeap(boolean offHeap) {
    this.attrsFactory.setOffHeap(offHeap);
    return this;
  }
}
