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
import java.util.Set;

import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.compression.Compressor;

/** Defines attributes for configuring a region.
 * These are <code>EvictionAttributes</code>,
 * <code>CacheListener</code>, <code>CacheLoader</code>, <code>CacheWriter</code>,
 * scope, data policy, and expiration attributes
 * for the region itself, expiration attributes for the region entries,
 * and whether statistics are enabled for the region and its entries.
 *
 * To create an instance of this interface use {@link AttributesFactory#createRegionAttributes}.
 *
 * For compatibility rules and default values, see {@link AttributesFactory}.
 *
 * <p>Note that the <code>RegionAttributes</code> are not distributed with the region.
 *
 *
 * @see AttributesFactory
 * @see AttributesMutator
 * @see Region#getAttributes
 * @see com.gemstone.gemfire.cache.EvictionAttributes
 * @since GemFire 2.0
 */
public interface RegionAttributes<K,V> {

  /** Returns the cache loader associated with this region.
   * @return the cache loader
   */
  public CacheLoader<K,V> getCacheLoader();

  /** Returns the cache writer associated with this region.
   * @return the cache writer
   */
  public CacheWriter<K,V> getCacheWriter();

  /** Returns the class that the keys in this region are constrained to.
   *
   * @return the <code>Class</code> the keys must be an
   * <code>instanceof</code>
   */
  public Class<K> getKeyConstraint();

  /** Returns the class that the values in this region are constrained to.
   *
   * @return the <code>Class</code> the values must be an
   * <code>instanceof</code>
   */
  
  public Class<V> getValueConstraint();


  /** Gets the <code>timeToLive</code> expiration attributes for the region as a whole.
   * Default is 0 which indicates that no expiration of this type will happen.
   * @return the timeToLive expiration attributes for this region
   */
  public ExpirationAttributes getRegionTimeToLive();

  /** Gets the idleTimeout expiration attributes for the region as a whole.
   * Default is 0 which indicates that no expiration of this type will happen.
   * Note that the XML element that corresponds to this method "region-idle-time", does not include "out" in its name.
   *
   * @return the IdleTimeout expiration attributes for this region
   */
  public ExpirationAttributes getRegionIdleTimeout();

  /** Gets the <code>timeToLive</code> expiration attributes for entries in this region.
   * Default is 0 which indicates that no expiration of this type is set.
   * @return the timeToLive expiration attributes for entries in this region
   */
  public ExpirationAttributes getEntryTimeToLive();

  /** Gets the <code>idleTimeout</code> expiration attributes for entries in this region.
   * Default is 0 which indicates that no expiration of this type is set.
   * Note that the XML element that corresponds to this method "entry-idle-time", does not include "out" in its name.
   * @return the idleTimeout expiration attributes for entries in this region
   */
  public ExpirationAttributes getEntryIdleTimeout();

  /** Gets the <code>entryTimeToLive</code> <code>CustomExpiry</code>, if any 
   * for entries in this region
   * @return the entryTimeToLive CustomExpiry for entries in this region
   */
  public CustomExpiry<K,V> getCustomEntryTimeToLive();
  
  /** Gets the <code>idleTimeout</code> <code>CustomExpiry</code>, if any 
   * for entries in this region
   * @return the idleTimeout CustomExpiry for entries in this region
   */
  public CustomExpiry<K,V> getCustomEntryIdleTimeout();

  /**
   * Gets the flag telling a region to ignore JTA transactions.
   * Default value is set to false.
   * @since GemFire 5.0
   */
  public boolean getIgnoreJTA();

  /** Returns the type of mirroring for this region.
   *
   * @return the region's <code>MirrorType</code>
   * @deprecated as of GemFire 5.0, use {@link #getDataPolicy} instead.
   */
  @Deprecated
  public MirrorType getMirrorType();


  /** Returns the data policy for this region.
   * Default value of DataPolicy is set to 'Normal'. Please refer the gemfire documentation for more details on this.
   *
   * @return the region's <code>DataPolicy</code>
   * @since GemFire 5.0
   */
  public DataPolicy getDataPolicy();

  /** Returns the scope of the region.
   *  Default scope is DISTRIBUTED_NO_ACK. Please refer the gemfire documentation for more details on this.
   * @return the region's <code>Scope</code>
   */
  public Scope getScope();


 /**
   * Attributes that control the size of the <code>Region</code> using an
   * {@link EvictionAlgorithm} and a {@link EvictionAction}.
   * @return the region's EvictionAttributes
   */
  public EvictionAttributes getEvictionAttributes();

  /** Returns the cache listener for the region.
   * @throws IllegalStateException if more than one cache listener exists on this attributes
   * @return the region's <code>CacheListener</code>
   * @deprecated as of GemFire 5.0, use {@link #getCacheListeners} instead
   */
  @Deprecated
  public CacheListener<K,V> getCacheListener();

  /** Returns an array of all the cache listeners on this attributes.
   * Modifications to the returned array will not effect the attributes.
   * @return the region's <code>CacheListener</code>s; an empty array if no listeners
   * @since GemFire 5.0
   */
  public CacheListener<K,V>[] getCacheListeners();

  // MAP ATTRIBUTES


  /** Returns the initial capacity of the entries map.
   * Default is 16.
   * @return the initial capacity of the entries map
   * @see java.util.HashMap
   */
  public int getInitialCapacity();

  /** Returns the load factor of the entries map.
   * Default is 0.75.
   * @return the load factor of the entries map
   * @see java.util.HashMap
   */
  public float getLoadFactor();

  /**
   * Returns true if this member is configured to be lock grantor for the
   * region. Result will always be false if the scope is not
   * <code>Scope.GLOBAL</code>.
   * <p>
   * This attribute does not indicate whether or not this member is currently
   * lock grantor. It only indicates that at the time of region creation, this
   * member should attempt to become lock grantor.
   * Default value is false.
   *
   * @return true if this member is configured to be lock grantor for the region
   * @see AttributesFactory
   * @see Region#becomeLockGrantor
   */
  public boolean isLockGrantor();

  /**
   * Returns true if multicast communications are enabled for this region.
   * Multicast must also be enabled in the DistributedSystem.
   * Default value is set to false.
   *
   * @since GemFire 5.0
   * @return true if this region is configured to allow use of multicast
   * for distributed messaging
   * @see AttributesFactory#setMulticastEnabled
   */
  public boolean getMulticastEnabled();

  /** Returns the concurrencyLevel of the entries map.
   * Default is 16.
   * @return the concurrencyLevel
   * @see AttributesFactory
   */
  public int getConcurrencyLevel();

  /**
   * Returns whether or not a persistent backup should be made of the
   * region (as opposed to just writing the overflow data to disk).
   *
   * @since GemFire 3.2
   * @deprecated as of GemFire 5.0, use {@link DataPolicy#PERSISTENT_REPLICATE} instead
   */
  @Deprecated
  public boolean getPersistBackup();

  /**
   * Returns the <code>DiskWriteAttributes</code> that configure how
   * the region is written to disk.
   *
   * @since GemFire 3.2
   * @deprecated as of 6.5 use {@link #getDiskStoreName} instead.
   */
  @Deprecated
  public DiskWriteAttributes getDiskWriteAttributes();

  /**
   * Returns the directories to which the region's data are written.  If
   * multiple directories are used, GemFire will attempt to distribute the
   * data evenly amongst them.
   *
   * @since GemFire 3.2
   * @deprecated as of 6.5 use {@link DiskStore#getDiskDirs} instead.
   */
  @Deprecated
  public File[] getDiskDirs();

  /**
   * Returns the value of <code>IndexMaintenanceSynchronous</code> which
   * specifies whether the region indexes are updated synchronously when a
   * region is modified or asynchronously in a background thread. 
   * Default value is true.
   *
   * @since GemFire 4.0
   */
  public boolean getIndexMaintenanceSynchronous();

  /**
   * Returns the <code>PartitionAttributes</code> that configure how
   * the region is partitioned.
   *
   * @since GemFire 5.0
   */
  public PartitionAttributes getPartitionAttributes();

  /**
   * Returns the <code>MembershipAttributes</code> that configure required
   * roles for reliable access to the region.
   *
   * @deprecated this API is scheduled to be removed
   */
  public MembershipAttributes getMembershipAttributes();

  /**
   * Returns the <code>SubscriptionAttributes</code> that configure
   * how this region behaves as a subscriber to remote caches.
   *
   * @since GemFire 5.0
   */
  public SubscriptionAttributes getSubscriptionAttributes();


  // STATISTICS
  /** Returns whether the statistics are enabled for this region and its
   * entries.
   * Default is false.
   * @return true if statistics are enabled
   */
  public boolean getStatisticsEnabled();

  /**
   * Returns whether or not acks are sent after an update is processed.
   * @return True if acks are sent after updates are processed;
   *         false if acks are sent before updates are processed.
   *
   * @since GemFire 4.1
   * @deprecated Setting early ack no longer has any effect.
   */
  @Deprecated
  public boolean getEarlyAck();

  /**
   * Returns whether or not this region is a publisher. Publishers are regions
   * on which distributed write operations are done.
   * @return True if a publisher;
   *         false if not (default).
   *
   * @since GemFire 4.2.3
   * @deprecated as of 6.5
   */
  public boolean getPublisher();

  /**
   * Returns whether or not conflation is enabled for sending
   * messages from a cache server to its clients.
   *
   * Note: This parameter is only valid for cache server to client
   * communication. It has no effect in peer to peer communication.
   *
   * @deprecated as of GemFire 5.0, use {@link #getEnableSubscriptionConflation} instead
   #
   * @return True if conflation is enabled;
   *         false conflation is not enabled (default).
   *
   * @since GemFire 4.2
   */
  @Deprecated
  public boolean getEnableConflation();

  /**
   * Returns whether or not conflation is enabled for sending
   * messages from a cache server to its clients.
   *
   * Note: This parameter is only valid for cache server to client
   * communication. It has no effect in peer to peer communication.
   *
   * @return True if conflation is enabled;
   *         false conflation is not enabled (default).
   *
   * @since GemFire 5.0
   * @deprecated as of GemFire 5.0, use {@link #getEnableSubscriptionConflation} instead
   */
  @Deprecated
  public boolean getEnableBridgeConflation();

  /**
   * Returns whether or not conflation is enabled for sending
   * messages from a cache server to its clients.
   *
   * Note: This parameter is only valid for cache server to client
   * communication. It has no effect in peer to peer communication.
   * 
   * Default is false.
   *
   * @return True if conflation is enabled;
   *         false conflation is not enabled (default).
   *
   * @since GemFire 5.7
   */
  public boolean getEnableSubscriptionConflation();
  /**
   * Returns whether or not async conflation is enabled for sending
   * messages to async peers.
   * 
   * Default is false.
   *
   * @return True if async conflation is enabled;
   *         false async conflation is not enabled (default).
   *
   * @since GemFire 4.2.3
   */
  public boolean getEnableAsyncConflation();

  /**
   * Returns the sizes of the disk directories in megabytes
   * @return int[] sizes of the disk directories
   * @deprecated as of 6.5 use {@link DiskStore#getDiskDirSizes} instead.
   */
  public int[] getDiskDirSizes();
  
  
  /**
   * Returns the name of the {@link Pool} that this region
   * will use to communicate with servers, if any.
   * Returns <code>null</code> if this region communicates with peers.
   * @return the name of the client-server {@link Pool}
   * this region will use for server communication; <code>null</code> is returned if
   * the region communicates with peers.
   * @since GemFire 5.7
   */
  public String getPoolName();
  
  /**
   * Returns whether or not cloning is enabled on region.
   * Default is false.
   *
   * @return True if cloning is enabled;
   *         false cloning is not enabled (default).
   *
   * @since GemFire 6.1
   */
  public boolean getCloningEnabled();

  /**
   * Returns the name of the {@link DiskStore} that this region belongs
   * to, if any.
   * Returns <code>null</code> if this region belongs to default {@link DiskStore}.
   * @return the name of the {@link DiskStore} of this region; 
   * <code>null</code> is returned if this region belongs to default 
   * {@link DiskStore}.
   * @since GemFire 6.5
   */
  public String getDiskStoreName();
  
  /**
   * Returns true if configured synchronous disk writes.
   * Default is set to true.
   * 
   * @return Returns true if writes to disk are synchronous and false otherwise
   * @since GemFire 6.5
   */
  public boolean isDiskSynchronous();
  
  /**
   * Returns a set of gatewaysenderIds
   */
  public Set<String> getGatewaySenderIds();
  
  /**
   * Returns a set of AsyncEventQueueIds added to the region
   */
  public Set<String> getAsyncEventQueueIds();
  
  /**
   * Returns true if concurrent update checks are turned on for this region.
   * <p>
   * When this is enabled, concurrent updates will be conflated if they are
   * applied out of order.
   * <p>
   * All members must set this attribute the same.
   * 
   * Default is set to true.
   * 
   * @since GemFire 7.0
   * @return true if concurrent update checks are turned on
   */
  public boolean getConcurrencyChecksEnabled();
  
  /**
   * Returns the compressor used by this region's entry values.
   * @since GemFire 8.0
   * @return null if the region does not have compression enabled.
   */
  public Compressor getCompressor();

  /**
   * Returns whether or not this region uses off-heap memory.
   * @return True if a usage of off-heap memory is enabled;
   *         false if usage of off-heap memory is disabled (default).
   * @since Geode 1.0
   */
  public boolean getOffHeap();
}



