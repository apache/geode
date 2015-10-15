/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.cache.AbstractRegion;
import com.gemstone.gemfire.internal.cache.CustomEvictionAttributesImpl;
import com.gemstone.gemfire.internal.cache.DiskStoreFactoryImpl;
import com.gemstone.gemfire.internal.cache.DiskWriteAttributesImpl;
import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.UserSpecifiedRegionAttributes;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/** Creates instances of {@link RegionAttributes}. An <code>AttributesFactory</code>
 * instance maintains state for creating <code>RegionAttributes</code> instances.
 * The setter methods are used to change the settings that will be used for
 * creating the next attributes instance with the {@link #create}
 * method. If you create a factory with the default constructor, then the
 * factory is set up to create attributes with all default settings. You can
 * also create a factory by providing a <code>RegionAttributes</code>, which
 * will set up the new factory with the settings provided in that attributes
 * instance.
 *
 * <p>Once a <code>RegionAttributes</code> is created, it can only be modified
 * after it has been used to create a <code>Region</code>, and then only by
 * using an {@link AttributesMutator} obtained from the region.
 *
 * <h3>Attributes</h3>
 * <h4>Callbacks</h4>
 * <dl>
 * <dt>{@link CacheLoader} [<em>default:</em> <code>null</code>, meaning no loader]</dt>
 *     <dd>User-implemented plug-in for loading data on cache misses.<br>
 *        {@link #setCacheLoader} {@link RegionAttributes#getCacheLoader}
 *        {@link AttributesMutator#setCacheLoader}</dd>
 *
 * <dt>{@link CacheWriter} [<em>default:</em> <code>null</code>, meaning no writer]</dt>
 *     <dd>User-implemented plug-in for intercepting cache modifications, e.g.
 *         for writing to an external data source.<br>
 *         {@link #setCacheWriter} {@link RegionAttributes#getCacheWriter}
 *         {@link AttributesMutator#setCacheWriter}</dd>
 *
 * <dt>{@link CacheListener} [<em>default:</em> <code>null</code>, meaning no listener ]</dt>
 *     <dd>User-implemented plug-in for receiving and handling cache related events.<br>
 *         {@link #addCacheListener} {@link #initCacheListeners}
 *         {@link #initCacheListeners}
 *         {@link RegionAttributes#getCacheListeners}
 *         {@link AttributesMutator#initCacheListeners}
 *         {@link AttributesMutator#addCacheListener}
 *         {@link AttributesMutator#removeCacheListener}</dd>
 * </dl>
 * <h4>Expiration</h4>
 * <dl>
 * <dt>RegionTimeToLive [<em>default:</em> no expiration]</dt>
 * <dd>Expiration configuration for the entire region based on the
 * {@link CacheStatistics#getLastModifiedTime lastModifiedTime}.<br>
 * {@link #setRegionTimeToLive} {@link RegionAttributes#getRegionTimeToLive}
 * {@link AttributesMutator#setRegionTimeToLive}</dd>
 *
 * <dt>RegionIdleTimeout [<em>default:</em> no expiration]</dt>
 * <dd>Expiration configuration for the entire region based on the
 * {@link CacheStatistics#getLastAccessedTime lastAccessedTime}.<br>
 * {@link #setRegionIdleTimeout} {@link RegionAttributes#getRegionIdleTimeout}
 * {@link AttributesMutator#setRegionIdleTimeout}</dd>
 *
 * <dt>EntryTimeToLive [<em>default:</em> no expiration]</dt>
 * <dd>Expiration configuration for individual entries based on the
 * {@link CacheStatistics#getLastModifiedTime lastModifiedTime}.<br>
 * {@link #setEntryTimeToLive} {@link RegionAttributes#getEntryTimeToLive}
 * {@link AttributesMutator#setEntryTimeToLive}</dd>
 *
 * <dt>EntryIdleTimeout [<em>default:</em> no expiration]</dt>
 * <dd>Expiration configuration for individual entries based on the
 * {@link CacheStatistics#getLastAccessedTime lastAccessedTime}.<br>
 * {@link #setEntryIdleTimeout} {@link RegionAttributes#getEntryIdleTimeout}
 * {@link AttributesMutator#setEntryIdleTimeout}</dd>
 * </dl>
 * <h4>Distribution</h4>
 * <dl>
 * <dt>{@link Scope}[<em>default:</em> {@link Scope#DISTRIBUTED_NO_ACK}]
 * </dt>
 * <dd>Properties of distribution for the region, including whether it is
 * distributed at all, whether acknowledgements are required, and whether
 * distributed synchronization is required. <br>
 * {@link #setScope} {@link RegionAttributes#getScope}</dd>
 *
 * <dt>EarlyAck [<em>default:</em> <code>false</code>]</dt>
 *     <dd>Whether or not acks required by <code>Scope.DISTRIBUTED_ACK</code>
 *     are sent after an operation is processed. If <code>true</code>
 *     then remote caches will ACK before processing an operation sent
 *     by the cache that has set earlyAck to <code>true</code>.
 *     Note that this attribute is only meaningful on the cache that
 *     is initiating an operation; it does not matter what it is set to
 *     on the cache that receives the operation.<br>
 *     {@link #setEarlyAck} {@link RegionAttributes#getEarlyAck}</dd>

 * <dt>{@link SubscriptionAttributes} [<em>default:</em> {@link InterestPolicy#DEFAULT}]</dt>
 *     <dd>How will the region in this cache subscribe to other distributed
 *     instances of this region.
 *     <br>
 *     {@link #setSubscriptionAttributes} {@link RegionAttributes#getSubscriptionAttributes}</dd>
 *
 * <dt>EnableAsyncConflation [<em>default:</em> <code>false</code>]</dt>
 *     <dd>Whether or not conflation is enabled for sending
 *     messages to async peers. Async peers are those whose
 *     <code>async-distribution-timeout</code> gemfire.property is greater
 *     than zero. AsyncConflation is ignored if the scope is
 *     <code>DISTRIBUTED_ACK</code> or <code>GLOBAL</code>.
 *     Conflation is only done on entry update operations. It is done
 *     by dropping the earlier update from the message queue.
 *     {@link #setEnableAsyncConflation} {@link RegionAttributes#getEnableAsyncConflation}</dd>
 * <dt>poolName [<em>default:</em> <code>null</code>, meaning no pool]</dt>
 *     <dd>Whether or not this region is a client that is to use
 *     connections from the named pool to communicate with servers.
 *     If <code>null</code>, then it is not a client.
 *     If <code>non-null</code>, then the named pool will be used.
 *     {@link #setPoolName} {@link RegionAttributes#getPoolName}</dd>
 * 
 *
 * <dt>EnableSubscriptionConflation [<em>default:</em> <code>false</code>]</dt>
 *     <dd>Whether or not conflation is enabled for sending
 *     messages from a cache server to its clients. Note: This parameter
 *     is only valid for cache server to client communication. It has no
 *     effect in peer to peer communication.
 *     If <code>true</code>, messages will be conflated before they are
 *     sent from a cache server to its clients. Only the latest value
 *     will be sent.
 *     Note that this attribute is only meaningful in a client server
 *     topology.
 *     {@link #setEnableSubscriptionConflation} {@link RegionAttributes#getEnableSubscriptionConflation}</dd>
 * <dt>Publisher [<em>default:</em> <code>false</code>]</dt>
 *     <dd>Whether or not a region is a publisher. Publishers are regions
 *         that will have distributed write operations done on them.
 *         If a publisher is also a replicate then it will be used
 *         as the preferred source for initializing other replicates.
 *     {@link #setPublisher} {@link RegionAttributes#getPublisher}</dd>
 * <dt>isCloningEnabled [<em>default:</em> <code>false</code>]</dt>
 *     <dd>Whether or not value is cloned before appling <code>Delta</code>s
 *     If <code>false</code>, value will not be cloned
 *     {@link #setCloningEnabled} {@link RegionAttributes#getCloningEnabled()}</dd></dt>
 * </dl>
 * <h4>Storage (see also <a href="package-summary.html#storage">package summary
 * </a>)</h4>
 * <dl>
 * <dt>{@link DataPolicy} [<em>default:</em> <code>DataPolicy.NORMAL</code>]</dt>
 *     <dd>Specifies the data storage policy.<br>
 *         {@link #setDataPolicy} {@link RegionAttributes#getDataPolicy}</dd>
 *
 * <dt>{@link MirrorType} [<em>default:</em> <code>MirrorType.NONE</code>]</dt>
 *     <dd><em>Deprecated</em>, use DataPolicy instead.</dd>
 *
 * <dt>{@link #setEvictionAttributes(EvictionAttributes) EvictionAttributes}</dt>
 *      <dd>{@link EvictionAttributes} are the replacement for the deprecated and removed CapacityController interface.
 *          EvictionAttributes describe the {@link EvictionAlgorithm} and the {@link EvictionAction}
 *          as well as the various conditions under which the algorithm perform the action
 *          e.g. when the maximum number of entries has been reached or
 *          the maximum percentage of JVM heap has been consumed.
 *          Setting <code>EvictionAttributes</code> installs an eviction controller
 *          on the Region instantiated with the associated RegionAttributes </dd>
 *
 * <dt>KeyConstraint [<em>default:</em> <code>null</code>, meaning no constraint]</dt>
 *     <dd>The Class to constrain the keys to in the region.<br>
 *         {@link #setKeyConstraint} {@link RegionAttributes#getKeyConstraint}</dd>
 *
 * <dt>ValueConstraint [<em>default:</em> <code>null</code>, meaning no constraint]</dt>
 *     <dd>The Class to constrain the values to in the region. In addition to the
 *         utility of this for applications in general, a <code>valueConstraint</code>
 *         is helpful for compiling queries.<br>
 *         {@link #setValueConstraint} {@link RegionAttributes#getValueConstraint}</dd>
 *
 * <dt>InitialCapacity [<em>default:</em> <code>16</code>]</dt>
 * <dd>The initial capacity of the map used for storing the entries. <br>
 * {@link java.util.HashMap} {@link #setInitialCapacity}
 * {@link RegionAttributes#getInitialCapacity}</dd>
 *
 * <dt>LoadFactor [<em>default:</em> <code>0.75</code>]</dt>
 * <dd>The load factor of the map used for storing the entries. <br>
 * {@link java.util.HashMap} {@link #setLoadFactor}
 * {@link RegionAttributes#getLoadFactor}</dd>
 *
 * <dt>ConcurrencyLevel [<em>default:</em> <code>16</code>]</dt>
 * <dd>The allowed concurrency among updates to values in the region is guided
 * by the <tt>concurrencyLevel</tt>, which is used as a hint for internal
 * sizing. The actual concurrency will vary. Ideally, you should choose a value
 * to accommodate as many threads as will ever concurrently modify values in the
 * region. Using a significantly higher value than you need can waste space and
 * time, and a significantly lower value can lead to thread contention. But
 * overestimates and underestimates within an order of magnitude do not usually
 * have much noticeable impact. A value of one is appropriate when it is known
 * that only one thread will modify and all others will only read. <br>
 * {@link #setConcurrencyLevel} {@link RegionAttributes#getConcurrencyLevel}
 * </dd>
 * 
 * <dt>ConcurrencyChecksEnabled [<em>default:</em> <code>false</code>]</dt>
 * <dd>Enables a distributed versioning algorithm that detects concurrency
 * conflicts in regions and ensures that changes to an
 * entry are not applied in a different order in other members.  This can
 * cause operations to be conflated, so that some cache listeners may see
 * an event while others do not, but it guarantees that the system will
 * be consistent.
 * </dd>
 *
 * <dt>StatisticsEnabled [<em>default:</em> <code>false</code>]</dt>
 * <dd>Whether statistics are enabled for this region. The default is disabled,
 * which conserves on memory. <br>
 * {@link #setStatisticsEnabled} {@link RegionAttributes#getStatisticsEnabled}
 * </dd>
 *
 * <dt>IgnoreJTA [<em>default:</em> <code>false</code>]</dt>
 *     <dd>Whether JTA transactions are ignored for this region.  The
 *     default is to look for and join JTA transactions for operations
 *     performed on a region.
 *
 * <dt>DiskStoreName [<em>default:</em> <code>null</code>, meaning no disk store]</dt>
 *    <dd>If not <code>null</code> then this region will write its data
 *    to the named {@link DiskStore}.<br>
 *    {@link #setDiskStoreName} {@link RegionAttributes#getDiskStoreName}</dd>
 *
 * <dt>DiskSynchronous [<em>default:</em> <code>true</code>]</dt>
 *    <dd>If <code>true</code> then any writes to disk done for this region
 *    will be done synchronously. This means that they will be in the file system
 *    buffer before the operation doing the write returns.<br>
 *    If <code>false</code> then any writes to disk done for this region
 *    will be done asynchronously. This means that they are queued up to be written
 *    and when they are actually written to the file system buffer is determined
 *    by the region's {@link DiskStore} configuration.
 *    Asynchronous writes will be conflated if the same entry is written while a
 *    previous operation for the same entry is still in the queue.<br>
 *    {@link #setDiskSynchronous} {@link RegionAttributes#isDiskSynchronous}</dd>

 * <dt>PersistBackup [<em>default:</em> <code>false</code>]</dt>
 *     <dd>Whether or not a persistent backup should be made of the
 *     region.<br>
 *     {@link #setPersistBackup} {@link RegionAttributes#getPersistBackup}</dd>
 *     <dd><em>Deprecated</em>, use {@link DataPolicy#PERSISTENT_REPLICATE} or {@link DataPolicy#PERSISTENT_PARTITION} instead.</dd>
 *
 * <dt>DiskWriteAttributes [<em>default:</em> Asynchronously write to
 *            disk every second (a <code>timeInterval</code> of 1000 and a
 *            <code>byteThreshold</codE> of 0). <code>rollOplogs</code> is set to true and
 *            <code>maxOplogSize</code> is set to 1024 MB]</dt>
 *     <dd>How region data should be written to disk.  Determines
 *     whether data should be written synchronously or asynchronously.
 *     Data that is written asynchronously can be written at a certain
 *     {@linkplain DiskWriteAttributes#getTimeInterval time interval}
 *     or once a certain number of {@linkplain
 *     DiskWriteAttributes#getBytesThreshold bytes of data} have been
 *     enqueued.<br>
 *     {@link DiskWriteAttributes} {@link #setDiskWriteAttributes} {@link RegionAttributes#getDiskWriteAttributes}</dd>
 *     <dd><em>Deprecated</em>, use {@link #setDiskStoreName} and {@link #setDiskSynchronous} instead.</dd>
 *
 * <dt>DiskDirs [<em>default:</em> Current working directory (<code>user.dir</code> {@linkplain System#getProperties system property})]</dt>
 *     <dd>The directories to which the region's data are written.  If
 *     multiple directories are used, GemFire will attempt to distribute the
 *     data evenly among them. <br>
 *     {@link #setDiskDirs} {@link RegionAttributes#getDiskDirs}</dd>
 *     <dd><em>Deprecated</em>, use {@link #setDiskStoreName} instead.</dd>
 *
 * <dt>DiskDirSizes [<em>default:</em> 10240 MB]</dt>
 * <dd> The size of the directory to which region's data is written.<br>
 * {@link #setDiskDirsAndSizes} {@link RegionAttributes#getDiskDirSizes}</dd>
 * <dd><em>Deprecated</em>, use {@link #setDiskStoreName} instead.</dd>
 *
 *
 * <dt>{@link PartitionAttributes} [<em>default:</em> <code>null</code>, meaning no region partitioning]</dt>
 *     <dd>How region data is partitioned among the members of the
 *     distributed system.
 *     <br>
 *     {@link #setPartitionAttributes} {@link RegionAttributes#getPartitionAttributes}</dd>
 *
 * <dt>{@link MembershipAttributes} [<em>default:</em> no required roles]</dt>
 *     <dd>How access to the region is affected when one or more required roles
 *     are missing from the region membership.
 *     <br>
 *     {@link #setMembershipAttributes} {@link RegionAttributes#getMembershipAttributes}</dd>
 *
 * </dt>
 * </dl>
 *
 * <h4>Locking</h4>
 * <dl>
 * <dt>LockGrantor [<em>default:</em> <code>false</code>]</dt>
 *     <dd>Should this process become lock grantor for the region?</dd><br>
 *     {@link #setLockGrantor} {@link RegionAttributes#isLockGrantor}
 *     {@link Region#becomeLockGrantor}
 * </dl>
 *
 * <h4>Querying</h4>
 * <dl>
 * <dt>IndexMaintenanceSynchronous [<em>default:</em> <code>false</code>]</dt>
 *     <dd>Are indexes built over in this region updated
 *         synchronously when the underlying data is
 *         modified?</dd><br>
 *     {@link #setIndexMaintenanceSynchronous} {@link
 *     RegionAttributes#getIndexMaintenanceSynchronous}
 * </dl>
 *
 * <p>Note that the RegionAttributes are not distributed with the region.
 *
 * <a name="compatibility"><h3>Compatibility Rules</h3>
 * <h4>RegionAttributes Creation Constraints</h4>
 * If any of the following compatibility rules are violated when
 * {@link #create}</code> is called then an
 * {@link IllegalStateException} is thrown.
 * See {@link #validateAttributes}.
 *
 * <a name="creationConstraints"><h3>Creation Constraints</h3>
 * <h4>Region Creation Constraints on RegionAttributes</h4>
 *
 * If any of the following rules are violated when {@link
 * Region#createSubregion createSubregion} or {@link Cache#createRegion
 * createRegion} are called, then an
 * <code>IllegalStateException</code> is thrown.
 *
 * <ul>
 * <li>A region with <code>Scope.LOCAL</code> can only have subregions with
 * <code>Scope.LOCAL</code>.</li>
 * <li><code>Scope.GLOBAL</code> is illegal if there is any other cache in
 * the distributed system that has the same region with
 * <code>Scope.DISTRIBUTED_NO_ACK</code> or <code>Scope.DISTRIBUTED_ACK</code>.
 * </li>
 * <li><code>Scope.DISTRIBUTED_ACK</code> is illegal if there is any other
 * cache in the distributed system that has the same region with
 * <code>Scope.DISTRIBUTED_NO_ACK</code> or <code>Scope.GLOBAL</code>.
 * </li>
 * <li><code>Scope.DISTRIBUTED_NO_ACK</code> is illegal if there is any other
 * cache in the distributed system that has the same region with
 * <code>Scope.DISTRIBUTED_ACK</code> or <code>Scope.GLOBAL</code>.</li>
 * </ul>
 *
 * @see RegionAttributes
 * @see AttributesMutator
 * @see Region#createSubregion(String, RegionAttributes)
 *
 * @author Eric Zoerner
 * @since 3.0
 * @deprecated as of 6.5 use {@link Cache#createRegionFactory(RegionShortcut)} or {@link ClientCache#createClientRegionFactory(ClientRegionShortcut)} instead.
 */
@SuppressWarnings("synthetic-access")
public class AttributesFactory<K,V> {
  private final RegionAttributesImpl<K,V> regionAttributes = new RegionAttributesImpl<K,V>();

  /**
   * The default disk synchronous write setting
   * <p>Current value: <code>true</code> each.
   * @since 6.5
   */
  public static final boolean DEFAULT_DISK_SYNCHRONOUS = true;
  
  /**
   * Creates a new instance of AttributesFactory ready to create a
   * <code>RegionAttributes</code> with default settings.
   */
  public AttributesFactory() {
  }

  /**
   * Creates a new instance of AttributesFactory ready to create a
   * <code>RegionAttributes</code> with the same settings as those in the
   * specified <code>RegionAttributes</code>.
   *
   * @param regionAttributes
   *          the <code>RegionAttributes</code> used to initialize this
   *          AttributesFactory
   */
  @SuppressWarnings("deprecation")
  public AttributesFactory(RegionAttributes<K,V> regionAttributes) {
    synchronized (this.regionAttributes) {
      this.regionAttributes.cacheListeners = new ArrayList<CacheListener<K,V>>(Arrays.asList(regionAttributes.getCacheListeners()));
    }
    this.regionAttributes.cacheLoader = regionAttributes.getCacheLoader();
    this.regionAttributes.cacheWriter = regionAttributes.getCacheWriter();
    this.regionAttributes.regionTimeToLive = regionAttributes
        .getRegionTimeToLive().getTimeout();
    this.regionAttributes.regionTimeToLiveExpirationAction = regionAttributes
        .getRegionTimeToLive().getAction();
    this.regionAttributes.regionIdleTimeout = regionAttributes
        .getRegionIdleTimeout().getTimeout();
    this.regionAttributes.regionIdleTimeoutExpirationAction = regionAttributes
        .getRegionIdleTimeout().getAction();
    
    this.regionAttributes.entryTimeToLive = regionAttributes
        .getEntryTimeToLive().getTimeout();
    this.regionAttributes.entryTimeToLiveExpirationAction = regionAttributes
        .getEntryTimeToLive().getAction();
    this.regionAttributes.customEntryTimeToLive = regionAttributes
        .getCustomEntryTimeToLive();
    this.regionAttributes.entryIdleTimeout = regionAttributes
        .getEntryIdleTimeout().getTimeout();
    this.regionAttributes.entryIdleTimeoutExpirationAction = regionAttributes
        .getEntryIdleTimeout().getAction();
    this.regionAttributes.customEntryIdleTimeout = regionAttributes
        .getCustomEntryIdleTimeout();
    
    this.regionAttributes.scope = regionAttributes.getScope();
    this.regionAttributes.dataPolicy = regionAttributes.getDataPolicy();
    this.regionAttributes.statisticsEnabled = regionAttributes.getStatisticsEnabled();
    this.regionAttributes.ignoreJTA = regionAttributes.getIgnoreJTA();
    this.regionAttributes.keyConstraint = regionAttributes.getKeyConstraint();
    this.regionAttributes.valueConstraint = regionAttributes
        .getValueConstraint();
    this.regionAttributes.initialCapacity = regionAttributes
        .getInitialCapacity();
    this.regionAttributes.loadFactor = regionAttributes.getLoadFactor();
    this.regionAttributes.concurrencyLevel = regionAttributes
        .getConcurrencyLevel();
    this.regionAttributes.concurrencyChecksEnabled = regionAttributes.getConcurrencyChecksEnabled();
    this.regionAttributes.earlyAck = regionAttributes.getEarlyAck();
    this.regionAttributes.diskStoreName = regionAttributes.getDiskStoreName();
    if (this.regionAttributes.diskStoreName == null) {
      this.regionAttributes.diskWriteAttributes = regionAttributes
      .getDiskWriteAttributes();
      this.regionAttributes.diskDirs = regionAttributes.getDiskDirs();
      this.regionAttributes.diskSizes = regionAttributes.getDiskDirSizes();
    }
    this.regionAttributes.diskSynchronous = regionAttributes.isDiskSynchronous();
    this.regionAttributes.indexMaintenanceSynchronous = regionAttributes
        .getIndexMaintenanceSynchronous();
    this.regionAttributes.partitionAttributes = regionAttributes
        .getPartitionAttributes();
    this.regionAttributes.evictionAttributes = (EvictionAttributesImpl)regionAttributes
        .getEvictionAttributes();
    this.regionAttributes.customEvictionAttributes = regionAttributes
        .getCustomEvictionAttributes();

    this.regionAttributes.membershipAttributes = regionAttributes.getMembershipAttributes();
    this.regionAttributes.subscriptionAttributes = regionAttributes.getSubscriptionAttributes();
    this.regionAttributes.evictionAttributes = (EvictionAttributesImpl) regionAttributes.getEvictionAttributes();

    this.regionAttributes.publisher = regionAttributes.getPublisher();
    this.regionAttributes.enableAsyncConflation = regionAttributes.getEnableAsyncConflation();
    this.regionAttributes.enableSubscriptionConflation = regionAttributes.getEnableSubscriptionConflation();
    this.regionAttributes.poolName = regionAttributes.getPoolName();
    this.regionAttributes.isCloningEnabled = regionAttributes.getCloningEnabled();
    this.regionAttributes.multicastEnabled = regionAttributes.getMulticastEnabled();
    this.regionAttributes.gatewaySenderIds = new CopyOnWriteArraySet<String>(regionAttributes.getGatewaySenderIds());
    this.regionAttributes.asyncEventQueueIds = new CopyOnWriteArraySet<String>(regionAttributes.getAsyncEventQueueIds());
    this.regionAttributes.hdfsStoreName = regionAttributes.getHDFSStoreName();
    this.regionAttributes.isLockGrantor = regionAttributes.isLockGrantor(); // fix for bug 47067
    if (regionAttributes instanceof UserSpecifiedRegionAttributes) {
      this.regionAttributes.setIndexes(((UserSpecifiedRegionAttributes<K,V>) regionAttributes).getIndexes());
    }

    if (regionAttributes instanceof UserSpecifiedRegionAttributes) {
      // Selectively set has* fields to true, propigating those non-default 
      // (aka user specified) fields as such
      UserSpecifiedRegionAttributes<K,V> nonDefault = (UserSpecifiedRegionAttributes<K,V>) regionAttributes;
      this.regionAttributes.initHasFields(nonDefault);
      this.regionAttributes.requiresPoolName = nonDefault.requiresPoolName;
    } else {
      // Set all fields to false, essentially starting with a new set of defaults
      this.regionAttributes.setAllHasFields(false);
      
      
      
//      
//      // Special Partitioned Region handling by
//      // pretending the user didn't explicitly ask for the default scope
//      if (AbstractRegion.DEFAULT_SCOPE.equals(this.regionAttributes.getScope())) {
//        this.regionAttributes.setHasScope(false); 
//      }
    }
    
    this.regionAttributes.compressor = regionAttributes.getCompressor();
    this.regionAttributes.hdfsWriteOnly = regionAttributes.getHDFSWriteOnly();
    if (regionAttributes instanceof UserSpecifiedRegionAttributes) {
      this.regionAttributes.setHasHDFSWriteOnly(((UserSpecifiedRegionAttributes<K,V>) regionAttributes).hasHDFSWriteOnly());
    }
    this.regionAttributes.offHeap = regionAttributes.getOffHeap();
  }

  // CALLBACKS

  /**
   * Sets the cache loader for the next <code>RegionAttributes</code> created.
   *
   * @param cacheLoader
   *          the cache loader or null if no loader
   * @throws IllegalStateException if this region has a {@link #setPoolName pool name set}
   */
  public void setCacheLoader(CacheLoader<K,V> cacheLoader)
  {
    this.regionAttributes.cacheLoader = cacheLoader;
    this.regionAttributes.setHasCacheLoader(true);
  }

  /**
   * Sets the cache writer for the next <code>RegionAttributes</code> created.
   *
   * @param cacheWriter
   *          the cache writer or null if no cache writer
   * @throws IllegalStateException if this region has a {@link #setPoolName pool name set}
   */
  public void setCacheWriter(CacheWriter<K,V> cacheWriter)
  {
    this.regionAttributes.cacheWriter = cacheWriter;
    this.regionAttributes.setHasCacheWriter(true);
  }

  /** Sets the CacheListener for the next <code>RegionAttributes</code> created.
   * Any existing cache listeners on this factory are removed.
   * @param aListener a user defined CacheListener, null if no listener
   * @deprecated as of GemFire 5.0, use {@link #addCacheListener} instead.
   */
  @Deprecated
  public void setCacheListener(CacheListener<K,V> aListener) {
    ArrayList<CacheListener<K,V>> col;
    if (aListener == null) {
      col = null;
    } else {
      col = new ArrayList<CacheListener<K,V>>(1);
      col.add(aListener);
    }
    synchronized (this.regionAttributes) {
      this.regionAttributes.cacheListeners = col;
    }
    this.regionAttributes.setHasCacheListeners(true);
  }
  /**
   * Adds a cache listener to the end of the list of cache listeners on this factory.
   * @param aListener the cache listener to add to the factory.
   * @throws IllegalArgumentException if <code>aListener</code> is null
   * @since 5.0
   */
  public void addCacheListener(CacheListener<K,V> aListener) {
    if (aListener == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_ADDCACHELISTENER_PARAMETER_WAS_NULL.toLocalizedString());
    }
    synchronized (this.regionAttributes) {
      this.regionAttributes.addCacheListener(aListener);
    }
  }
  /**
   * Removes all cache listeners and then adds each listener in the specified array.
   * @param newListeners a possibly null or empty array of listeners to add to this factory.
   * @throws IllegalArgumentException if the <code>newListeners</code> array has a null element
   * @since 5.0
   */
  public void initCacheListeners(CacheListener<K,V>[] newListeners) {
    synchronized (this.regionAttributes) {
      if (newListeners == null || newListeners.length == 0) {
        this.regionAttributes.cacheListeners = null;
      } else {
        List<CacheListener<K,V>> nl = Arrays.asList(newListeners);
        if (nl.contains(null)) {
          throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_INITCACHELISTENERS_PARAMETER_HAD_A_NULL_ELEMENT.toLocalizedString());
        }
        this.regionAttributes.cacheListeners = new ArrayList<CacheListener<K,V>>(nl);
      }
    }
    this.regionAttributes.setHasCacheListeners(true);
  }


  // EXPIRATION ATTRIBUTES

  /**
   * Sets the idleTimeout expiration attributes for region entries for the next
   * <code>RegionAttributes</code> created.
   * Default is 0 which indicates no expiration of this type.
   *
   * @param idleTimeout
   *          the idleTimeout ExpirationAttributes for entries in this region
   * @throws IllegalArgumentException
   *           if idleTimeout is null
   */
  public void setEntryIdleTimeout(ExpirationAttributes idleTimeout)
  {
    if (idleTimeout == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_IDLETIMEOUT_MUST_NOT_BE_NULL.toLocalizedString());
    }
    this.regionAttributes.entryIdleTimeout = idleTimeout.getTimeout();
    this.regionAttributes.entryIdleTimeoutExpirationAction = idleTimeout
        .getAction();
    this.regionAttributes.setHasEntryIdleTimeout(true);
  }

  /**
   * Sets the idleTimeout CustomExpiry for the next <code>RegionAttributes</code>
   * created.
   * 
   * @param custom the CustomExpiry to use; null means none will be used.
   */
  public void setCustomEntryIdleTimeout(CustomExpiry<K,V> custom) {
    this.regionAttributes.customEntryIdleTimeout = custom;
    this.regionAttributes.setHasCustomEntryIdleTimeout(true);
  }
  
  /**
   * Sets the timeToLive expiration attributes for region entries for the next
   * <code>RegionAttributes</code> created.
   * Default is 0 which indicates no expiration of this type.
   *
   * @param timeToLive
   *          the timeToLive ExpirationAttributes for entries in this region
   * @throws IllegalArgumentException
   *           if timeToLive is null
   */
  public void setEntryTimeToLive(ExpirationAttributes timeToLive)
  {
    if (timeToLive == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_TIMETOLIVE_MUST_NOT_BE_NULL.toLocalizedString());
    }
    this.regionAttributes.entryTimeToLive = timeToLive.getTimeout();
    this.regionAttributes.entryTimeToLiveExpirationAction = timeToLive
        .getAction();
    this.regionAttributes.setHasEntryTimeToLive(true);
  }

  /**
   * Sets the custom timeToLive for the next <code>RegionAttributes</code>
   * created.
   * 
   * @param custom the CustomExpiry to use, none if the default for the region
   * is to be used.
   */
  public void setCustomEntryTimeToLive(CustomExpiry<K,V> custom) {
    this.regionAttributes.customEntryTimeToLive = custom;
    this.regionAttributes.setHasCustomEntryTimeToLive(true);
  }
  
  /**
   * Sets the idleTimeout expiration attributes for the region itself for the
   * next <code>RegionAttributes</code> created.
   * Default is 0 which indicates no expiration of this type is set. 
   *
   * @param idleTimeout
   *          the ExpirationAttributes for this region idleTimeout
   * @throws IllegalArgumentException
   *           if idleTimeout is null
   */
  public void setRegionIdleTimeout(ExpirationAttributes idleTimeout)
  {
    if (idleTimeout == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_IDLETIMEOUT_MUST_NOT_BE_NULL.toLocalizedString());
    }
    this.regionAttributes.regionIdleTimeout = idleTimeout.getTimeout();
    this.regionAttributes.regionIdleTimeoutExpirationAction = idleTimeout
        .getAction();
    this.regionAttributes.setHasRegionIdleTimeout(true);
  }
  

  /**
   * Sets the timeToLive expiration attributes for the region itself for the
   * next <code>RegionAttributes</code> created.
   * Default is 0 i.e. no expiration of this type.
   *
   * @param timeToLive
   *          the ExpirationAttributes for this region timeToLive
   * @throws IllegalArgumentException
   *           if timeToLive is null
   */
  public void setRegionTimeToLive(ExpirationAttributes timeToLive)
  {
    if (timeToLive == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_TIMETOLIVE_MUST_NOT_BE_NULL.toLocalizedString());
    }
    this.regionAttributes.regionTimeToLive = timeToLive.getTimeout();
    this.regionAttributes.regionTimeToLiveExpirationAction = timeToLive
        .getAction();
    this.regionAttributes.setHasRegionTimeToLive(true);
  }

  // DISTRIBUTION ATTRIBUTES

  /**
   * Sets the scope for the next <code>RegionAttributes</code> created.
   * Default scope is DISTRIBUTED_NO_ACK. Refer gemfire documentation for more details on this.
   * @param scopeType
   *          the type of Scope to use for the region
   * @throws IllegalArgumentException
   *           if scopeType is null
   */
  public void setScope(Scope scopeType)
  {
    if (scopeType == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_SCOPETYPE_MUST_NOT_BE_NULL.toLocalizedString());
    }
    this.regionAttributes.setScope(scopeType);
  }

  // STORAGE ATTRIBUTES

  /**
   * Sets the EvictionController for the next <code>RegionAttributes</code>
   * created. Use one of the creation methods on {@link EvictionAttributes} e.g.
   * {@link EvictionAttributes#createLRUHeapAttributes()} to create the desired
   * instance for this <code>AttributesFactory</code>
   *
   * @param evictAttrs
   *          Explains how and when eviction occurs in the Region.
   */
   public void setEvictionAttributes(EvictionAttributes evictAttrs) {
     if (evictAttrs != null) {
       this.regionAttributes.evictionAttributes = (EvictionAttributesImpl) evictAttrs;
     } else {
       this.regionAttributes.evictionAttributes = new EvictionAttributesImpl();
     }
     this.regionAttributes.setHasEvictionAttributes(true);
   }

  /**
   * Set custom {@link EvictionCriteria} for the region with start time and
   * frequency of evictor task to be run in milliseconds, or evict incoming rows
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
  public void setCustomEvictionAttributes(EvictionCriteria<K, V> criteria,
      long start, long interval) {
    this.regionAttributes.customEvictionAttributes =
        new CustomEvictionAttributesImpl(criteria, start, interval,
            start == 0 && interval == 0);
    this.regionAttributes.setHasCustomEviction(true);
  }

   /** Sets the mirror type for the next <code>RegionAttributes</code> created.
   * @param mirrorType The type of mirroring to use for the region
   * @throws IllegalArgumentException if mirrorType is null
   * @deprecated use {@link #setDataPolicy} instead.
   */
  @Deprecated
  public void setMirrorType(MirrorType mirrorType) {
    if (mirrorType == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_MIRRORTYPE_MUST_NOT_BE_NULL.toLocalizedString());
    }
    DataPolicy dp = mirrorType.getDataPolicy();
    if (dp.withReplication()) {
      // requested a mirror type that has replication
      // if current data policy is not replicated change it
      if (!this.regionAttributes.getDataPolicy().withReplication()) {
        setDataPolicy(dp);
      }
    } else {
      // requested a mirror type none;
      // if current data policy is replicated change it
      if (this.regionAttributes.getDataPolicy().withReplication()) {
        setDataPolicy(dp);
      }
    }
  }
  /** Sets the data policy for the next <code>RegionAttributes</code> created.
   * Default data policy is 'Normal'. Please refer gemfire documentation for more details on this.
   * @param dataPolicy The data policy to use for the region
   * @throws IllegalArgumentException if dataPolicy is null
   */
  public void setDataPolicy(DataPolicy dataPolicy) {
    if (dataPolicy == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_DATAPOLICY_MUST_NOT_BE_NULL.toLocalizedString());
    }
    if (this.regionAttributes.partitionAttributes != null) {
      if ( !PartitionedRegionHelper.ALLOWED_DATA_POLICIES.contains(dataPolicy) ) {
        throw new IllegalStateException( LocalizedStrings.AttributesFactory_DATA_POLICIES_OTHER_THAN_0_ARE_NOT_SUPPORTED_FOR_PARTITIONED_REGIONS
            .toLocalizedString(PartitionedRegionHelper.ALLOWED_DATA_POLICIES));
      }
    }
    this.regionAttributes.setDataPolicy(dataPolicy);
  }



  /** Sets the key constraint for the next <code>RegionAttributes</code> created.
   * Keys in the region will be constrained to this class (or subclass).
   * Any attempt to store a key of an incompatible type in the region will
   * cause a <code>ClassCastException</code> to be thrown.
   * @param keyConstraint The Class to constrain the keys to, or null if no constraint
   * @throws IllegalArgumentException if <code>keyConstraint</code> is a class
   * denoting a primitive type
   */
  public void setKeyConstraint(Class<K> keyConstraint) {
    if (keyConstraint != null && keyConstraint.isPrimitive())
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_KEYCONSTRAINT_MUST_NOT_BE_A_PRIMITIVE_TYPE.toLocalizedString());
    this.regionAttributes.keyConstraint = keyConstraint;
    this.regionAttributes.setHasKeyConstraint(true);
  }

  /** Sets the value constraint for the next <code>RegionAttributes</code> created.
   * Values in the region will be constrained to this class (or subclass).
   * Any attempt to store a value of an incompatible type in the region will
   * cause a <code>ClassCastException</code> to be thrown.
   * @param valueConstraint The Class to constrain the values to, or null if no constraint
   * @throws IllegalArgumentException if <code>valueConstraint</code> is a class
   * denoting a primitive type
   */
  public void setValueConstraint(Class<V> valueConstraint) {
    if (valueConstraint != null && valueConstraint.isPrimitive())
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_VALUECONSTRAINT_MUST_NOT_BE_A_PRIMITIVE_TYPE.toLocalizedString());
    this.regionAttributes.valueConstraint = valueConstraint;
    this.regionAttributes.setHasValueConstraint(true);
  }



  // MAP ATTRIBUTES
  /** Sets the entry initial capacity for the next <code>RegionAttributes</code>
   * created. This value
   * is used in initializing the map that holds the entries.
   * Default is 16.
   * @param initialCapacity the initial capacity of the entry map
   * @throws IllegalArgumentException if initialCapacity is negative.
   * @see java.util.HashMap
   */
  public void setInitialCapacity(int initialCapacity) {
    if (initialCapacity < 0)
        throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_INITIALCAPACITY_MUST_BE_0.toLocalizedString());
    this.regionAttributes.initialCapacity = initialCapacity;
    this.regionAttributes.setHasInitialCapacity(true);
  }

  /** Sets the entry load factor for the next <code>RegionAttributes</code>
   * created. This value is
   * used in initializing the map that holds the entries.
   * Default is 0.75.
   * @param loadFactor the load factor of the entry map
   * @throws IllegalArgumentException if loadFactor is nonpositive
   * @see java.util.HashMap
   */
  public void setLoadFactor(float loadFactor) {
    if (loadFactor <= 0)
        throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_LOADFACTOR_MUST_BE_0_VALUE_IS_0.toLocalizedString(new Float(loadFactor)));
    this.regionAttributes.loadFactor = loadFactor;
    this.regionAttributes.setHasLoadFactor(true);
  }

  /** Sets the concurrency level of the next <code>RegionAttributes</code>
   * created. This value is used in initializing the map that holds the entries.
   * Default is 16.
   * @param concurrencyLevel the concurrency level of the entry map
   * @throws IllegalArgumentException if concurrencyLevel is nonpositive
   */
  public void setConcurrencyLevel(int concurrencyLevel) {
    if (concurrencyLevel <= 0)
        throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_CONCURRENCYLEVEL_MUST_BE_0.toLocalizedString());
    this.regionAttributes.concurrencyLevel = concurrencyLevel;
    this.regionAttributes.setHasConcurrencyLevel(true);
  }
  
  /**
   * Enables or disabled concurrent modification checks.  Concurrency checks are enabled
   * by default.
   * @since 7.0
   * @param concurrencyChecksEnabled whether to perform concurrency checks on operations
   */
  public void setConcurrencyChecksEnabled(boolean concurrencyChecksEnabled) {
    this.regionAttributes.concurrencyChecksEnabled = concurrencyChecksEnabled;
    this.regionAttributes.setHasConcurrencyChecksEnabled(true);
  }

  /**
   * Sets whether or not a persistent backup should be made of the
   * region.
   *
   * @since 3.2
   * @deprecated as of GemFire 5.0, use {@link DataPolicy#PERSISTENT_REPLICATE} instead
   */
  @Deprecated
  public void setPersistBackup(boolean persistBackup) {
    if (persistBackup) {
      if (!this.regionAttributes.getDataPolicy().withPersistence()) {
        if (this.regionAttributes.getDataPolicy().withPartitioning()) {
          setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        } else {
          setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        }
      }
    } else {
      // It is less clear what we should do here for backwards compat.
      // If the current data policy is persist then we need to change it
      // otherwise just leave it alone
      if (this.regionAttributes.getDataPolicy().withReplication()) {
        setDataPolicy(DataPolicy.REPLICATE);
      } else if (this.regionAttributes.getDataPolicy().withPartitioning()) {
        setDataPolicy(DataPolicy.PARTITION);
      }
    }
  }
  /**
   * Sets whether or not acks are sent after an operation is processed.
   *
   * @since 4.1
   * @deprecated This setting no longer has any effect. 
   */
  @Deprecated
  public void setEarlyAck(boolean earlyAck) {
    this.regionAttributes.earlyAck = earlyAck;
    this.regionAttributes.setHasEarlyAck(true);
  }
  
  /**
   * Sets whether or not this region should be considered a publisher.
   *
   * @since 4.2.3
   * @deprecated as of 6.5
   */
  @Deprecated
  public void setPublisher(boolean v) {
//    this.regionAttributes.publisher = v;
//    this.regionAttributes.setHasPublisher(true);
  }

  /**
   * Sets whether or not conflation is enabled for sending messages
   * to async peers.
   * Default value is false.
   *
   * @since 4.2.3
   */
  public void setEnableAsyncConflation(boolean enableAsyncConflation) {
    this.regionAttributes.enableAsyncConflation = enableAsyncConflation;
    this.regionAttributes.setHasEnableAsyncConflation(true);
  }
  

  /**
   * Sets whether or not conflation is enabled for sending messages
   * from a cache server to its clients.
   * Default is false.
   *
   * @since 5.0
   */
  public void setEnableSubscriptionConflation(boolean enableSubscriptionConflation) {
    this.regionAttributes.enableSubscriptionConflation = enableSubscriptionConflation;
    this.regionAttributes.setHasEnableSubscriptionConflation(true);
  }

  /**
   * adds a gateway sender to the end of list of gateway senders on this factory
   * @param gatewaySenderId
   * @throws IllegalArgumentException if <code>gatewaySender</code> is null
   * @since 7.0
   */
  public void addGatewaySenderId(String gatewaySenderId) {
    if (gatewaySenderId == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_GATEWAY_SENDER_ID_IS_NULL.toLocalizedString());
    }
    synchronized (this.regionAttributes) {
      this.regionAttributes.addGatewaySenderId(gatewaySenderId);
    }
  }
  
  /**
   * Adds a AsyncEventQueue to the end of list of async event queues on this factory
   * @param asyncEventQueueId
   * @throws IllegalArgumentException if <code>gatewaySender</code> is null
   * @since 7.0
   */
  public void addAsyncEventQueueId(String asyncEventQueueId) {
    if (asyncEventQueueId == null) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_GATEWAY_SENDER_ID_IS_NULL.toLocalizedString());
    }
    synchronized (this.regionAttributes) {
      this.regionAttributes.addAsyncEventQueueId(asyncEventQueueId);
    }
  }
  
  /**
   * Sets whether or not conflation is enabled for sending messages
   * from a cache server to its clients.
   *
   * @since 5.0
   * @deprecated as of 5.7 use {@link #setEnableSubscriptionConflation} instead.
   */
  @Deprecated
  public void setEnableBridgeConflation(boolean enableBridgeConflation) {
    setEnableSubscriptionConflation(enableBridgeConflation);
  }

  /**
   * Sets whether or not conflation is enabled for sending messages
   * from a cache server to its clients.
   *
   * @deprecated as of GemFire 5.0, use {@link #setEnableSubscriptionConflation}
  */
  @Deprecated
  public void setEnableConflation(boolean enableBridgeConflation) {
    setEnableSubscriptionConflation(enableBridgeConflation);
  }

  /**
   * Returns whether or not disk writes are asynchronous.
   *
   * @see Region#writeToDisk
   *
   * @since 3.2
   * @deprecated as of 6.5 use {@link #setDiskStoreName} instead
   */
  @Deprecated
  public void setDiskWriteAttributes(DiskWriteAttributes attrs) {
    if (this.regionAttributes.getDiskStoreName() != null) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setDiskWriteAttributes", this.regionAttributes.getDiskStoreName()}));
    }
    this.regionAttributes.diskWriteAttributes = attrs;
    this.regionAttributes.setHasDiskWriteAttributes(true);
    if (attrs != null) {
      // keep new apis in sync with old
      this.regionAttributes.diskSynchronous = attrs.isSynchronous();
    }
  }

  /**
   * Sets the directories with
   * the default size of 10240 MB to which the region's data is written
   *
   * @throws GemFireIOException if a directory does not exist
   *
   * @since 3.2
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setDiskDirs} instead
   */
  @Deprecated
  public void setDiskDirs(File[] diskDirs) {
    if (this.regionAttributes.getDiskStoreName() != null) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setDiskDirs", this.regionAttributes.getDiskStoreName()}));
    }
    DiskStoreFactoryImpl.checkIfDirectoriesExist(diskDirs);
    this.regionAttributes.diskDirs = diskDirs;
    this.regionAttributes.diskSizes = new int[diskDirs.length];
    for (int i=0; i < diskDirs.length; i++) {
      this.regionAttributes.diskSizes[i] = DiskStoreFactory.DEFAULT_DISK_DIR_SIZE;
    }
    if (!this.regionAttributes.hasDiskWriteAttributes()
        && !this.regionAttributes.hasDiskSynchronous()) {
      // switch to the old default
      this.regionAttributes.diskSynchronous = false;
      this.regionAttributes.diskWriteAttributes = DiskWriteAttributesImpl.getDefaultAsyncInstance();
    }
    this.regionAttributes.setHasDiskDirs(true);
  }

  /**
   * Sets the DiskStore name attribute.
   * This causes the region to use the {@link DiskStore}.
   * @param name the name of the diskstore
   * @since 6.5 
   */
  public void setDiskStoreName(String name) {
    if (this.regionAttributes.hasDiskDirs() ||  this.regionAttributes.hasDiskWriteAttributes()) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setDiskDirs or setDiskWriteAttributes", name}));
    }
    this.regionAttributes.diskStoreName = name;
    this.regionAttributes.setHasDiskStoreName(true);
  }
  
  /**
   * Sets whether or not the writing to the disk is synchronous.
   * Default is true.
   * 
   * @param isSynchronous
   *          boolean if true indicates synchronous writes
   * @since 6.5 
   */
  @SuppressWarnings("deprecation")
  public void setDiskSynchronous(boolean isSynchronous)
  {
    this.regionAttributes.diskSynchronous = isSynchronous;
    this.regionAttributes.setHasDiskSynchronous(true);
    if (this.regionAttributes.hasDiskWriteAttributes()) {
      DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory(this.regionAttributes.diskWriteAttributes);
      dwaf.setSynchronous(isSynchronous);
      this.regionAttributes.diskWriteAttributes = dwaf.create();
    } else {
      if (isSynchronous) {
        this.regionAttributes.diskWriteAttributes = DiskWriteAttributesImpl.getDefaultSyncInstance();
      } else {
        this.regionAttributes.diskWriteAttributes = DiskWriteAttributesImpl.getDefaultAsyncInstance();
      }
    }
  }

  /**
   * Sets the directories to which the region's data is written and also set their sizes in megabytes
   *
   * @throws IllegalArgumentException if a dir does not exist or the length of the size array
   * does not match to the length of the dir array or the given length is not a valid positive number
   *
   * @since 5.1
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setDiskDirsAndSizes} instead
   */
  @Deprecated
  public void setDiskDirsAndSizes(File[] diskDirs,int[] diskSizes) {
    if (this.regionAttributes.getDiskStoreName() != null) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setDiskDirsAndSizes", this.regionAttributes.getDiskStoreName()}));
    }
    DiskStoreFactoryImpl.checkIfDirectoriesExist(diskDirs);
    this.regionAttributes.diskDirs = diskDirs;
    if(diskSizes.length != this.regionAttributes.diskDirs.length) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_NUMBER_OF_DISKSIZES_IS_0_WHICH_IS_NOT_EQUAL_TO_NUMBER_OF_DISK_DIRS_WHICH_IS_1.toLocalizedString(new Object[] {Integer.valueOf(diskSizes.length), Integer.valueOf(diskDirs.length)}));
    }
    DiskStoreFactoryImpl.verifyNonNegativeDirSize(diskSizes);
    this.regionAttributes.diskSizes = diskSizes;
    if (!this.regionAttributes.hasDiskWriteAttributes()
        && !this.regionAttributes.hasDiskSynchronous()) {
      // switch to the old default
      this.regionAttributes.diskSynchronous = false;
      this.regionAttributes.diskWriteAttributes = DiskWriteAttributesImpl.getDefaultAsyncInstance();
    }
    this.regionAttributes.setHasDiskDirs(true);
  }

  /**
   * Sets the <code>PartitionAttributes</code> that describe how the
   * region is partitioned among members of the distributed system.  This
   * also establishes a data policy of {@link DataPolicy#PARTITION PARTITION},
   * if the data policy has not already been set.
   *
   * @since 5.0
   */
  public void setPartitionAttributes(PartitionAttributes partition) {
    if (partition != null) {
      if (! this.regionAttributes.hasDataPolicy()) { 
          this.regionAttributes.dataPolicy = PartitionedRegionHelper.DEFAULT_DATA_POLICY;        
      }
      else if ( !PartitionedRegionHelper.ALLOWED_DATA_POLICIES.contains(this.regionAttributes.dataPolicy) ) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_DATA_POLICY_0_IS_NOT_ALLOWED_FOR_A_PARTITIONED_REGION_DATAPOLICIES_OTHER_THAN_1_ARE_NOT_ALLOWED
            .toLocalizedString(new Object[] {this.regionAttributes.dataPolicy, PartitionedRegionHelper.ALLOWED_DATA_POLICIES}));
      }
      if (this.regionAttributes.hasPartitionAttributes() 
          && this.regionAttributes.partitionAttributes instanceof PartitionAttributesImpl
          && partition instanceof PartitionAttributesImpl) {
        // Make a copy and call merge on it to prevent bug 51616
        PartitionAttributesImpl copy = ((PartitionAttributesImpl) this.regionAttributes.partitionAttributes).copy();
        copy.merge((PartitionAttributesImpl) partition);
        this.regionAttributes.partitionAttributes = copy;
      } else {
        this.regionAttributes.partitionAttributes = partition;
        this.regionAttributes.setHasPartitionAttributes(true);
      }
      
      ((PartitionAttributesImpl) this.regionAttributes.partitionAttributes).setOffHeap(this.regionAttributes.offHeap);
    }
    else {
      this.regionAttributes.partitionAttributes = null;
      this.regionAttributes.setHasPartitionAttributes(false);
    }
  }

  protected void setBucketRegion(boolean b) {
    this.regionAttributes.isBucketRegion = b;
  }   
   
  /**
   * Sets the <code>MembershipAttributes</code> that describe the membership
   * roles required for reliable access to the region.
   *
   * @since 5.0
   */
  public void setMembershipAttributes(MembershipAttributes membership) {
    this.regionAttributes.membershipAttributes = membership;
    this.regionAttributes.setHasMembershipAttributes(true);
  }

  /**
   * Sets the <code>SubscriptionAttributes</code> that describe how the region
   * will subscribe to other distributed cache instances of the region.
   *
   * @since 5.0
   */
  public void setSubscriptionAttributes(SubscriptionAttributes subscription) {
    this.regionAttributes.subscriptionAttributes = subscription;
    this.regionAttributes.setHasSubscriptionAttributes(true);
  }

  /**
   * Set how indexes on the region should be maintained. It will be either synchronous
   * or asynchronous.
   * Default is true.
   */
  public void setIndexMaintenanceSynchronous(boolean synchronous) {
    this.regionAttributes.indexMaintenanceSynchronous = synchronous;
    this.regionAttributes.setHasIndexMaintenanceSynchronous(true);
  }

  // STATISTICS
  /** Sets whether statistics are enabled for this region and its entries.
   * Default is false.
   * @param statisticsEnabled whether statistics are enabled
   */
  public void setStatisticsEnabled(boolean statisticsEnabled) {
    this.regionAttributes.statisticsEnabled = statisticsEnabled;
    this.regionAttributes.setHasStatisticsEnabled(true);
  }

  /**
   * Sets the flag telling a region to ignore JTA transactions.
   * Default is false.
   * @since 5.0
   */
  public void setIgnoreJTA(boolean flag) {
    this.regionAttributes.ignoreJTA = flag;
    this.regionAttributes.setHasIgnoreJTA(true);
  }

  /** Sets whether this region should become lock grantor.
   * Default value is false.
   * @param isLockGrantor whether this region should become lock grantor
   */
  public void setLockGrantor(boolean isLockGrantor) {
    this.regionAttributes.isLockGrantor = isLockGrantor;
    this.regionAttributes.setHasIsLockGrantor(true);
  }

  /** Sets whether distributed operations on this region should attempt
      to use multicast.  Multicast must also be enabled in the
      cache's DistributedSystem (see
      <a href=../distributed/DistributedSystem.html#mcast-port">"mcast-port"</a>).
      Default is false.
      @since 5.0
      @see RegionAttributes#getMulticastEnabled
   */
  public void setMulticastEnabled(boolean value) {
    this.regionAttributes.multicastEnabled = value;
    this.regionAttributes.setHasMulticastEnabled(true);
  }  
  /**
   * Sets cloning on region.
   * Default is false.
   * 
   * @param cloningEnable
   * @since 6.1
   * @see RegionAttributes#getCloningEnabled()
   */
  public void setCloningEnabled(boolean cloningEnable) {
    this.regionAttributes.isCloningEnabled = cloningEnable;
    this.regionAttributes.setHasCloningEnabled(true);
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
   * @param name the name of the connection pool to use; if <code>null</code>
   * or <code>""</code> then the connection pool is disabled for regions
   * using these attributes.
   * @since 5.7
   */
  public void setPoolName(String name) {
    String nm = name;
    if ("".equals(nm)) {
      nm = null;
    }
    this.regionAttributes.poolName = nm;
    this.regionAttributes.setHasPoolName(true);
    
  }
  
  /**
   * Sets the HDFSStore name attribute.
   * This causes the region to use the {@link HDFSStore}.
   * @param name the name of the HDFSstore
   */
  public void setHDFSStoreName(String name) {
    //TODO:HDFS throw an exception if the region is already configured for a disk store and 
    // vice versa
    this.regionAttributes.hdfsStoreName = name;
    this.regionAttributes.setHasHDFSStoreName(true);
  }
  
  /**
   * Sets the HDFS write only attribute. if the region
   * is configured to be write only to HDFS, events that have 
   * been evicted from memory cannot be read back from HDFS.
   * Events are written to HDFS in the order in which they occurred.
   */
  public void setHDFSWriteOnly(boolean writeOnly) {
    //TODO:HDFS throw an exception if the region is already configured for a disk store and 
    // vice versa
    this.regionAttributes.hdfsWriteOnly = writeOnly;
    this.regionAttributes.setHasHDFSWriteOnly(true);
  }
  
  /**
   * Sets this region's compressor for compressing entry values.
   * @since 8.0
   * @param compressor a compressor.
   */
  public void setCompressor(Compressor compressor) {
    this.regionAttributes.compressor = compressor;
    this.regionAttributes.setHasCompressor(true);
    
    // Cloning must be enabled when a compressor is set
    if (compressor != null) {
      setCloningEnabled(true);
    }
  }

  /**
   * Enables this region's usage of off-heap memory if true.
   * @since 9.0
   * @param offHeap boolean flag to enable off-heap memory
   */
  public void setOffHeap(boolean offHeap) {
    this.regionAttributes.offHeap = offHeap;
    this.regionAttributes.setHasOffHeap(true);
    
    if (this.regionAttributes.partitionAttributes != null) {
      ((PartitionAttributesImpl) this.regionAttributes.partitionAttributes).setOffHeap(offHeap);
    }
  }
  
  // FACTORY METHOD

  /** Creates a <code>RegionAttributes</code> with the current settings.
   * @return the newly created <code>RegionAttributes</code>
   * @throws IllegalStateException if the current settings violate the
   * <a href="#compatibility">compatibility rules</a>
   * @deprecated as of GemFire 5.0, use {@link #create} instead
   */
  @Deprecated
  public RegionAttributes<K,V> createRegionAttributes() {
    return create();
  }
  /** Creates a <code>RegionAttributes</code> with the current settings.
   * @return the newly created <code>RegionAttributes</code>
   * @throws IllegalStateException if the current settings violate the
   * <a href="#compatibility">compatibility rules</a>
   * @since 5.0
   */
  @SuppressWarnings("unchecked")
  public RegionAttributes<K,V> create() {
    if (this.regionAttributes.hasDataPolicy() &&
        this.regionAttributes.dataPolicy.withPartitioning() &&
        this.regionAttributes.partitionAttributes == null) {
      this.regionAttributes.partitionAttributes = (new PartitionAttributesFactory()).create();
      // fix bug #52033 by invoking setOffHeap now (localMaxMemory may now be the temporary placeholder for off-heap until DistributedSystem is created
      ((PartitionAttributesImpl)this.regionAttributes.partitionAttributes).setOffHeap(this.regionAttributes.getOffHeap());
    }
    // As of 6.5 we automatically enable stats if expiration is used.
    {
      RegionAttributesImpl attrs = this.regionAttributes;
      if (!attrs.hasStatisticsEnabled() && !attrs.getStatisticsEnabled() &&
          (attrs.getRegionTimeToLive().getTimeout() != 0 ||
           attrs.getRegionIdleTimeout().getTimeout() != 0 ||
           attrs.getEntryTimeToLive().getTimeout() != 0 ||
           attrs.getEntryIdleTimeout().getTimeout() != 0 ||
           attrs.getCustomEntryIdleTimeout() != null ||
           attrs.getCustomEntryTimeToLive() != null)
          ) {
        // @todo we could do some more implementation work so that we would
        // not need to enable stats unless entryIdleTimeout is enabled.
        // We need the stats in that case because we need a new type of RegionEntry
        // so we know that last time it was accessed. But for all the others we
        // the stat less region keeps track of everything we need.
        // The only problem is that some places in the code are conditionalized
        // on statisticsEnabled.
        setStatisticsEnabled(true);
      }
      // SQLFabric does not handle PRELOADED, so do not change the policy
      if (attrs.getDataPolicy().withReplication()
          && !attrs.getDataPolicy().withPersistence()
          && attrs.getScope().isDistributed()
          && !GemFireCacheImpl.sqlfSystem()) {
        RegionAttributesImpl<?,?> rattr = attrs;
        if (!rattr.isForBucketRegion()) {
          if (attrs.getEvictionAttributes().getAction().isLocalDestroy()
              || attrs.getEntryIdleTimeout().getAction().isLocal()
              || attrs.getEntryTimeToLive().getAction().isLocal()
              || attrs.getRegionIdleTimeout().getAction().isLocalInvalidate()
              || attrs.getRegionTimeToLive().getAction().isLocalInvalidate()) {
            // new to 6.5; switch to PRELOADED and interest ALL
            setDataPolicy(DataPolicy.PRELOADED);
            setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
          }
        }
      }
    }
    validateAttributes(this.regionAttributes);
    return (RegionAttributes<K,V>)this.regionAttributes.clone();
  }

  /**
   * Validates that the attributes are consistent with each other.
   * The following rules are checked and enforced:
   <ul>
   <li>If the data policy {@link DataPolicy#withReplication uses replication}
       and the scope is {@link Scope#isDistributed distributed} then the
       following are incompatible:
      <ul>
      <li>ExpirationAction.LOCAL_INVALIDATE on the region</li
      <li>ExpirationAction.LOCAL_DESTROY on the entries</li>
      <li>ExpirationAction.LOCAL_INVALIDATE on the entries</li>
      <li>An LRU with local destroy eviction action</li>
      </ul>
   </li>
   <li>Region or entry expiration
      is incompatible with disabled statistics on the region</li>
   <li>Entry expiration
      is incompatible with the {@link DataPolicy#EMPTY} data policy</li>
   <li>{@link EvictionAttributes Eviction}
      is incompatible with the {@link DataPolicy#EMPTY} data policy</li>
   </ul>
   * @param attrs the attributes to validate
   * @throws IllegalStateException if the attributes are not consistent with each other.
   * @since 3.5
   */
  public static void validateAttributes(RegionAttributes<?, ?> attrs) {
    // enforce the creation constraints

    if (attrs.getDataPolicy().withReplication()
        && attrs.getScope().isDistributed()) {
      boolean isForBucketRegion = false; 
      if (attrs instanceof RegionAttributesImpl) {
        RegionAttributesImpl<?,?> regionAttributes = (RegionAttributesImpl<?,?>)attrs;
        if (regionAttributes.isForBucketRegion()) {
          isForBucketRegion = true;
        }
      }
      if (!isForBucketRegion) {
        ExpirationAction idleAction = attrs.getEntryIdleTimeout().getAction();
        ExpirationAction ttlAction = attrs.getEntryTimeToLive().getAction();

        if (idleAction == ExpirationAction.LOCAL_DESTROY
            || ttlAction == ExpirationAction.LOCAL_DESTROY) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_EXPIRATIONACTIONLOCAL_DESTROY_ON_THE_ENTRIES_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION.toLocalizedString());
        }

        if (attrs.getEvictionAttributes().getAction().isLocalDestroy()) {
          throw new IllegalStateException(LocalizedStrings.AttributesFactory_AN_EVICTION_CONTROLLER_WITH_LOCAL_DESTROY_EVICTION_ACTION_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION.toLocalizedString());
        }

        if (attrs.getRegionIdleTimeout().getAction() == ExpirationAction.LOCAL_INVALIDATE
            || attrs.getRegionTimeToLive().getAction() == ExpirationAction.LOCAL_INVALIDATE) {
          throw new IllegalStateException(LocalizedStrings.AttributesFactory_EXPIRATIONACTIONLOCAL_INVALIDATE_ON_THE_REGION_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION.toLocalizedString());
        }

        if (idleAction == ExpirationAction.LOCAL_INVALIDATE
            || ttlAction == ExpirationAction.LOCAL_INVALIDATE) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_EXPIRATIONACTIONLOCAL_INVALIDATE_ON_THE_ENTRIES_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION.toLocalizedString());
        }
         //TODO: Is it possible to add this check while region is getting created
//        for(String senderId : attrs.getGatewaySenderIds()){
//          if(sender.isParallel()){
//            throw new IllegalStateException(
//                LocalizedStrings.AttributesFactory_PARALLELGATEWAYSENDER_0_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION
//                    .toLocalizedString(sender));
//          }
//        }
      }
    }
    
    if (attrs.getDiskStoreName() != null) {
      EvictionAttributes ea = attrs.getEvictionAttributes();
      if (!attrs.getDataPolicy().withPersistence() && (ea != null && ea.getAction() != EvictionAction.OVERFLOW_TO_DISK)) {
        throw new IllegalStateException(LocalizedStrings.DiskStore_IS_USED_IN_NONPERSISTENT_REGION.toLocalizedString());        
      }
    }
    
    if (attrs.getHDFSStoreName() != null) {
      if (!attrs.getDataPolicy().withHDFS() && (attrs.getPartitionAttributes() == null || attrs.getPartitionAttributes().getLocalMaxMemory() != 0)) {
        throw new IllegalStateException(LocalizedStrings.HDFSSTORE_IS_USED_IN_NONHDFS_REGION.toLocalizedString());        
      }
    }

    if (!attrs.getStatisticsEnabled() &&
          (attrs.getRegionTimeToLive().getTimeout() != 0 ||
           attrs.getRegionIdleTimeout().getTimeout() != 0 ||
           attrs.getEntryTimeToLive().getTimeout() != 0 ||
           attrs.getEntryIdleTimeout().getTimeout() != 0 ||
           attrs.getCustomEntryIdleTimeout() != null ||
           attrs.getCustomEntryTimeToLive() != null)
           ) {
      throw new IllegalStateException(LocalizedStrings.AttributesFactory_STATISTICS_MUST_BE_ENABLED_FOR_EXPIRATION.toLocalizedString());
    }

    if (attrs.getDataPolicy() == DataPolicy.EMPTY) {
      if (attrs.getEntryTimeToLive().getTimeout() != 0 ||
          attrs.getEntryIdleTimeout().getTimeout() != 0 ||
          attrs.getCustomEntryTimeToLive() != null ||
          attrs.getCustomEntryIdleTimeout() != null
          ) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_IF_THE_DATA_POLICY_IS_0_THEN_ENTRY_EXPIRATION_IS_NOT_ALLOWED
            .toLocalizedString(attrs.getDataPolicy()));
      }
      if (!attrs.getEvictionAttributes().getAlgorithm().isNone()) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_IF_THE_DATA_POLICY_IS_0_THEN_EVICTION_IS_NOT_ALLOWED
            .toLocalizedString(attrs.getDataPolicy()));
      }
    }
    if (attrs.getMembershipAttributes().hasRequiredRoles()) {
      if (attrs.getScope().isLocal()) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_IF_THE_MEMBERSHIP_ATTRIBUTES_HAS_REQUIRED_ROLES_THEN_SCOPE_MUST_NOT_BE_LOCAL.toLocalizedString());
      }
    }
    
    final PartitionAttributes pa = attrs.getPartitionAttributes();
    // Validations for PartitionRegion Attributes
    if (pa != null) {
      ((PartitionAttributesImpl)pa).validateWhenAllAttributesAreSet(attrs instanceof RegionAttributesCreation);
      ExpirationAttributes regionIdleTimeout = attrs.getRegionIdleTimeout();
      ExpirationAttributes regionTimeToLive = attrs.getRegionTimeToLive();
      if ((regionIdleTimeout.getAction().isInvalidate() && regionIdleTimeout.getTimeout() > 0)
          || (regionIdleTimeout.getAction().isLocalInvalidate() && regionIdleTimeout.getTimeout() > 0)
          || (regionTimeToLive.getAction().isInvalidate() && regionTimeToLive.getTimeout() > 0)
          || (regionTimeToLive.getAction().isLocalInvalidate()) && regionTimeToLive.getTimeout() > 0 ) {
        throw new IllegalStateException(
            LocalizedStrings.AttributesFactory_INVALIDATE_REGION_NOT_SUPPORTED_FOR_PR.toLocalizedString());
      }
      
      if ((regionIdleTimeout.getAction().isDestroy() && regionIdleTimeout.getTimeout() > 0)
          || (regionIdleTimeout.getAction().isLocalDestroy() && regionIdleTimeout.getTimeout() > 0)
          || (regionTimeToLive.getAction().isDestroy() && regionTimeToLive.getTimeout() > 0)
          || (regionTimeToLive.getAction().isLocalDestroy() && regionTimeToLive.getTimeout() > 0)) {
        throw new IllegalStateException(
            LocalizedStrings.AttributesFactory_DESTROY_REGION_NOT_SUPPORTED_FOR_PR
                .toLocalizedString());
      }
      
      ExpirationAttributes entryIdleTimeout = attrs.getEntryIdleTimeout();
      ExpirationAttributes entryTimeToLive = attrs.getEntryTimeToLive();
      if ((entryIdleTimeout.getAction().isLocalDestroy() && entryIdleTimeout.getTimeout() > 0)
          || (entryTimeToLive.getAction().isLocalDestroy() && entryTimeToLive.getTimeout() > 0)) {
        throw new IllegalStateException(
            LocalizedStrings.AttributesFactory_LOCAL_DESTROY_IS_NOT_SUPPORTED_FOR_PR.toLocalizedString());
      }
      if ((entryIdleTimeout.getAction().isLocalInvalidate() && entryIdleTimeout.getTimeout() > 0)
          || (entryTimeToLive.getAction().isLocalInvalidate() && entryTimeToLive.getTimeout() > 0)) {
        throw new IllegalStateException(
            LocalizedStrings.AttributesFactory_LOCAL_INVALIDATE_IS_NOT_SUPPORTED_FOR_PR.toLocalizedString());
      }

      if (attrs instanceof UserSpecifiedRegionAttributes<?,?>) {
        UserSpecifiedRegionAttributes<?,?> rac = (UserSpecifiedRegionAttributes<?,?>) attrs;
        if (rac.hasScope()) {
          throw new IllegalStateException(LocalizedStrings.AttributesFactory_SETTING_SCOPE_ON_A_PARTITIONED_REGIONS_IS_NOT_ALLOWED.toLocalizedString());
        }
      }
      
      if (attrs.getPoolName() != null) {
        throw new IllegalStateException("Setting pool name on a Partitioned Region is not allowed");
      }
      
//    if (attrs.getScope() == Scope.GLOBAL) {
//    throw new IllegalStateException(
//    "Global Scope is incompatible with Partitioned Regions");
//  }
//  if (attrs.getScope() == Scope.LOCAL) {
//    throw new IllegalStateException(
//        "Local Scope is incompatible with Partitioned Regions");
//  }
      if (pa.getTotalMaxMemory() <= 0) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_TOTAL_SIZE_OF_PARTITION_REGION_MUST_BE_0.toLocalizedString());
      }
// listeners are supported here as of v5.1
//      if (attrs.getCacheListeners().length > 0) {
//        throw new IllegalStateException(
//            "Can not add cache listeners to RegionAttributes when PartitionAttributes are set.");
//      }
// loaders are supported here as of v5.1
//      if (attrs.getCacheLoader() != null) {
//        throw new IllegalStateException(
//            "Can not set CacheLoader in RegionAttributes when PartitionAttributes are set.");
//      }
      if ( ! PartitionedRegionHelper.ALLOWED_DATA_POLICIES.contains(attrs.getDataPolicy())) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_DATA_POLICIES_OTHER_THAN_0_ARE_NOT_ALLOWED_IN_PARTITIONED_REGIONS
            .toLocalizedString(PartitionedRegionHelper.ALLOWED_DATA_POLICIES));
      }
//      if ( attrs.getDataPolicy().isEmpty() && pa.getLocalMaxMemory() != 0) {
//        throw new IllegalStateException(
//            "A non-zero PartitionAttributes localMaxMemory setting is not compatible" +
//            " with an empty DataPolicy.  Please use DataPolicy.NORMAL instead.");
//      }
      
      // fix bug #52033 by invoking getLocalMaxMemoryForValidation here
      if (((PartitionAttributesImpl)pa).getLocalMaxMemoryForValidation() < 0) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_PARTITIONATTRIBUTES_LOCALMAXMEMORY_MUST_NOT_BE_NEGATIVE.toLocalizedString());
      }
      
      if (attrs.isLockGrantor() == true) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_SETLOCKGRANTERTRUE_IS_NOT_ALLOWED_IN_PARTITIONED_REGIONS.toLocalizedString());
      }
      
      // fix bug #52033 by invoking getLocalMaxMemoryForValidation here
      if (((PartitionAttributesImpl)pa).getLocalMaxMemoryForValidation() == 0 && attrs.getDataPolicy() == DataPolicy.PERSISTENT_PARTITION) {
        throw new IllegalStateException("Persistence is not allowed when local-max-memory is zero.");
      }
    }
    
    if (null != attrs.getCompressor() && !attrs.getCloningEnabled()) {
      throw new IllegalStateException("Cloning cannot be disabled when a compressor is set.");
    }
  }


  private static class RegionAttributesImpl<K,V>
  extends UserSpecifiedRegionAttributes<K,V> implements Cloneable, Serializable {
    public Set<String> gatewaySenderIds;
    public Set<String>  asyncEventQueueIds;
    private static final long serialVersionUID = -3663000883567530374L;

    ArrayList<CacheListener<K,V>> cacheListeners;
    CacheLoader<K,V> cacheLoader;
    CacheWriter<K,V> cacheWriter;
    int regionTimeToLive = 0;
    ExpirationAction regionTimeToLiveExpirationAction = ExpirationAction.INVALIDATE;
    int regionIdleTimeout = 0;
    ExpirationAction regionIdleTimeoutExpirationAction = ExpirationAction.INVALIDATE;

    int entryTimeToLive = 0;
    ExpirationAction entryTimeToLiveExpirationAction = ExpirationAction.INVALIDATE;
    CustomExpiry<K,V> customEntryTimeToLive = null;
    int entryIdleTimeout = 0;
    ExpirationAction entryIdleTimeoutExpirationAction = ExpirationAction.INVALIDATE;
    CustomExpiry<K,V> customEntryIdleTimeout = null;

    Scope scope = AbstractRegion.DEFAULT_SCOPE;
    DataPolicy dataPolicy = DataPolicy.DEFAULT;
    boolean statisticsEnabled = false;
    boolean ignoreJTA = false;
    boolean isLockGrantor = false;
    Class<K> keyConstraint = null;
    Class<V> valueConstraint = null;
    int initialCapacity = 16;
    float loadFactor = 0.75f;
    int concurrencyLevel = 16;
    boolean concurrencyChecksEnabled = true;
    boolean earlyAck = false;
    boolean publisher = false;
    boolean enableAsyncConflation = false;
    boolean enableSubscriptionConflation = false;
    @SuppressWarnings("deprecation")
    DiskWriteAttributes diskWriteAttributes = DiskWriteAttributesImpl.getDefaultSyncInstance();
    File[] diskDirs = DiskStoreFactory.DEFAULT_DISK_DIRS;
    int[] diskSizes = new int[] {DiskStoreFactory.DEFAULT_DISK_DIR_SIZE}; // 10* 1024 MB }
    boolean indexMaintenanceSynchronous = true;
    PartitionAttributes partitionAttributes = null; //new PartitionAttributes();
    MembershipAttributes membershipAttributes = new MembershipAttributes();
    SubscriptionAttributes subscriptionAttributes = new SubscriptionAttributes();
    boolean multicastEnabled = false;
    EvictionAttributesImpl evictionAttributes = new EvictionAttributesImpl();  // TODO need to determine the constructor
    transient CustomEvictionAttributes customEvictionAttributes;
    String poolName = null;
    String diskStoreName = null;
    String hdfsStoreName = null;
    private boolean hdfsWriteOnly = false;
    boolean diskSynchronous = DEFAULT_DISK_SYNCHRONOUS;
    protected boolean isBucketRegion = false;
    private boolean isCloningEnabled = false;
    Compressor compressor = null;
    
    boolean offHeap = false;

    /** Constructs an instance of <code>RegionAttributes</code> with default settings.
     * @see AttributesFactory
     */
    public RegionAttributesImpl() {
    }
    @Override
    public String toString() {
      StringBuffer buf = new StringBuffer(1000);
      buf
        .append("RegionAttributes@").append(System.identityHashCode(this)).append(": ")
        .append("scope=").append(scope)
        .append("; earlyAck=").append(earlyAck)
        .append("; publisher=").append(publisher)
        .append("; partitionAttrs=").append(partitionAttributes)
        .append("; membershipAttrs=").append(membershipAttributes)
        .append("; subscriptionAttrs=").append(subscriptionAttributes)
        .append("; regionTTL=").append(regionTimeToLive)
        .append("; action=").append(regionTimeToLiveExpirationAction)
        .append("; regionIdleTimeout=").append(regionIdleTimeout)
        .append("; action=").append(regionIdleTimeoutExpirationAction)
        .append("; TTL=").append(entryTimeToLive)
        .append("; action=").append(entryTimeToLiveExpirationAction)
        .append("; custom=").append(customEntryTimeToLive)
        .append("; idleTimeout=").append(entryIdleTimeout)
        .append("; action=").append(entryIdleTimeoutExpirationAction)
        .append("; custom=").append(customEntryIdleTimeout)
        .append("; dataPolicy=").append(dataPolicy)
        .append("; statisticsEnabled=").append(statisticsEnabled)
        .append("; ignoreJTA=").append(ignoreJTA)
        .append("; isLockGrantor=").append(isLockGrantor)
        .append("; keyConstraint=").append(keyConstraint)
        .append("; valueConstraint=").append(valueConstraint)
        .append("; initialCapacity=").append(initialCapacity)
        .append("; loadFactor=").append(loadFactor)
        .append("; concurrencyLevel=").append(concurrencyLevel)
        .append("; concurrencyChecksEnabled=").append(concurrencyChecksEnabled)
        .append("; enableAsyncConflation=").append(enableAsyncConflation)
        .append("; enableSubscriptionConflation=").append(enableSubscriptionConflation)
        .append("; isBucketRegion=").append(isBucketRegion) 
        .append("; poolName=").append(poolName)
        .append("; diskSynchronous=").append(diskSynchronous)
        .append("; multicastEnabled=").append(multicastEnabled)
        .append("; isCloningEnabled=").append(isCloningEnabled)
        ;
      if (hasDiskWriteAttributes() || hasDiskDirs()) {
        buf.append("; diskAttrs=").append(diskWriteAttributes)
          .append("; diskDirs=").append(Arrays.toString(diskDirs))
          .append("; diskDirSizes=").append(Arrays.toString(diskSizes));
      } else {
        buf.append("; diskStoreName=").append(diskStoreName);
      }
      buf.append("; hdfsStoreName=").append(hdfsStoreName);
      buf.append("; hdfsWriteOnly=").append(hdfsWriteOnly);
      buf.append("; GatewaySenderIds=").append(gatewaySenderIds);
      buf.append("; AsyncEventQueueIds=").append(asyncEventQueueIds);
      buf.append("; compressor=").append(compressor == null ? null : compressor.getClass().getName());
      buf.append("; offHeap=").append(offHeap);
      return buf.toString();
    }
    public CacheLoader<K,V> getCacheLoader() {
      return this.cacheLoader;
    }
    public CacheWriter<K,V> getCacheWriter() {
      return this.cacheWriter;
    }
    public Class<K> getKeyConstraint() {
      return this.keyConstraint;
    }
    public Class<V> getValueConstraint() {
      return this.valueConstraint;
    }
    private boolean isForBucketRegion() {
      return this.isBucketRegion;
    } 
    public ExpirationAttributes getRegionTimeToLive() {
      return new ExpirationAttributes(
      this.regionTimeToLive, this.regionTimeToLiveExpirationAction);
    }
    public ExpirationAttributes getRegionIdleTimeout() {
      return new ExpirationAttributes(
      this.regionIdleTimeout, this.regionIdleTimeoutExpirationAction);
    }

    public ExpirationAttributes getEntryTimeToLive() {
      return new ExpirationAttributes(
      this.entryTimeToLive, this.entryTimeToLiveExpirationAction);
    }
    public CustomExpiry<K,V> getCustomEntryTimeToLive() {
      return this.customEntryTimeToLive;
    }
    public ExpirationAttributes getEntryIdleTimeout() {
      return new ExpirationAttributes(
      this.entryIdleTimeout, this.entryIdleTimeoutExpirationAction);
    }
    public CustomExpiry<K,V> getCustomEntryIdleTimeout() {
      return this.customEntryIdleTimeout;
    }

    @SuppressWarnings("deprecation")
    public MirrorType getMirrorType() {
      if (this.dataPolicy.isNormal() || this.dataPolicy.isPreloaded()
          || this.dataPolicy.isEmpty() || this.dataPolicy.withPartitioning()) {
        return MirrorType.NONE;
      } else if (this.dataPolicy.withReplication()) {
        return MirrorType.KEYS_VALUES;
      } else {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_NO_MIRROR_TYPE_CORRESPONDS_TO_DATA_POLICY_0
            .toLocalizedString(this.dataPolicy));
      }
    }
    public DataPolicy getDataPolicy() {
      return this.dataPolicy;
    }
    public void setDataPolicy(DataPolicy dp) {
      this.dataPolicy = dp;
      setHasDataPolicy(true);
    }
    
    public Scope getScope() {
      return this.scope;
    }
    public void setScope(Scope s) {
      this.scope = s;
      setHasScope(true);
    }
    private static final CacheListener<?,?>[] EMPTY_LISTENERS = new CacheListener[0];
    @SuppressWarnings("unchecked")
    public CacheListener<K,V>[] getCacheListeners() {
      ArrayList<CacheListener<K,V>> listeners = this.cacheListeners;
      if (listeners == null) {
        return (CacheListener<K,V>[])EMPTY_LISTENERS;
      } else {
        synchronized (listeners) {
          if (listeners.size() == 0) {
            return (CacheListener<K,V>[])EMPTY_LISTENERS;
          } else {
            CacheListener<K,V>[] result = new CacheListener[listeners.size()];
            listeners.toArray(result);
            return result;
          }
        }
      }
    }
    public CacheListener<K,V> getCacheListener() {
      ArrayList<CacheListener<K,V>> listeners = this.cacheListeners;
      if (listeners == null) {
        return null;
      }
      synchronized (listeners) {
        if (listeners.size() == 0) {
          return null;
        }
        if (listeners.size() == 1) {
          return this.cacheListeners.get(0);
        }
      }
      throw new IllegalStateException(LocalizedStrings.AttributesFactory_MORE_THAN_ONE_CACHE_LISTENER_EXISTS.toLocalizedString());
    }
    protected void addCacheListener(CacheListener<K,V> aListener) {
      ArrayList<CacheListener<K,V>> listeners = this.cacheListeners;
      if (listeners == null) {
        ArrayList<CacheListener<K,V>> al = new ArrayList<CacheListener<K,V>>(1);
        al.add(aListener);
        this.cacheListeners = al;
      } else {
        synchronized (listeners) {
          listeners.add(aListener);
        }
      }
      setHasCacheListeners(true);
    }
    
    public void addGatewaySenderId(String gatewaySenderId) {
      if(this.gatewaySenderIds == null){
        this.gatewaySenderIds = new CopyOnWriteArraySet<String>();
        this.gatewaySenderIds.add(gatewaySenderId);
      }else{
        synchronized (this.gatewaySenderIds) { // TODO: revisit this
          // synchronization : added as per
          // above code
          if (this.gatewaySenderIds.contains(gatewaySenderId)) {
            throw new IllegalArgumentException(
                LocalizedStrings.AttributesFactory_GATEWAY_SENDER_ID_0_IS_ALREADY_ADDED
                .toLocalizedString(gatewaySenderId));
          }
          this.gatewaySenderIds.add(gatewaySenderId);
        }
      }
      setHasGatewaySenderIds(true);
    }
    
    public void addAsyncEventQueueId(String asyncEventQueueId) {
      if(this.asyncEventQueueIds == null){
        this.asyncEventQueueIds = new CopyOnWriteArraySet<String>();
        this.asyncEventQueueIds.add(asyncEventQueueId);
      } else{
        synchronized (this.asyncEventQueueIds) { // TODO: revisit this
          // synchronization : added as per
          // above code
          if (this.asyncEventQueueIds.contains(asyncEventQueueId)) {
            throw new IllegalArgumentException(
                LocalizedStrings.AttributesFactory_ASYNC_EVENT_QUEUE_ID_0_IS_ALREADY_ADDED
                .toLocalizedString(asyncEventQueueId));
          }
          this.asyncEventQueueIds.add(asyncEventQueueId);
        }
      }
      setHasAsyncEventListeners(true);
    }
    
    public int getInitialCapacity() {
      return this.initialCapacity;
    }
    public float getLoadFactor() {
      return this.loadFactor;
    }
    public boolean getStatisticsEnabled() {
      return this.statisticsEnabled;
    }
    public boolean getIgnoreJTA() {
      return this.ignoreJTA;
    }
    public boolean isLockGrantor() {
      return this.isLockGrantor;
    }
    public int getConcurrencyLevel() {
      return this.concurrencyLevel;
    }
    public boolean getConcurrencyChecksEnabled() {
      return this.concurrencyChecksEnabled;
    }
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Object clone() {
      try {
        RegionAttributesImpl<K,V> copy = (RegionAttributesImpl<K,V>) super.clone();
        if (copy.getIndexes() != null) {
          copy.setIndexes(new ArrayList(copy.getIndexes()));
        }
        if (copy.partitionAttributes != null) {
          copy.partitionAttributes = ((PartitionAttributesImpl)copy.partitionAttributes).copy();
        }
        if (copy.cacheListeners != null) {
          copy.cacheListeners = new ArrayList<CacheListener<K,V>>(copy.cacheListeners);
        }
        if (copy.gatewaySenderIds != null) {
          copy.gatewaySenderIds =  new CopyOnWriteArraySet<String>(copy.gatewaySenderIds);
        }
        if (copy.asyncEventQueueIds != null) {
          copy.asyncEventQueueIds = new CopyOnWriteArraySet<String>(copy.asyncEventQueueIds);
        }
        return copy;
      }
      catch (CloneNotSupportedException e) {
        throw new InternalError(LocalizedStrings.AttributesFactory_CLONENOTSUPPORTEDEXCEPTION_THROWN_IN_CLASS_THAT_IMPLEMENTS_CLONEABLE.toLocalizedString());
      }
    }

    public boolean getPersistBackup() {
      return getDataPolicy().withPersistence();
    }

    public boolean getEarlyAck() {
      return this.earlyAck;
    }

    /*
     * @deprecated as of 6.5
     */
    @Deprecated
    public boolean getPublisher() {
      return this.publisher;
    }

    public boolean getEnableConflation() { // deprecated in 5.0
      return getEnableSubscriptionConflation();
    }

    public boolean getEnableAsyncConflation() {
      return this.enableAsyncConflation;
    }

    public boolean getEnableBridgeConflation() { // deprecated in 5.7
      return getEnableSubscriptionConflation();
    }

    public boolean getEnableSubscriptionConflation() {
      return this.enableSubscriptionConflation;
    }
    
    /**
     * @deprecated as of 6.5
     */
    @Deprecated
    public DiskWriteAttributes getDiskWriteAttributes() {
      if (this.diskStoreName != null) {
        throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
            .toLocalizedString(new Object[] {"getDiskWriteAttributes", this.diskStoreName}));
      }
      return this.diskWriteAttributes;
    }

    /**
     * @deprecated as of 6.5
     */
    @Deprecated
    public File[] getDiskDirs() {
      if (this.diskStoreName != null) {
        throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
            .toLocalizedString(new Object[] {"getDiskDirs", this.diskStoreName}));
      }
      return this.diskDirs;
    }

    public boolean getIndexMaintenanceSynchronous() {
      return this.indexMaintenanceSynchronous;
    }

    public PartitionAttributes getPartitionAttributes() {
      return this.partitionAttributes;
    }
    public EvictionAttributes getEvictionAttributes()
    {
      return this.evictionAttributes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CustomEvictionAttributes getCustomEvictionAttributes() {
      return this.customEvictionAttributes;
    }

    public MembershipAttributes getMembershipAttributes() {
      return this.membershipAttributes;
    }
    public SubscriptionAttributes getSubscriptionAttributes() {
      return this.subscriptionAttributes;
    }
    
    /**
     * @deprecated as of 6.5
     */
    @Deprecated
    public int[] getDiskDirSizes() {
      if (this.diskStoreName != null) {
        throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
            .toLocalizedString(new Object[] {"getDiskDirSizes", this.diskStoreName}));
      }
      return this.diskSizes;
    }
    public String getDiskStoreName() {
      return this.diskStoreName;
    }
    public boolean getMulticastEnabled() {
      return this.multicastEnabled; 
    }
    
    public String getPoolName() {
      return this.poolName;
    }
    public boolean getCloningEnabled() {
      return this.isCloningEnabled;
    }
//    public void setCloningEnable(boolean val) {
//      this.isCloningEnabled = val;
//      setHasCloningEnabled(true);
//    }
    public boolean isDiskSynchronous() {
      return this.diskSynchronous;
    }

    public Set<String> getGatewaySenderIds() {
      if(!hasGatewaySenderId()){
        this.gatewaySenderIds = new CopyOnWriteArraySet<String>();
      }
      return this.gatewaySenderIds;
    }
    
    public Set<String> getAsyncEventQueueIds() {
      if(!hasAsyncEventListeners()){
        this.asyncEventQueueIds = new CopyOnWriteArraySet<String>();
      }
      return this.asyncEventQueueIds;
    }

    @Override
    public String getHDFSStoreName() {
      return hdfsStoreName;
    }
    
    @Override
    public boolean getHDFSWriteOnly() {
      return hdfsWriteOnly;
    }

    @Override
    public Compressor getCompressor() {
      return this.compressor;
    }
    
    @Override
    public boolean getOffHeap() {
      return this.offHeap;
    }
  }
}
