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
package org.apache.geode.internal.cache.xmlcache;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.UserSpecifiedRegionAttributes;

/**
 * Represents {@link RegionAttributes} that are created declaratively. Notice that it implements the
 * {@link RegionAttributes} interface so that this class must be updated when
 * {@link RegionAttributes} is modified. This class is public for testing purposes.
 *
 *
 * @since GemFire 3.0
 */
public class RegionAttributesCreation extends UserSpecifiedRegionAttributes
    implements Serializable {
  private static final long serialVersionUID = 2241078661206355376L;

  private static final RegionAttributes defaultAttributes = new AttributesFactory().create();

  /** The attributes' cache listener */
  private ArrayList cacheListeners;
  /** The attributes' gateway senders */
  private Set<String> gatewaySenderIds;
  /** The attributes' AsyncEventQueues */
  private Set<String> asyncEventQueueIds;
  /** The attributes' cache loader */
  private CacheLoader cacheLoader;
  /** The attributes' cache writer */
  private CacheWriter cacheWriter;
  /** The attributes' entry idle timeout */
  private ExpirationAttributes entryIdleTimeout;
  /** The attributes' custom entry idle timeout */
  private CustomExpiry customEntryIdleTimeout;
  /** The attributes' entry time to live */
  private ExpirationAttributes entryTimeToLive;
  /** The attributes' custom entry time to live */
  private CustomExpiry customEntryTimeToLive;
  /** The attributes' initial capacity */
  private int initialCapacity;
  /** The attributes' key constraint */
  private Class keyConstraint;
  /** The attributes' key constraint */
  private Class valueConstraint;
  /** The attributes' load factor */
  private float loadFactor;
  /** The attributes' region idle timeout */
  private ExpirationAttributes regionIdleTimeout;
  /** The attributes' region time to live */
  private ExpirationAttributes regionTimeToLive;
  /** The attributes' scope */
  private Scope scope;
  /** The attributes' statistics enabled */
  private boolean statisticsEnabled;
  /** The attributes ignore-jta flag */
  private boolean ignoreJTA;
  /** The attributes' become lock grantor setting */
  private boolean isLockGrantor;
  /** The attributes' concurrency level */
  private int concurrencyLevel;
  /** whether versioning is enabled */
  private boolean concurrencyChecksEnabled = true;
  /** The attributes' EarlyAck */
  private boolean earlyAck;
  /** The attributes' MulticastEnabled */
  private boolean multicastEnabled;
  /** The attributes' disk write attributes */
  private DiskWriteAttributes diskWriteAttributes;
  /** The attributes' disk directories */
  private File[] diskDirs;
  private int[] diskSizes;
  /**
   * disk store name of the region
   *
   * @since GemFire prPersistPrint2
   */
  private String diskStoreName;
  private boolean isDiskSynchronous = AttributesFactory.DEFAULT_DISK_SYNCHRONOUS;

  private boolean cloningEnabled = false;

  /** The DataPolicy attribute */
  private DataPolicy dataPolicy;
  private boolean indexMaintenanceSynchronous;
  /**
   * The attributes's id
   *
   * @since GemFire 4.1
   */
  private String id;

  /**
   * The id of the attributes that this attributes "inherits"
   *
   * @since GemFire 4.1
   */
  private String refid;

  /** The partitioning attributes */
  private PartitionAttributes partitionAttributes;
  /** The membership attributes */
  private MembershipAttributes membershipAttributes;
  /** The subscription attributes */
  private SubscriptionAttributes subscriptionAttributes;
  private EvictionAttributesImpl evictionAttributes;

  /**
   * Whether to mark this region as a publisher
   *
   * @since GemFire 4.2.3
   */
  private boolean publisher;

  /**
   * Whether to enable subscription conflation for this region
   *
   * @since GemFire 4.2
   */
  private boolean enableSubscriptionConflation;

  /**
   * Whether to enable a async conflation for this region
   *
   * @since GemFire 4.2.3
   */
  private boolean enableAsyncConflation;

  /**
   * The client to server Connection Pool
   *
   * @since GemFire 5.7
   */
  private String poolName;

  /**
   * The region compressor.
   *
   * @since GemFire 8.0
   */
  private Compressor compressor;

  /**
   * True if usage of off-heap memory is enabled for this region.
   *
   * @since Geode 1.0
   */
  private boolean offHeap;

  private static RegionAttributes getDefaultAttributes(CacheCreation cc) {
    if (cc != null) {
      return cc.getDefaultAttributes();
    } else {
      return defaultAttributes;
    }
  }

  /**
   * Creates a new <code>RegionAttributesCreation</code> with the default region attributes.
   */
  public RegionAttributesCreation(CacheCreation cc) {
    this(cc, getDefaultAttributes(cc), true);
  }

  public RegionAttributesCreation() {
    this(defaultAttributes, true);
  }

  public RegionAttributesCreation(RegionAttributes attrs, boolean defaults) {
    this(null, attrs, defaults);
  }

  /**
   * Creates a new <code>RegionAttributesCreation</code> with the given region attributes. NOTE:
   * Currently attrs will not be an instance of RegionAttributesCreation. If it could be then this
   * code should be changed to use attrs' hasXXX methods to initialize the has booleans when
   * defaults is false.
   *
   * @param attrs the attributes with which to initialize this region.
   * @param defaults true if <code>attrs</code> are defaults; false if they are not
   */
  public RegionAttributesCreation(CacheCreation cc, RegionAttributes attrs, boolean defaults) {
    this.cacheListeners = new ArrayList(Arrays.asList(attrs.getCacheListeners()));
    this.gatewaySenderIds = new HashSet<String>(attrs.getGatewaySenderIds());
    this.asyncEventQueueIds = new HashSet<String>(attrs.getAsyncEventQueueIds());
    this.cacheLoader = attrs.getCacheLoader();
    this.cacheWriter = attrs.getCacheWriter();
    this.entryIdleTimeout = attrs.getEntryIdleTimeout();
    this.customEntryIdleTimeout = attrs.getCustomEntryIdleTimeout();
    this.entryTimeToLive = attrs.getEntryTimeToLive();
    this.customEntryTimeToLive = attrs.getCustomEntryTimeToLive();
    this.initialCapacity = attrs.getInitialCapacity();
    this.keyConstraint = attrs.getKeyConstraint();
    this.valueConstraint = attrs.getValueConstraint();
    this.loadFactor = attrs.getLoadFactor();
    this.regionIdleTimeout = attrs.getRegionIdleTimeout();
    this.regionTimeToLive = attrs.getRegionTimeToLive();
    this.scope = attrs.getScope();
    this.statisticsEnabled = attrs.getStatisticsEnabled();
    this.ignoreJTA = attrs.getIgnoreJTA();
    this.concurrencyLevel = attrs.getConcurrencyLevel();
    this.concurrencyChecksEnabled = attrs.getConcurrencyChecksEnabled();
    this.earlyAck = attrs.getEarlyAck();
    this.diskStoreName = attrs.getDiskStoreName();
    if (this.diskStoreName == null) {
      this.diskWriteAttributes = attrs.getDiskWriteAttributes();
      this.diskDirs = attrs.getDiskDirs();
      this.diskSizes = attrs.getDiskDirSizes();
    } else {
      this.diskWriteAttributes = null;
      this.diskDirs = null;
      this.diskSizes = null;
    }
    this.isDiskSynchronous = attrs.isDiskSynchronous();
    this.indexMaintenanceSynchronous = attrs.getIndexMaintenanceSynchronous();
    this.partitionAttributes = attrs.getPartitionAttributes();
    this.membershipAttributes = attrs.getMembershipAttributes();
    this.subscriptionAttributes = attrs.getSubscriptionAttributes();
    this.dataPolicy = attrs.getDataPolicy();
    this.evictionAttributes = (EvictionAttributesImpl) attrs.getEvictionAttributes();
    this.id = null;
    this.refid = null;
    this.enableSubscriptionConflation = attrs.getEnableSubscriptionConflation();
    this.publisher = attrs.getPublisher();
    this.enableAsyncConflation = attrs.getEnableAsyncConflation();
    this.poolName = attrs.getPoolName();
    this.multicastEnabled = attrs.getMulticastEnabled();
    this.cloningEnabled = attrs.getCloningEnabled();

    this.compressor = attrs.getCompressor();
    this.offHeap = attrs.getOffHeap();
    if (attrs instanceof UserSpecifiedRegionAttributes) {
      UserSpecifiedRegionAttributes nonDefault = (UserSpecifiedRegionAttributes) attrs;
      this.requiresPoolName = nonDefault.requiresPoolName;
      if (!defaults) {
        // Selectively set has* fields to true, propagating those non-default
        // (aka user specified) fields as such
        initHasFields(nonDefault);
      }
    } else if (!defaults) {
      // Set all fields to true
      setAllHasFields(true);
    }
  }

  /**
   * Returns whether or not two objects are {@linkplain Object#equals equals} taking
   * <code>null</code> into account.
   */
  static boolean equal(Object o1, Object o2) {
    if (o1 == null) {
      if (o2 != null) {
        return false;

      } else {
        return true;
      }

    } else {
      return o1.equals(o2);
    }
  }

  /**
   * returns true if two long[] are equal
   *
   * @return true if equal
   */
  private boolean equal(long[] array1, long[] array2) {
    if (array1.length != array2.length) {
      return false;
    }
    for (int i = 0; i < array1.length; i++) {
      if (array1[i] != array2[i]) {
        return false;
      }
    }
    return true;
  }


  /**
   * returns true if two int[] are equal
   *
   * @return true if equal
   */
  private boolean equal(int[] array1, int[] array2) {
    if (array1.length != array2.length) {
      return false;
    }
    for (int i = 0; i < array1.length; i++) {
      if (array1[i] != array2[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether or not two <code>File</code> arrays specify the same files.
   */
  private boolean equal(File[] array1, File[] array2) {
    if (array1.length != array2.length) {
      return false;
    }

    for (int i = 0; i < array1.length; i++) {
      boolean found = false;
      for (int j = 0; j < array2.length; j++) {
        if (equal(array1[i].getAbsoluteFile(), array2[j].getAbsoluteFile())) {
          found = true;
          break;
        }
      }

      if (!found) {
        StringBuffer sb = new StringBuffer();
        sb.append("Didn't find ");
        sb.append(array1[i]);
        sb.append(" in ");
        for (int k = 0; k < array2.length; k++) {
          sb.append(array2[k]);
          sb.append(" ");
        }
        System.out.println(sb);
        return false;
      }
    }

    return true;
  }

  /**
   * Returns whether or not this <code>RegionAttributesCreation</code> is equivalent to another
   * <code>RegionAttributes</code>.
   */
  public boolean sameAs(RegionAttributes other) {
    if (!equal(this.cacheListeners, Arrays.asList(other.getCacheListeners()))) {
      throw new RuntimeException(
          "CacheListeners are not the same");
    }
    if (!equal(this.entryIdleTimeout, other.getEntryIdleTimeout())) {
      throw new RuntimeException(
          "EntryIdleTimeout is not the same");
    }
    if (!equal(this.customEntryIdleTimeout, other.getCustomEntryIdleTimeout())) {
      throw new RuntimeException(String.format(
          "CustomEntryIdleTimeout is not the same. this %s, other: %s", this.customEntryIdleTimeout,
          other.getCustomEntryIdleTimeout()));
    }
    if (!equal(this.entryTimeToLive, other.getEntryTimeToLive())) {
      throw new RuntimeException(
          "EntryTimeToLive is not the same");
    }
    if (!equal(this.customEntryTimeToLive, other.getCustomEntryTimeToLive())) {
      throw new RuntimeException(
          "CustomEntryTimeToLive is not the same");
    }
    if (!equal(this.partitionAttributes, other.getPartitionAttributes())) {
      throw new RuntimeException(
          String.format("PartitionAttributes are not the same. this: %s, other: %s",
              this, other.getPartitionAttributes()));
    }
    if (!equal(this.membershipAttributes, other.getMembershipAttributes())) {
      throw new RuntimeException(
          "Membership Attributes are not the same");
    }
    if (!equal(this.subscriptionAttributes, other.getSubscriptionAttributes())) {
      throw new RuntimeException(
          "Subscription Attributes are not the same");
    }
    if (!equal(this.evictionAttributes, other.getEvictionAttributes())) {
      throw new RuntimeException(
          String.format("Eviction Attributes are not the same: this: %s other: %s",
              this.evictionAttributes, other.getEvictionAttributes()));
    }
    if (this.diskStoreName == null) {
      // only compare the DWA, diskDirs and diskSizes when disk store is not configured
      if (!equal(this.diskWriteAttributes, other.getDiskWriteAttributes())) {
        throw new RuntimeException(
            "DistWriteAttributes are not the same");
      }
      if (!equal(this.diskDirs, other.getDiskDirs())) {
        throw new RuntimeException(
            "Disk Dirs are not the same");
      }
      if (!equal(this.diskSizes, other.getDiskDirSizes())) {
        throw new RuntimeException(
            "Disk Dir Sizes are not the same");
      }
    }
    if (!equal(this.diskStoreName, other.getDiskStoreName())) {
      throw new RuntimeException(
          String.format("DiskStore is not the same: this: %s other: %s",
              new Object[] {this.diskStoreName, other.getDiskStoreName()}));
    }
    if (this.isDiskSynchronous != other.isDiskSynchronous()) {
      throw new RuntimeException(
          "Disk Synchronous write is not the same.");
    }
    if (this.dataPolicy != other.getDataPolicy()) {
      throw new RuntimeException(
          String.format("Data Policies are not the same: this: %s other: %s",
              new Object[] {this.getDataPolicy(), other.getDataPolicy()}));
    }
    if (this.earlyAck != other.getEarlyAck()) {
      throw new RuntimeException(
          "Early Ack is not the same");
    }
    if (this.enableSubscriptionConflation != other.getEnableSubscriptionConflation()) {
      throw new RuntimeException(
          "Enable Subscription Conflation is not the same");
    }
    if (this.enableAsyncConflation != other.getEnableAsyncConflation()) {
      throw new RuntimeException(
          "Enable Async Conflation is not the same");
    }
    if (this.initialCapacity != other.getInitialCapacity()) {
      throw new RuntimeException(
          "initial Capacity is not the same");
    }
    if (!equal(this.keyConstraint, other.getKeyConstraint())) {
      throw new RuntimeException(
          "Key Constraints are not the same");
    }
    if (!equal(this.valueConstraint, other.getValueConstraint())) {
      throw new RuntimeException(
          "Value Constraints are not the same");
    }
    if (this.loadFactor != other.getLoadFactor()) {
      throw new RuntimeException(
          "Load Factors are not the same");
    }
    if (!equal(this.regionIdleTimeout, other.getRegionIdleTimeout())) {
      throw new RuntimeException(
          "Region Idle Timeout is not the same");
    }
    if (!equal(this.scope, this.getScope())) {
      throw new RuntimeException(
          "Scope is not the same");
    }
    if (this.statisticsEnabled != other.getStatisticsEnabled()) {
      throw new RuntimeException(
          String.format("Statistics enabled is not the same: this: %s other: %s",
              new Object[] {Boolean.valueOf(this.statisticsEnabled),
                  Boolean.valueOf(other.getStatisticsEnabled())}));
    }
    if (this.ignoreJTA != other.getIgnoreJTA()) {
      throw new RuntimeException(
          "Ignore JTA is not the same");
    }
    if (this.concurrencyLevel != other.getConcurrencyLevel()) {
      throw new RuntimeException(
          "ConcurrencyLevel is not the same");
    }
    if (this.concurrencyChecksEnabled != other.getConcurrencyChecksEnabled()) {
      throw new RuntimeException(
          "ConcurrencyChecksEnabled is not the same");
    }
    if (this.indexMaintenanceSynchronous != other.getIndexMaintenanceSynchronous()) {
      throw new RuntimeException(
          "Index Maintenance Synchronous is not the same");
    }
    if (!equal(this.poolName, other.getPoolName())) {
      throw new RuntimeException(
          "poolName is not the same: " + this.poolName + " != " + other.getPoolName());
    }
    if (!equal(this.cacheLoader, other.getCacheLoader())) {
      throw new RuntimeException("CacheLoader are not the same");
    }
    if (!equal(this.cacheWriter, other.getCacheWriter())) {
      throw new RuntimeException("CacheWriter is not the same");
    }
    if (this.multicastEnabled != other.getMulticastEnabled()) {
      String s = "MulticastEnabled is not the same: " + this.multicastEnabled + "!="
          + other.getMulticastEnabled();
      throw new RuntimeException(s);
    }
    if (this.cloningEnabled != other.getCloningEnabled()) {
      throw new RuntimeException(
          String.format("Cloning enabled is not the same: this: %s other: %s",
              new Object[] {Boolean.valueOf(this.cloningEnabled),
                  Boolean.valueOf(other.getCloningEnabled())}));
    }
    if (!equal(this.compressor, other.getCompressor())) {
      throw new RuntimeException("Compressors are not the same.");
    }
    if (this.offHeap != other.getOffHeap()) {
      throw new RuntimeException(
          "EnableOffHeapMemory is not the same");
    }
    return true;
  }

  public CacheLoader getCacheLoader() {
    return this.cacheLoader;
  }

  public CacheLoader setCacheLoader(CacheLoader cacheLoader) {
    CacheLoader old = this.cacheLoader;
    this.cacheLoader = cacheLoader;
    setHasCacheLoader(true);
    return old;
  }

  public CacheWriter getCacheWriter() {
    return this.cacheWriter;
  }

  public CacheWriter setCacheWriter(CacheWriter cacheWriter) {
    CacheWriter old = this.cacheWriter;
    this.cacheWriter = cacheWriter;
    setHasCacheWriter(true);
    return old;
  }

  public Class getKeyConstraint() {
    return this.keyConstraint;
  }

  public void setKeyConstraint(Class keyConstraint) {
    this.keyConstraint = keyConstraint;
    setHasKeyConstraint(true);
  }

  public Class getValueConstraint() {
    return this.valueConstraint;
  }

  public void setValueConstraint(Class valueConstraint) {
    this.valueConstraint = valueConstraint;
    setHasValueConstraint(true);
  }

  public ExpirationAttributes getRegionTimeToLive() {
    return this.regionTimeToLive;
  }

  public ExpirationAttributes setRegionTimeToLive(ExpirationAttributes timeToLive) {
    ExpirationAttributes old = this.regionTimeToLive;
    this.regionTimeToLive = timeToLive;
    setHasRegionTimeToLive(true);
    return old;
  }

  public ExpirationAttributes getRegionIdleTimeout() {
    return this.regionIdleTimeout;
  }

  public ExpirationAttributes setRegionIdleTimeout(ExpirationAttributes idleTimeout) {
    ExpirationAttributes old = this.regionIdleTimeout;
    this.regionIdleTimeout = idleTimeout;
    setHasRegionIdleTimeout(true);
    return old;
  }

  public ExpirationAttributes getEntryTimeToLive() {
    return this.entryTimeToLive;
  }

  public CustomExpiry getCustomEntryTimeToLive() {
    return this.customEntryTimeToLive;
  }

  public ExpirationAttributes setEntryTimeToLive(ExpirationAttributes timeToLive) {
    ExpirationAttributes old = this.entryTimeToLive;
    this.entryTimeToLive = timeToLive;
    setHasEntryTimeToLive(true);
    return old;
  }

  public CustomExpiry setCustomEntryTimeToLive(CustomExpiry custom) {
    CustomExpiry old = this.customEntryTimeToLive;
    this.customEntryTimeToLive = custom;
    setHasCustomEntryTimeToLive(true);
    return old;
  }

  public ExpirationAttributes getEntryIdleTimeout() {
    return this.entryIdleTimeout;
  }

  public CustomExpiry getCustomEntryIdleTimeout() {
    return this.customEntryIdleTimeout;
  }

  public ExpirationAttributes setEntryIdleTimeout(ExpirationAttributes idleTimeout) {
    ExpirationAttributes old = this.entryIdleTimeout;
    this.entryIdleTimeout = idleTimeout;
    setHasEntryIdleTimeout(true);
    return old;
  }

  public CustomExpiry setCustomEntryIdleTimeout(CustomExpiry custom) {
    CustomExpiry old = this.customEntryIdleTimeout;
    this.customEntryIdleTimeout = custom;
    setHasCustomEntryIdleTimeout(true);
    return old;
  }

  public MirrorType getMirrorType() {
    if (this.dataPolicy.isNormal() || this.dataPolicy.isPreloaded() || this.dataPolicy.isEmpty()
        || this.dataPolicy.withPartitioning()) {
      return MirrorType.NONE;
    } else if (this.dataPolicy.withReplication()) {
      return MirrorType.KEYS_VALUES;
    } else {
      throw new IllegalStateException(
          String.format("No mirror type corresponds to data policy %s",
              this.dataPolicy));
    }
  }

  public void setMirrorType(MirrorType mirrorType) {
    DataPolicy dp = mirrorType.getDataPolicy();
    if (dp.withReplication()) {
      // requested a mirror type that has replication
      // if current data policy is not replicated change it
      if (!getDataPolicy().withReplication()) {
        setDataPolicy(dp);
      }
    } else {
      // requested a mirror type none;
      // if current data policy is replicated change it
      if (getDataPolicy().withReplication()) {
        setDataPolicy(dp);
      }
    }
  }

  public DataPolicy getDataPolicy() {
    return this.dataPolicy;
  }

  public void setDataPolicy(DataPolicy dataPolicy) {
    this.dataPolicy = dataPolicy;
    setHasDataPolicy(true);
    if (this.dataPolicy.withPartitioning() && !this.hasPartitionAttributes()) {
      setPartitionAttributes((new PartitionAttributesFactory()).create());
      setHasPartitionAttributes(false);
    }
  }

  public void secretlySetDataPolicy(DataPolicy dataPolicy) {
    this.dataPolicy = dataPolicy;
  }

  public Scope getScope() {
    return this.scope;
  }

  public void setScope(Scope scope) {
    this.scope = scope;
    setHasScope(true);
  }

  public CacheListener[] getCacheListeners() {
    CacheListener[] result = new CacheListener[this.cacheListeners.size()];
    this.cacheListeners.toArray(result);
    return result;
  }

  public CacheListener getCacheListener() {
    if (this.cacheListeners.isEmpty()) {
      return null;
    } else if (this.cacheListeners.size() == 1) {
      return (CacheListener) this.cacheListeners.get(0);
    } else {
      throw new IllegalStateException(
          "more than one cache listener exists");
    }
  }

  public void initCacheListeners(CacheListener[] listeners) {
    this.cacheListeners = new ArrayList(Arrays.asList(listeners));
    setHasCacheListeners(true);
  }

  public void addCacheListener(CacheListener listener) {
    this.cacheListeners.add(listener);
    setHasCacheListeners(true);
  }

  public void setCacheListener(CacheListener listener) {
    this.cacheListeners = new ArrayList(1);
    this.cacheListeners.add(listener);
    setHasCacheListeners(true);
  }

  public void initGatewaySenders(Set<String> gatewaySenderIds) {
    this.gatewaySenderIds = new HashSet<String>(gatewaySenderIds);
    setHasGatewaySenderIds(true);
  }

  public void initAsyncEventQueues(Set<String> asyncEventQueues) {
    this.asyncEventQueueIds = new HashSet<String>(asyncEventQueues);
    setHasAsyncEventListeners(true);
  }

  public void addGatewaySenderId(String gatewaySenderId) {
    this.gatewaySenderIds.add(gatewaySenderId);
    setHasGatewaySenderIds(true);
  }

  public void addAsyncEventQueueId(String asyncEventQueueId) {
    this.asyncEventQueueIds.add(asyncEventQueueId);
    setHasAsyncEventListeners(true);
  }

  public int getInitialCapacity() {
    return this.initialCapacity;
  }

  public void setInitialCapacity(int initialCapacity) {
    this.initialCapacity = initialCapacity;
    setHasInitialCapacity(true);
  }

  public float getLoadFactor() {
    return this.loadFactor;
  }

  public void setLoadFactor(float loadFactor) {
    this.loadFactor = loadFactor;
    setHasLoadFactor(true);
  }

  public int getConcurrencyLevel() {
    return this.concurrencyLevel;
  }

  public boolean getConcurrencyChecksEnabled() {
    return this.concurrencyChecksEnabled;
  }

  public void setConcurrencyLevel(int concurrencyLevel) {
    this.concurrencyLevel = concurrencyLevel;
    setHasConcurrencyLevel(true);
  }

  public void setConcurrencyChecksEnabled(boolean enabled) {
    this.concurrencyChecksEnabled = enabled;
    setHasConcurrencyChecksEnabled(true);
  }

  public boolean getStatisticsEnabled() {
    return this.statisticsEnabled;
  }

  public void setStatisticsEnabled(boolean statisticsEnabled) {
    this.statisticsEnabled = statisticsEnabled;
    setHasStatisticsEnabled(true);
  }

  public boolean getIgnoreJTA() {
    return this.ignoreJTA;
  }

  public void setIgnoreJTA(boolean flag) {
    this.ignoreJTA = flag;
    setHasIgnoreJTA(true);
  }

  public boolean isLockGrantor() {
    return this.isLockGrantor;
  }

  public void setLockGrantor(boolean isLockGrantor) {
    this.isLockGrantor = isLockGrantor;
    setHasIsLockGrantor(true);
  }

  public boolean getPersistBackup() {
    return getDataPolicy().withPersistence();
  }

  public void setPersistBackup(boolean persistBackup) {
    if (persistBackup) {
      if (!getDataPolicy().withPersistence()) {
        if (getDataPolicy().withPartitioning()) {
          setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        } else {
          setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        }
      }
    } else {
      // It is less clear what we should do here for backwards compat.
      // If the current data policy is persist then we need to change it
      // otherwise just leave it alone
      if (getDataPolicy().withReplication()) {
        setDataPolicy(DataPolicy.REPLICATE);
      } else if (getDataPolicy().withPartitioning()) {
        setDataPolicy(DataPolicy.PARTITION);
      }
    }
  }

  public boolean getEarlyAck() {
    return this.earlyAck;
  }

  public void setEarlyAck(boolean earlyAck) {
    this.earlyAck = earlyAck;
    setHasEarlyAck(true);
  }

  public boolean getMulticastEnabled() {
    return this.multicastEnabled;
  }

  public void setMulticastEnabled(boolean multicastEnabled) {
    this.multicastEnabled = multicastEnabled;
    setHasMulticastEnabled(true);
  }

  /**
   * @deprecated as of prPersistSprint1
   */
  @Deprecated
  public boolean getPublisher() {
    return this.publisher;
  }

  /**
   * @deprecated as of prPersistSprint1
   */
  @Deprecated
  public void setPublisher(boolean v) {
    // nothing
  }

  public boolean getEnableConflation() { // deprecated in 5.0
    return getEnableSubscriptionConflation();
  }

  public boolean getEnableBridgeConflation() { // deprecated in 5.7
    return getEnableSubscriptionConflation();
  }

  public boolean getEnableSubscriptionConflation() {
    return this.enableSubscriptionConflation;
  }

  public void setEnableBridgeConflation(boolean v) {// deprecated in 5.7
    setEnableSubscriptionConflation(v);
  }

  public void setEnableSubscriptionConflation(boolean v) {
    this.enableSubscriptionConflation = v;
    setHasEnableSubscriptionConflation(true);
  }

  public boolean getEnableAsyncConflation() {
    return this.enableAsyncConflation;
  }

  public void setEnableAsyncConflation(boolean enableAsyncConflation) {
    this.enableAsyncConflation = enableAsyncConflation;
    setHasEnableAsyncConflation(true);
  }

  public void setIndexMaintenanceSynchronous(boolean isSynchronous) {
    this.indexMaintenanceSynchronous = isSynchronous;
    setHasIndexMaintenanceSynchronous(true);
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  public DiskWriteAttributes getDiskWriteAttributes() {
    // not throw exception for mixed API, since it's internal
    return this.diskWriteAttributes;
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  @Deprecated
  public void setDiskWriteAttributes(DiskWriteAttributes attrs) {
    // not throw exception for mixed API, since it's internal
    this.diskWriteAttributes = attrs;
    this.isDiskSynchronous = attrs.isSynchronous();
    setHasDiskWriteAttributes(true);
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  @Deprecated
  public File[] getDiskDirs() {
    // not throw exception for mixed API, since it's internal
    return this.diskDirs;
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  @Deprecated
  public int[] getDiskDirSizes() {
    // not throw exception for mixed API, since it's internal
    return this.diskSizes;
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  @Deprecated
  public void setDiskDirs(File[] diskDirs) {
    // not throw exception for mixed API, since it's internal
    checkIfDirectoriesExist(diskDirs);
    this.diskDirs = diskDirs;
    this.diskSizes = new int[diskDirs.length];
    for (int i = 0; i < diskDirs.length; i++) {
      this.diskSizes[i] = DiskStoreFactory.DEFAULT_DISK_DIR_SIZE;
    }
    setHasDiskDirs(true);
  }

  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  public void setDiskStoreName(String diskStoreName) {
    this.diskStoreName = diskStoreName;
    setHasDiskStoreName(true);
  }

  public boolean isDiskSynchronous() {
    return this.isDiskSynchronous;
    // If DiskWriteAttributes is set, the flag needs to be checked from DiskWriteAttribs
    // TODO: Should we set the correct value in the flag isDiskSynchronous
  }

  public void setDiskSynchronous(boolean isDiskSynchronous) {
    this.isDiskSynchronous = isDiskSynchronous;
    setHasDiskSynchronous(true);
  }

  /**
   * Checks if directories exist
   *
   */
  private void checkIfDirectoriesExist(File[] diskDirs) {
    for (int i = 0; i < diskDirs.length; i++) {
      if (!diskDirs[i].isDirectory()) {
        throw new IllegalArgumentException(
            String.format("%s was not an existing directory.",
                diskDirs[i]));
      }
    }
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  @Deprecated
  public void setDiskDirsAndSize(File[] diskDirs, int[] sizes) {
    // not throw exception for mixed API, since it's internal
    checkIfDirectoriesExist(diskDirs);
    this.diskDirs = diskDirs;
    if (sizes.length != this.diskDirs.length) {
      throw new IllegalArgumentException(
          String.format(
              "Number of diskSizes is %s which is not equal to number of disk Dirs which is %s",

              new Object[] {Integer.valueOf(sizes.length), Integer.valueOf(diskDirs.length)}));
    }
    verifyNonNegativeDirSize(sizes);
    this.diskSizes = sizes;
    this.setHasDiskDirs(true);
  }

  private void verifyNonNegativeDirSize(int[] sizes) {
    for (int i = 0; i < sizes.length; i++) {
      if (sizes[i] < 0) {
        throw new IllegalArgumentException(
            String.format("Dir size cannot be negative : %s",
                Integer.valueOf(sizes[i])));
      }
    }
  }

  public boolean getIndexMaintenanceSynchronous() {
    return this.indexMaintenanceSynchronous;
  }

  /**
   * Sets the id of the region attributes being created
   *
   * @since GemFire 4.1
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Returns the id of the region attributes being created
   *
   * @since GemFire 4.1
   */
  public String getId() {
    return this.id;
  }

  /**
   * Sets the refid of the region attributes being created
   *
   * @since GemFire 4.1
   */
  public void setRefid(String refid) {
    this.refid = refid;
  }

  /**
   * Returns the refid of the region attributes being created
   *
   * @since GemFire 4.1
   */
  public String getRefid() {
    return this.refid;
  }

  /**
   * Causes this region attributes to inherit all of the attributes of its "parent" attributes
   * specified by its <code>refid</code>.
   *
   * @param cache Used to look up named region attributes
   *
   * @throws IllegalStateException If no region attributes named <code>refid</code> exist.
   *
   * @since GemFire 4.1
   */
  void inheritAttributes(Cache cache) {
    inheritAttributes(cache, true);
  }

  void inheritAttributes(Cache cache, boolean setDefaultPool) {
    if (this.refid == null) {
      // No attributes to inherit
      if (setDefaultPool && this.requiresPoolName && !hasPoolName()) {
        String defaultPoolName = null;
        if (cache instanceof GemFireCacheImpl) {
          InternalClientCache gfc = (InternalClientCache) cache;
          if (gfc.getDefaultPool() != null) {
            defaultPoolName = gfc.getDefaultPool().getName();
          }
        } else if (cache instanceof ClientCacheCreation) {
          ClientCacheCreation ccc = (ClientCacheCreation) cache;
          defaultPoolName = ccc.getDefaultPoolName();
        }

        if (defaultPoolName != null) {
          setPoolName(defaultPoolName);
        }
      }
      return;
    }
    RegionAttributes parent = cache.getRegionAttributes(this.refid);
    if (parent == null) {
      throw new IllegalStateException(
          String.format("Cannot reference non-existing region attributes named %s",
              this.refid));
    }

    final boolean parentIsUserSpecified = parent instanceof UserSpecifiedRegionAttributes;
    final UserSpecifiedRegionAttributes parentWithHas;
    if (parentIsUserSpecified) {
      parentWithHas = (UserSpecifiedRegionAttributes) parent;
    } else {
      parentWithHas = null;
    }

    if (parentWithHas != null) {
      if (setDefaultPool && parentWithHas.requiresPoolName) {
        this.requiresPoolName = true;
        if (!hasPoolName()) {
          String defaultPoolName = null;
          if (cache instanceof GemFireCacheImpl) {
            InternalClientCache gfc = (InternalClientCache) cache;
            if (gfc.getDefaultPool() != null) {
              defaultPoolName = gfc.getDefaultPool().getName();
            }
          } else if (cache instanceof ClientCacheCreation) {
            ClientCacheCreation ccc = (ClientCacheCreation) cache;
            defaultPoolName = ccc.getDefaultPoolName();
          }

          if (defaultPoolName != null) {
            setPoolName(defaultPoolName);
          }
        }
      }
    }

    // Inherit attributes that are not overridden
    if (!hasCacheListeners()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasCacheListeners()) {
          initCacheListeners(parent.getCacheListeners());
        }
      } else {
        initCacheListeners(parent.getCacheListeners());
      }
    }

    if (!hasGatewaySenderId()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasGatewaySenderId()) {
          initGatewaySenders(parent.getGatewaySenderIds());
        }
      } else {
        initGatewaySenders(parent.getGatewaySenderIds());
      }
    }

    if (!hasAsyncEventListeners()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasAsyncEventListeners()) {
          initAsyncEventQueues(parent.getAsyncEventQueueIds());
        }
      } else {
        initAsyncEventQueues(parent.getAsyncEventQueueIds());
      }
    }

    if (!hasCacheLoader()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasCacheLoader()) {
          setCacheLoader(parent.getCacheLoader());
        }
      } else {
        setCacheLoader(parent.getCacheLoader());
      }
    }

    if (!hasCacheWriter()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasCacheWriter()) {
          setCacheWriter(parent.getCacheWriter());
        }
      } else {
        setCacheWriter(parent.getCacheWriter());
      }
    }

    if (!hasEntryIdleTimeout()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasEntryIdleTimeout()) {
          setEntryIdleTimeout(parent.getEntryIdleTimeout());
        }
      } else {
        setEntryIdleTimeout(parent.getEntryIdleTimeout());
      }
    }
    if (!hasCustomEntryIdleTimeout()) {
      setCustomEntryIdleTimeout(parent.getCustomEntryIdleTimeout());
    }

    if (!hasEntryTimeToLive()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasEntryTimeToLive()) {
          setEntryTimeToLive(parent.getEntryTimeToLive());
        }
      } else {
        setEntryTimeToLive(parent.getEntryTimeToLive());
      }
    }
    if (!hasCustomEntryTimeToLive()) {
      setCustomEntryTimeToLive(parent.getCustomEntryTimeToLive());
    }

    if (!hasInitialCapacity()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasInitialCapacity()) {
          setInitialCapacity(parent.getInitialCapacity());
        }
      } else {
        setInitialCapacity(parent.getInitialCapacity());
      }
    }

    if (!hasKeyConstraint()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasKeyConstraint()) {
          setKeyConstraint(parent.getKeyConstraint());
        }
      } else {
        setKeyConstraint(parent.getKeyConstraint());
      }
    }

    if (!hasValueConstraint()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasValueConstraint()) {
          setValueConstraint(parent.getValueConstraint());
        }
      } else {
        setValueConstraint(parent.getValueConstraint());
      }
    }

    if (!hasLoadFactor()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasLoadFactor()) {
          setLoadFactor(parent.getLoadFactor());
        }
      } else {
        setLoadFactor(parent.getLoadFactor());
      }
    }

    if (!hasRegionIdleTimeout()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasRegionIdleTimeout()) {
          setRegionIdleTimeout(parent.getRegionIdleTimeout());
        }
      } else {
        setRegionIdleTimeout(parent.getRegionIdleTimeout());
      }
    }

    if (!hasRegionTimeToLive()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasRegionTimeToLive()) {
          setRegionTimeToLive(parent.getRegionTimeToLive());
        }
      } else {
        setRegionTimeToLive(parent.getRegionTimeToLive());
      }
    }

    if (!hasScope()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasScope()) {
          setScope(parent.getScope());
        }
      } else {
        setScope(parent.getScope());
      }
    }

    if (!hasStatisticsEnabled()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasStatisticsEnabled()) {
          setStatisticsEnabled(parent.getStatisticsEnabled());
        }
      } else {
        setStatisticsEnabled(parent.getStatisticsEnabled());
      }
    }

    if (!hasIgnoreJTA()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasIgnoreJTA()) {
          setIgnoreJTA(parent.getIgnoreJTA());
        }
      } else {
        setIgnoreJTA(parent.getIgnoreJTA());
      }
    }

    if (!hasIsLockGrantor()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasIsLockGrantor()) {
          setLockGrantor(parent.isLockGrantor());
        }
      } else {
        setLockGrantor(parent.isLockGrantor());
      }
    }

    if (!hasConcurrencyLevel()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasConcurrencyLevel()) {
          setConcurrencyLevel(parent.getConcurrencyLevel());
        }
      } else {
        setConcurrencyLevel(parent.getConcurrencyLevel());
      }
    }

    if (!hasConcurrencyChecksEnabled()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasConcurrencyChecksEnabled()) {
          setConcurrencyChecksEnabled(parent.getConcurrencyChecksEnabled());
        }
      } else {
        setConcurrencyChecksEnabled(parent.getConcurrencyChecksEnabled());
      }
    }

    // no need to do persistBackup since it is done by dataPolicy

    if (!hasEarlyAck()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasEarlyAck()) {
          setEarlyAck(parent.getEarlyAck());
        }
      } else {
        setEarlyAck(parent.getEarlyAck());
      }
    }

    if (!this.hasEnableSubscriptionConflation()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasEnableSubscriptionConflation()) {
          setEnableSubscriptionConflation(parent.getEnableSubscriptionConflation());
        }
      } else {
        setEnableSubscriptionConflation(parent.getEnableSubscriptionConflation());
      }
    }

    if (!hasPublisher()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasPublisher()) {
          setPublisher(parent.getPublisher());
        }
      } else {
        setPublisher(parent.getPublisher());
      }
    }

    if (!hasEnableAsyncConflation()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasEnableAsyncConflation()) {
          setEnableAsyncConflation(parent.getEnableAsyncConflation());
        }
      } else {
        setEnableAsyncConflation(parent.getEnableAsyncConflation());
      }
    }

    if (!hasMulticastEnabled()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasMulticastEnabled()) {
          setMulticastEnabled(parent.getMulticastEnabled());
        }
      } else {
        setMulticastEnabled(parent.getMulticastEnabled());
      }
    }

    if (!hasDiskWriteAttributes()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasDiskWriteAttributes()) {
          setDiskWriteAttributes(parent.getDiskWriteAttributes());
        }
      } else {
        setDiskWriteAttributes(parent.getDiskWriteAttributes());
      }
    }

    if (!hasDiskDirs()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasDiskDirs()) {
          setDiskDirs(parent.getDiskDirs());
        }
      } else {
        setDiskDirs(parent.getDiskDirs());
      }
    }

    if (!hasIndexMaintenanceSynchronous()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasIndexMaintenanceSynchronous()) {
          setIndexMaintenanceSynchronous(parent.getIndexMaintenanceSynchronous());
        }
      } else {
        setIndexMaintenanceSynchronous(parent.getIndexMaintenanceSynchronous());
      }
    }

    if (!hasPartitionAttributes()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasPartitionAttributes()) {
          setPartitionAttributes(parent.getPartitionAttributes());
        }
      } else {
        setPartitionAttributes(parent.getPartitionAttributes());
      }
    }

    if (!hasSubscriptionAttributes()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasSubscriptionAttributes()) {
          setSubscriptionAttributes(parent.getSubscriptionAttributes());
        }
      } else {
        setSubscriptionAttributes(parent.getSubscriptionAttributes());
      }
    }

    if (!hasDataPolicy()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasDataPolicy()) {
          setDataPolicy(parent.getDataPolicy());
        }
      } else {
        setDataPolicy(parent.getDataPolicy());
      }
    }

    if (!hasEvictionAttributes()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasEvictionAttributes()) {
          setEvictionAttributes(parent.getEvictionAttributes());
        }
      } else {
        setEvictionAttributes(parent.getEvictionAttributes());
      }
    }
    if (!hasPoolName()) {
      setPoolName(parent.getPoolName());
    }

    if (!hasDiskStoreName()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasDiskStoreName()) {
          setDiskStoreName(parent.getDiskStoreName());
        }
      } else {
        setDiskStoreName(parent.getDiskStoreName());
      }
    }
    if (!hasDiskSynchronous()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasDiskSynchronous()) {
          setDiskSynchronous(parent.isDiskSynchronous());
        }
      } else {
        setDiskSynchronous(parent.isDiskSynchronous());
      }
    }

    if (!hasCompressor()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasCompressor()) {
          setCompressor(parent.getCompressor());
        }
      } else {
        setCompressor(parent.getCompressor());
      }
    }
    if (!hasConcurrencyChecksEnabled()) {
      if (parentIsUserSpecified) {
        if (parentWithHas.hasConcurrencyChecksEnabled()) {
          setConcurrencyChecksEnabled(parent.getConcurrencyChecksEnabled());
        }
      } else {
        setConcurrencyChecksEnabled(parent.getConcurrencyChecksEnabled());
      }
    }

    if (!hasMulticastEnabled()) { // bug #38836 - inherit multicast setting
      if (parentIsUserSpecified) {
        if (parentWithHas.hasMulticastEnabled()) {
          setMulticastEnabled(parent.getMulticastEnabled());
        }
      } else {
        setMulticastEnabled(parent.getMulticastEnabled());
      }
    }
  }

  public PartitionAttributes getPartitionAttributes() {
    return this.partitionAttributes;
  }

  public void setPartitionAttributes(PartitionAttributes pa) {
    if (pa != null) {
      if (!hasDataPolicy()) {
        this.setDataPolicy(PartitionedRegionHelper.DEFAULT_DATA_POLICY);
        setHasDataPolicy(false);
      }

      this.partitionAttributes = pa;
      setHasPartitionAttributes(true);
    }
  }

  /**
   * @deprecated this API is scheduled to be removed
   */
  @Deprecated
  public MembershipAttributes getMembershipAttributes() {
    return this.membershipAttributes;
  }

  /**
   * @deprecated this API is scheduled to be removed
   */
  @Deprecated
  public void setMembershipAttributes(MembershipAttributes pa) {
    this.membershipAttributes = pa;
    setHasMembershipAttributes(true);
  }

  /** @since GemFire 5.0 */
  public SubscriptionAttributes getSubscriptionAttributes() {
    return this.subscriptionAttributes;
  }

  /** @since GemFire 5.0 */
  public void setSubscriptionAttributes(SubscriptionAttributes pa) {
    this.subscriptionAttributes = pa;
    setHasSubscriptionAttributes(true);
  }

  public Region getRegion() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  public void setEvictionAttributes(EvictionAttributes ea) {
    this.evictionAttributes = (EvictionAttributesImpl) ea;
    setHasEvictionAttributes(true);
  }

  public EvictionAttributes getEvictionAttributes() {
    return this.evictionAttributes;
  }

  public void setPoolName(String poolName) {
    if ("".equals(poolName)) {
      poolName = null;
    }
    this.poolName = poolName;
    setHasPoolName(true);
  }

  public String getPoolName() {
    return this.poolName;
  }

  public void setCloningEnable(boolean val) {
    this.cloningEnabled = val;
    setHasCloningEnabled(true);
  }

  public boolean getCloningEnabled() {
    return this.cloningEnabled;
  }

  public void setCompressor(Compressor compressor) {
    this.compressor = compressor;
    setHasCompressor(true);

    // Cloning must be enabled when a compressor is set
    if (compressor != null) {
      setCloningEnable(true);
    }
  }

  public Compressor getCompressor() {
    return this.compressor;
  }

  public void setOffHeap(boolean offHeap) {
    this.offHeap = offHeap;
    setHasOffHeap(true);
  }

  public boolean getOffHeap() {
    return this.offHeap;
  }

  public void prepareForValidation() {
    // As of 6.5 we automatically enable stats if expiration is used.
    {
      if (!hasStatisticsEnabled() && !getStatisticsEnabled()
          && (getRegionTimeToLive().getTimeout() != 0 || getRegionIdleTimeout().getTimeout() != 0
              || getEntryTimeToLive().getTimeout() != 0 || getEntryIdleTimeout().getTimeout() != 0
              || getCustomEntryIdleTimeout() != null || getCustomEntryTimeToLive() != null)) {
        // TODO: we could do some more implementation work so that we would
        // not need to enable stats unless entryIdleTimeout is enabled.
        // We need the stats in that case because we need a new type of RegionEntry
        // so we know that last time it was accessed. But for all the others we
        // the stat less region keeps track of everything we need.
        // The only problem is that some places in the code are conditionalized
        // on statisticsEnabled.
        setStatisticsEnabled(true);
      }
      if (getDataPolicy().withReplication() && !getDataPolicy().withPersistence()
          && getScope().isDistributed()) {
        if (getEvictionAttributes().getAction().isLocalDestroy()
            || getEntryIdleTimeout().getAction().isLocal()
            || getEntryTimeToLive().getAction().isLocal()
            || getRegionIdleTimeout().getAction().isLocalInvalidate()
            || getRegionTimeToLive().getAction().isLocalInvalidate()) {
          // new to 6.5; switch to PRELOADED and interest ALL
          setDataPolicy(DataPolicy.PRELOADED);
          setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        }
      }
      // enable concurrency checks for persistent regions
      if (!hasConcurrencyChecksEnabled() && !getConcurrencyChecksEnabled()
          && getDataPolicy().withPersistence()) {
        setConcurrencyChecksEnabled(true);
      }
    }
  }

  public Set<String> getAsyncEventQueueIds() {
    return this.asyncEventQueueIds;
  }

  public Set<String> getGatewaySenderIds() {
    return this.gatewaySenderIds;
  }
}
