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

import java.util.Properties;

import com.gemstone.gemfire.cache.partition.PartitionListener;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * <p>
 * A factory that creates instances of {@link PartitionAttributes} which are
 * used to create a partitioned {@link Region}. The setter methods follow the
 * self-return idiom so that they can be "chained" together with the
 * {@link #create} method to create
 * {@link PartitionAttributes}. For example:<br>
 * 
 * <pre>
 * PartitionAttributes pa = new PartitionAttributesFactory()
 *  .setRedundantCopies(1)
 *  .setLocalMaxMemory(1240)
 *  .create();
 *     
 * final Region myRegion = new RegionFactory()
 *  .setPartitionAttributes(pa)
 *  .setKeyConstraint(String.class)
 *  .setValueConstraint(ArrayList.class)
 *  .create("myRegion");
 * </pre>
 * 
 * </p>
 * 
 * <p>
 * {@link PartitionAttributes} can also be defined in a declarative fashion using a
 * <a href="package-summary.html#declarative">cache.xml</a> file. Here is an
 * example of how to configure a Partitioned Region named "pRoot" whose 
 * {@link com.gemstone.gemfire.cache.Scope} is Distributed Ack, which maintains
 * a {@link #setRedundantCopies(int) redundant copy} of any given 
 * {@link com.gemstone.gemfire.cache.Region.Entry}, configures a 
 * {@link com.gemstone.gemfire.cache.CacheLoader} implementation, and sets 
 * {@link #setGlobalProperties(Properties) global properties} as well as 
 * {@link #setLocalMaxMemory(int) local max memory to use}.
 * 
 * <pre>
 *    &lt;root-region name=&quot;pRoot&quot;&gt;
 *      &lt;region-attributes scope=&quot;distributed-ack&quot; &gt;
 *        &lt;partition-attributes redundant-copies=&quot;1&quot;,&nbsp;local-max-memory=&quot;1240&quot;/&gt;
 *      &lt;/region-attributes&gt;
 *    &lt;/root-region&gt;
 * </pre>
 * </p>
 * 
 * @see com.gemstone.gemfire.cache.PartitionAttributes
 * @see com.gemstone.gemfire.cache.AttributesFactory#setPartitionAttributes(PartitionAttributes)
 * @since 5.0
 */
public class PartitionAttributesFactory<K,V>
{
  private final PartitionAttributesImpl partitionAttributes = new PartitionAttributesImpl();

  /**
   * @deprecated - please use the {@link #setLocalMaxMemory(int)} method instead.<p>
   * The {@link #setLocalProperties(Properties) local property} name that sets
   * the maximum heap storage a VM contributes to a partitioned Region. When set
   * to zero, the resulting Region reference allows access to the partitioned
   * Region without any consuming any heap storage.
   */
  public static final String LOCAL_MAX_MEMORY_PROPERTY = "LOCAL_MAX_MEMORY";

  /**
   * Default local max memory value in megabytes.  By default each partitioned
   * Region can contribute 90% of the maximum memory allocated to a VM.
   */
  static int computeMaxMem()
  {
    final long maxMemInMegabytes = Runtime.getRuntime().maxMemory() / (1024 * 1024);
    final long maxMemoryToUse = (long) (maxMemInMegabytes * 0.90); 
    int ret;
    if (maxMemoryToUse < Integer.MAX_VALUE) {
      ret = (int) maxMemoryToUse; 
      if (ret < 1) {
        ret = 1;
      }
    } else {
      ret = Integer.MAX_VALUE;
    }
    return ret;
  }
  
  /**
   * The default maximum amount of memory to be used by this region in this
   * process, in megabytes.
   * 
   * @deprecated Use {@link
   *             PartitionAttributesImpl#getLocalMaxMemoryDefault()} instead.
   */
  @Deprecated 
  public static final int LOCAL_MAX_MEMORY_DEFAULT = computeMaxMem();

  /**
   * @deprecated - use {@link #setTotalMaxMemory(long)} instead.<p>
   * The {@link #setGlobalProperties(Properties) global property} name that
   * defines the total maximum size for the partitioned Region.<p>
   * <em>This setting must be the same in all processes using the Region.</em>
   */
  public static final String GLOBAL_MAX_MEMORY_PROPERTY = "GLOBAL_MAX_MEMORY";

  /**
   * Default maximum total size of the region across all processes, in megabytes.
   */
  public static final long GLOBAL_MAX_MEMORY_DEFAULT = Integer.MAX_VALUE;

  /**
   * @deprecated - please use {@link #setTotalNumBuckets(int)} instead.<p>
   * <em>This setting must be the same in all processes using the Region.</em>
   */
  public static final String GLOBAL_MAX_BUCKETS_PROPERTY = "GLOBAL_MAX_BUCKETS"; // "GLOBAL_MAX_BUCKETS";

  /**
   * The default total number of buckets (113).
   */
  public static final int GLOBAL_MAX_BUCKETS_DEFAULT = 113;

  public static final long RECOVERY_DELAY_DEFAULT = -1;

  public static final long STARTUP_RECOVERY_DELAY_DEFAULT = 0;

  /**
   * Creates a new instance of PartitionAttributesFactory ready to create a
   * <code>PartitionAttributes</code> with default settings.
   */
  public PartitionAttributesFactory() {
  }

  /**
   * Creates a new instance of PartitionAttributesFactory ready to create a
   * {@link PartitionAttributes} with the same settings as those in
   * the specified {@link PartitionAttributes}
   * 
   * @param pa  the <code>PartitionAttributes</code> used to initialize this
   *          PartitionAttributesFactory
   */
  public PartitionAttributesFactory(PartitionAttributes pa) {
    this.partitionAttributes.setAll(pa);
  
  }

  // CALLBACKS

  /**
   * Sets the number of extra copies of buckets desired. Extra copies allow for
   * both high availability in the face of VM departure (intended or unintended)
   * and and load balancing read operations.<p>
   * <em>This setting must be the same in all processes using the Region.</em>
   * Default number of redundant copies is 0.
   * 
   * @param redundantCopies
   *          the number of redundant bucket copies, limited to values 0, 1, 2 and 3.
   * 
   * @return this
   */
  public PartitionAttributesFactory<K,V> setRedundantCopies(int redundantCopies)
  {
    this.partitionAttributes.setRedundantCopies(redundantCopies);
    return this;
  }

  /**
   * Sets the cache writer for the next <code>PartitionAttributes</code>
   * created.  <i>Currently unsupported for the early access release.</i>
   * 
   * @param cacheWriter
   *          the cache writer or null if no cache writer
   * @return this
  public PartitionAttributesFactory<K,V> setCacheWriter(CacheWriter cacheWriter)
  {
    this.partitionAttributes.setCacheWriter(cacheWriter);
    return this;
  }
     */

  /**
   * Sets the maximum amount of memory, in megabytes, to be used by the
   * region in this process.  If not set, a default of 
   * 90% of available heap is used.
   */
  public PartitionAttributesFactory<K,V> setLocalMaxMemory(int mb) {
    this.partitionAttributes.setLocalMaxMemory(mb);
    return this;
  }
  
  /**
   * Sets the maximum amount of memory, in megabytes, to be used by the
   * region in all processes.<p>
   * <em>This setting must be the same in all processes using the Region.</em>
   * The default value is Integer.MAX_VALUE.
   */
  public PartitionAttributesFactory<K,V> setTotalMaxMemory(long mb) {
    this.partitionAttributes.setTotalMaxMemory(mb);
    return this;
  }
  
  /**
   * Sets the total number of hash buckets to be used by the region in
   * all processes.
   * <p>
   * <em>This setting must be the same in all processes using the Region.</em>
   * <p>
   * A bucket is the smallest unit of data management in a partitioned region.
   * {@link com.gemstone.gemfire.cache.Region.Entry Entries} are stored in
   * buckets and buckets may move from one VM to another. Buckets may also have
   * copies, depending on {@link #setRedundantCopies(int) redundancy} to provide
   * high availability in the face of VM failure.
   * </p>
   * <p>
   * The number of buckets should be prime and as a rough guide at the least four
   * times the number of partition VMs. However, there is significant overhead
   * to managing a bucket, particularly for higher values of
   * {@link #setRedundantCopies(int) redundancy}.
   * </p>
   * The default number of buckets for a PartitionedRegion is 113.
   */
  public PartitionAttributesFactory<K,V> setTotalNumBuckets(int numBuckets) {
    this.partitionAttributes.setTotalNumBuckets(numBuckets);
    return this;
  }
  /**
   * Sets the <code>PartitionResolver</code> for the PartitionRegion.
   */
  public PartitionAttributesFactory<K,V> setPartitionResolver(
                  PartitionResolver<K,V> resolver) {
    this.partitionAttributes.setPartitionResolver(resolver);
    return this;
  }
  /**
   *  Sets the name of the PartitionRegion with which this newly created
   *  partitioned region is colocated 
   */
  public PartitionAttributesFactory<K,V> setColocatedWith(String colocatedRegionFullPath) {
    this.partitionAttributes.setColocatedWith(colocatedRegionFullPath);
    return this;
  }
  
  /**
   *  Sets the delay in milliseconds that
   * existing members will wait before satisfying redundancy
   * after another member crashes. 
   * Default value is set to -1 which indicates
   * that redundancy will not be recovered after a failure.
   * 
   * @since 6.0
   */
  public PartitionAttributesFactory<K,V> setRecoveryDelay(long recoveryDelay) {
    this.partitionAttributes.setRecoveryDelay(recoveryDelay);
    return this;
  }
  
  /**
   *  Sets the delay in milliseconds that
   * new members will wait before satisfying redundancy. -1 indicates
   * that adding new members will not trigger redundancy recovery.
   * The default (set to 0) is to recover redundancy immediately when a new
   * member is added. 
   * 
   * @since 6.0
   */
  public PartitionAttributesFactory<K,V> setStartupRecoveryDelay(long startupRecoveryDelay) {
    this.partitionAttributes.setStartupRecoveryDelay(startupRecoveryDelay);
    return this;
  }
  
  /**
   * adds a PartitionListener for the partitioned region.
   * 
   * @param listener
   * @return PartitionAttributeFactory
   * @since 6.5
   */
  public PartitionAttributesFactory<K, V> addPartitionListener(
      PartitionListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionAttributesFactory_PARTITION_LISTENER_PARAMETER_WAS_NULL
              .toLocalizedString());
    }
    synchronized (this.partitionAttributes) {
      this.partitionAttributes.addPartitionListener(listener);
    }
    return this;
  }  
  
  /**
   * Sets the <code>Properties</code> for the local instance the partitioned
   * Region. Local properties define how the local instance of the partitioned
   * region and any storage it may provide, behaves.  There are currently no
   * non-deprecated local properties.
   * @deprecated use {@link #setLocalMaxMemory(int)}  in GemFire 5.1 and later releases
   * @param localProps
   * @return PartitionAttributeFactory.
   * 
   */
  @Deprecated
  public PartitionAttributesFactory<K,V> setLocalProperties(Properties localProps)
  {
    if (localProps == null) {
      return this;
    }
    this.partitionAttributes.setLocalProperties(localProps);
 
    return this;
  }

  /**
   * Sets the global <code>Properties</code> for the next <code>PartitionAttributes</code>
   * created.  Global properties define how the entire partitioned Region behaves.
   * <p>
   * Note that global settings must be the same in all processes using the Region.
   * 
   * @deprecated use {@link #setTotalMaxMemory(long)} and {@link #setTotalNumBuckets(int)} in GemFire 5.1 and later releases
   * @param globalProps
   * @return PartitionAttributeFactory.
   * 
   * @see #GLOBAL_MAX_MEMORY_PROPERTY
   */
  @Deprecated
  public PartitionAttributesFactory<K,V> setGlobalProperties(Properties globalProps)
  {
    this.partitionAttributes.setGlobalProperties(globalProps);
    return this;
  }
  
  /**
   * FixedPartitionAttributes defined for this partitioned region is added to
   * PR attributes.
   * 
   * @since 6.6
   */
  public PartitionAttributesFactory<K, V> addFixedPartitionAttributes(
      FixedPartitionAttributes fpa) {
    synchronized (this.partitionAttributes) {
      this.partitionAttributes.addFixedPartitionAttributes(fpa);
      return this;
    }
  }
  
  // EXPIRATION ATTRIBUTES

  /**
   * Sets the idleTimeout expiration attributes for region entries for the next
   * <code>PartitionAttributes</code> created.
   * 
   * @param idleTimeout
   *          the idleTimeout ExpirationAttributes for entries in this region
   * @throws IllegalArgumentException
   *           if idleTimeout is null
   * @return PartitionAttributeFactory
   */
  /*
  public PartitionAttributesFactory<K,V> setEntryIdleTimeout(
      ExpirationAttributes idleTimeout)
  {
    if (idleTimeout == null) {
      throw new IllegalArgumentException(LocalizedStrings.PartitionAttributesFactory_IDLETIMEOUT_MUST_NOT_BE_NULL.toLocalizedString());
    }
    this.partitionAttributes.entryIdleTimeoutExpiration = idleTimeout;
    return this;
  }
  */

  /**
   * Sets the timeToLive expiration attributes for region entries for the next
   * <code>PartitionAttributes</code> created.
   * 
   * @param timeToLive
   *          the timeToLive ExpirationAttributes for entries in this region
   * @throws IllegalArgumentException
   *           if timeToLive is null
   */
  /*
  public PartitionAttributesFactory<K,V> setEntryTimeToLive(
      ExpirationAttributes timeToLive)
  {
    if (timeToLive == null) {
      throw new IllegalArgumentException(LocalizedStrings.PartitionAttributesFactory_TIMETOLIVE_MUST_NOT_BE_NULL.toLocalizedString());
    }
    this.partitionAttributes.entryTimeToLiveExpiration = timeToLive;
    return this;
  }
  */

  /**
   * Enables/Disables local Caching for this node for region entries for the
   * next <code>PartitionAttributes</code> created. Default is true.
   * 
   * @param isLocalCache
   * @return PartitionAttributesFactory
   */
  // TODO: Enable when we have local caching propertly implemented - mthomas
  // 2/32/2006
  // public PartitionAttributesFactory<K,V> enableLocalCaching(boolean isLocalCache)
  // {
  // this.partitionAttributes.localCaching = isLocalCache;
  // return this;
  // }
  // DISTRIBUTION ATTRIBUTES
  // FACTORY METHOD
  /**
   * Creates a <code>PartitionAttributes</code> with the current settings.
   * 
   * @return the newly created <code>PartitionAttributes</code>
   * @throws IllegalStateException
   *           if the current settings violate the <a
   *           href="#compatibility">compatibility rules </a>
   */
  @SuppressWarnings("unchecked")
  public PartitionAttributes<K,V> create()
  {
    this.partitionAttributes.validateAttributes();
//    setDefaults();  [bruce] defaults are set in the PartitionedRegion when the
//                            attributes are applied
    return (PartitionAttributes<K,V>)this.partitionAttributes.clone();
  }

//  /**
//   * This method sets the properties to their default values in preparation
//   * for returning a PartitionAttributes to the user. For example, if it
//   * doesn't have localMaxMemory, it would set it to
//   * LOCAL_MAX_MEMORY_DEFAULT.
//   * 
//   */
//  private void setDefaults()
//  {
//    if (this.partitionAttributes.localProperties
//        .get(PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY) == null) {
//      this.partitionAttributes.setLocalMaxMemory(PartitionAttributesFactory.LOCAL_MAX_MEMORY_DEFAULT);
//    }
//
//  }
}
