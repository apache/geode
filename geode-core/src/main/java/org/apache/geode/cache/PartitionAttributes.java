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

import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.partition.PartitionListener;

/**
 * 
 * Attributes that define the partitioned character of a Partitioned Region.  
 * This interface allows for the discovery of Partitioned Region attributes using 
 * {@link com.gemstone.gemfire.cache.RegionAttributes#getPartitionAttributes()} as well
 * as the configuration of a Partitioned Region using {@link com.gemstone.gemfire.cache.AttributesFactory#setPartitionAttributes(PartitionAttributes)}.
 * 
 * PartitionAttributes are created using the {@link com.gemstone.gemfire.cache.PartitionAttributesFactory}
 * 
 * The default PartitionAttributes can be determined using {@link com.gemstone.gemfire.cache.PartitionAttributesFactory#create()} with out
 * calling any of its mutator methods e.g.  {@link com.gemstone.gemfire.cache.PartitionAttributesFactory#setLocalMaxMemory(int)}
 *
 * Also see {@link com.gemstone.gemfire.cache.DataPolicy#PARTITION}.
 * 
 * @since GemFire 5.0
 * 
 */
public interface PartitionAttributes<K,V>
{
  /**
   * returns entryTimeToLive in ExpirationAttributes.
   * 
   * @return expirationAttributes
   */
  // public ExpirationAttributes getEntryTimeToLive();

  /**
   * returns entryIdleTimeout in ExpirationAttributes.
   * 
   * @return expirationAttributes
   */
  // public ExpirationAttributes getEntryIdleTimeout();

  /**
   * The number of Backups for an entry in PartitionedRegion. This value should
   * be between 0 and 3 (for a total of 1 to 4 instances of the data).
   * The default value is 0.
   * 
   * @return redundantCopies.
   */
  public int getRedundantCopies();

  /**
   * This method returns the maximum total size of the region in megabytes.
   * @deprecated use getTotalMaxMemory() instead
   * @return total size in megabytes.
   */
  @Deprecated
  public long getTotalSize();
  
  /**
   * This method returns the maximum total size of the region, in megabytes.
   * Default value is Integer.MAX_VALUE.
   * @return maximum size of the partitioned region, in megabytes
   */
  public long getTotalMaxMemory();

  /**
   * This method returns total number of buckets for a PartitionedRegion.
   * Default number of buckets for a PartitionedRegion is 113.
   * 
   * @return total number of buckets for a PartitionedRegion.
   */
  public int getTotalNumBuckets();  

  /**
   * This method returns the maximum amount of local memory that can be used
   * by the Region.
   * By default, a PartitionedRegion can contribute 90% of the maximum memory allocated to a VM.
   */
  public int getLocalMaxMemory();
  
  /**
   * Returns name of the colocated PartitionedRegion's name 
   * @since GemFire 6.0
   */
  public String getColocatedWith();

  /**
   * This method returns local properties.  There are currently no local
   * properties defined that are not also deprecated.
   * 
   * @deprecated use {@link #getLocalMaxMemory()} in GemFire 5.1 and later releases
   * @return localProperties
   */
  @Deprecated
  public Properties getLocalProperties();

  /**
   * This method returns global properties.  There are currently no global
   * properties defined that are not also deprecated.
   * 
   * @deprecated use {@link #getTotalMaxMemory()} and {@link #getTotalNumBuckets()} in GemFire 5.1 and later releases
   * @return globalProperties
   */
  @Deprecated
  public Properties getGlobalProperties();

  /**
   * Returns the PartitionResolver set for custom partitioning
   * @return <code>PartitionResolver</code> for the PartitionedRegion
   * @since GemFire 6.0
   */
  public PartitionResolver<K,V> getPartitionResolver();

  /**
   * Returns the delay in milliseconds that
   * existing members will wait before satisfying redundancy
   * after another member crashes.
   * Default value of recoveryDelay is -1 which indicates that redundancy won't 
   * be recovered if a member crashes.
   * 
   * @since GemFire 6.0
   */
  public long getRecoveryDelay();

  /**
   * Returns the delay in milliseconds that a new
   * member will wait before trying to satisfy redundancy
   * of data hosted on other members.
   * Default value is 0 which is to recover redundancy immediately when a new
   * member is added. 
   * 
   * @since GemFire 6.0
   */
  public long getStartupRecoveryDelay();
  
  /**
   * Returns array of PartitionListener{s} configured on this partitioned region
   * 
   * @see PartitionListener
   * @return PartitionListener configured on this partitioned region
   * @since GemFire 6.5
   */
  public PartitionListener[] getPartitionListeners();

  /**
   * Returns <code>FixedPartitionAttributes</code>'s list of local partitions
   * defined on this Partitioned Region
   * 
   * @since GemFire 6.6
   */
  public List<FixedPartitionAttributes> getFixedPartitionAttributes();
  
}
