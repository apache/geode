/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import java.util.Set;

import com.gemstone.gemfire.internal.cache.BucketAdvisor;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisee;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.persistence.PersistenceAdvisor;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Represents a storage or meta-data container for a 
 * <code>PartitionedRegion</code>.
 * @author Kirk Lund
 */
public interface Bucket extends CacheDistributionAdvisee {
  
  /**
   * Returns the distribution and metadata <code>BucketAdvisor</code> for this
   * bucket.
   */
  public BucketAdvisor getBucketAdvisor();
  
  /** 
   * Returns the serial number which identifies the static order in which this 
   * bucket was created in relation to other regions or other instances of 
   * this bucket during the life of this JVM.
   */
  public int getSerialNumber();
  
  /**
   * Returns true if this member is the primary for this bucket.
   */
  public boolean isPrimary();
  
  /**
   * Returns true if this bucket is currently backed by a {@link 
   * com.gemstone.gemfire.internal.cache.BucketRegion}.
   */
  public boolean isHosting();
  
  /**
   * Returns the bucket id used to uniquely identify the bucket in its
   * partitioned region
   * @return the unique identity of the bucket
   */
  public int getId();
  
  /**
   * Report members that are currently hosting the bucket 
   * @return set of members 
   * @since gemfire59poc
   */
  public Set/*InternalDistributedMembers*/ getBucketOwners();

  public PersistenceAdvisor getPersistenceAdvisor();

  public DiskRegion getDiskRegion();

  /**
   * Returns the parent {@link PartitionedRegion} of this bucket.
   */
  public PartitionedRegion getPartitionedRegion();
}
