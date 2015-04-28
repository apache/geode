/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.partition;

import java.util.Set;

/**
 * The detailed results of rebalancing a partitioned region.
 * 
 * @since 6.0
 */
public interface PartitionRebalanceInfo {
  
  /**
   * Returns the {@link com.gemstone.gemfire.cache.Region#getFullPath() 
   * full path} of the partitioned region that these details describe.
   * 
   * @return the full name of partioned region that these details describe.
   */
  public String getRegionPath();
  
  /**
   * Returns a <code>Set</code> of detailed information about each member that
   * had rebalancable resources at the time that the rebalance started.
   * 
   * @return a <code>Set</code> of detailed information about each member that
   * had rebalancable resources at the time that the rebalance started
   */
  public Set<PartitionMemberInfo> getPartitionMemberDetailsBefore();
  
  /**
   * Returns a <code>Set</code> of detailed information about each member that
   * had rebalancable resources at the time that the rebalance completed.
   * 
   * @return a <code>Set</code> of detailed information about each member that
   * had rebalancable resources at the time that the rebalance completed
   */
  public Set<PartitionMemberInfo> getPartitionMemberDetailsAfter();
  
  /**
   * Returns the time, in milliseconds, that the rebalance operation took for
   * this region.
   * 
   * @return the time, in milliseconds, that the rebalance operation took for
   * this region.
   */
  public long getTime();
  
  /**
   * Returns the number of buckets created during the rebalance operation.
   * 
   * @return the number of buckets created during the rebalance operation
   */
  public int getBucketCreatesCompleted();
  
  /**
   * Returns the size, in bytes, of all of the buckets that were created as
   * part of the rebalance operation.
   * 
   * @return the size, in bytes, of all of the buckets that were created as
   * part of the rebalance operation
   */
  public long getBucketCreateBytes();
  
  /**
   * Returns the time, in milliseconds, taken to create buckets for this region.
   * 
   * @return the time, in milliseconds, taken to create buckets for this region
   */
  public long getBucketCreateTime();
  
  /**
   * Returns the number of buckets removed during the rebalance operation.
   * 
   * @return the number of buckets removed during the rebalance operation
   */
  public int getBucketRemovesCompleted();
  
  /**
   * Returns the size, in bytes, of all of the buckets that were removed as
   * part of the rebalance operation.
   * 
   * @return the size, in bytes, of all of the buckets that were removed as
   * part of the rebalance operation
   */
  public long getBucketRemoveBytes();
  
  /**
   * Returns the time, in milliseconds, taken to remove buckets for this region.
   * 
   * @return the time, in milliseconds, taken to remove buckets for this region
   */
  public long getBucketRemoveTime();
  
  /**
   * Returns the number of buckets transferred for this region.
   * 
   * @return the number of buckets transferred for this region
   */
  public int getBucketTransfersCompleted();
  
  /**
   * Returns the size, in bytes, of buckets that were transferred for this 
   * region.
   * 
   * @return the size, in bytes, of buckets that were transferred for this 
   * region
   */
  public long getBucketTransferBytes();
  
  /**
   * Returns the amount of time, in milliseconds, it took to transfer buckets 
   * for this region.
   * 
   * @return the amount of time, in milliseconds, it took to transfer buckets 
   * for this region
   */
  public long getBucketTransferTime();
  
  /**
   * Returns the number of primaries that were transferred for this region.
   * 
   * @return the number of primaries that were transferred for this region
   */
  public int getPrimaryTransfersCompleted();
  
  /**
   * Returns the time, in milliseconds, spent transferring primaries for this 
   * region.
   * 
   * @return the time, in milliseconds, spent transferring primaries for this 
   * region
   */
  public long getPrimaryTransferTime();
}
