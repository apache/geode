/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.control;

import java.util.Set;

import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;

/**
 * The results of rebalancing <code>Cache</code> resources.
 * 
 * @since 6.0
 */
public interface RebalanceResults {
  
  /**
   * Returns a <code>Set</code> of detailed information about each partitioned
   * region that was rebalanced.
   * 
   * @return a <code>Set</code> of detailed information about each partitioned
   * region that was rebalanced
   */
  public Set<PartitionRebalanceInfo> getPartitionRebalanceDetails();
  
  /**
   * Returns the total time, in milliseconds, that the rebalance operation took.
   * 
   * @return the total time, in milliseconds, that the rebalance operation took
   */
  public long getTotalTime();
  
  /**
   * Returns the total number of buckets created during the rebalance operation.
   * 
   * @return the total number of buckets created during the rebalance operation
   */
  public int getTotalBucketCreatesCompleted();
  
  /**
   * Returns the total size, in bytes, of all of the buckets that were created
   * as part of the rebalance operation.
   * 
   * @return the total size, in bytes, of all of the buckets that were created
   * as part of the rebalance operation
   */
  public long getTotalBucketCreateBytes();
  
  /**
   * Returns the total time, in milliseconds, taken to create buckets.
   * 
   * @return the total time, in milliseconds, taken to create buckets
   */
  public long getTotalBucketCreateTime();
  
  /**
   * Returns the total number of buckets transferred.
   * 
   * @return the total number of buckets transferred
   */
  public int getTotalBucketTransfersCompleted();
  
  /**
   * Returns the total size, in bytes, of buckets that were transferred.
   * 
   * @return the total size, in bytes, of buckets that were transferred
   */
  public long getTotalBucketTransferBytes();
  
  /**
   * Returns the total amount of time, in milliseconds, it took to transfer
   * buckets.
   * 
   * @return the total amount of time, in milliseconds, it took to transfer 
   * buckets
   */
  public long getTotalBucketTransferTime();
  
  /**
   * Returns the total number of primaries that were transferred.
   * 
   * @return the total number of primaries that were transferred
   */
  public int getTotalPrimaryTransfersCompleted();
  
  /**
   * Returns the total time, in milliseconds, spent transferring primaries.
   * 
   * @return the total time, in milliseconds, spent transferring primaries
   */
  public long getTotalPrimaryTransferTime();
}
