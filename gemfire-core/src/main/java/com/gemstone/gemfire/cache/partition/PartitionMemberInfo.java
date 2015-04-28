/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.partition;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Describes a member that has been configured to provide storage space for
 * a partitioned region.
 * <p>
 * This is an immutable snapshot of the details.
 * 
 * @since 6.0
 */
public interface PartitionMemberInfo {
  
  /**
   * Identifies the member for which these details pertain to.
   * 
   * @return the member for which these details pertain to
   */
  public DistributedMember getDistributedMember();
  
  /**
   * Returns the {@link 
   * com.gemstone.gemfire.cache.PartitionAttributes#getLocalMaxMemory() max 
   * memory} in bytes that the member was configured to provide for storage
   * of data for the partitioned region.
   * 
   * @return the max memory in bytes that the member was configured to
   * provide for storage
   */
  public long getConfiguredMaxMemory(); // in bytes
  
  /**
   * The total size in bytes of memory being used by the member for storage
   * of actual data in the partitioned region.
   *  
   * @return size in bytes of memory being used by the member for storage
   */
  public long getSize(); // in bytes
  
  /**
   * Returns the number of buckets hosted within the member's partition space
   * for the partitioned region.
   * 
   * @return the number of buckets hosted within the member
   */
  public int getBucketCount();
  
  /**
   * The number of hosted buckets for which the member is hosting the primary
   * copy. Other copies are known as redundant backup copies.
   * 
   * @return the number of hosted buckets for which the member is hosting the 
   * primary copy
   */
  public int getPrimaryCount();
}
