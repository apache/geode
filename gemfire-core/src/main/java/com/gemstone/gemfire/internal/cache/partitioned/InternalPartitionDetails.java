/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;

/**
 * Provides load and bucket level details for internal use. Extends 
 * {@link com.gemstone.gemfire.cache.partition.PartitionMemberInfo}.
 * 
 * @author Kirk Lund
 */
public interface InternalPartitionDetails 
extends PartitionMemberInfo, Comparable<InternalPartitionDetails> {

  /**
   * Returns the load for the partitioned region.
   * 
   * @return the load for the partitioned region
   */
  public PRLoad getPRLoad();
  
  /**
   * Returns the size of the bucket in bytes.
   * 
   * @param bucketId the identity of the bucket from 0 to number of buckets -1
   * @return the size of the bucket in bytes
   */
  public long getBucketSize(int bucketId);
  
}
