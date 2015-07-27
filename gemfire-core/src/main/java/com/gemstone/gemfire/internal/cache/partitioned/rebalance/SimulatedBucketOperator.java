/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import java.util.Map;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * A BucketOperator which does nothing. Used for simulations.
 * @author dsmith
 *
 */
public class SimulatedBucketOperator implements BucketOperator {

  public void createRedundantBucket(
      InternalDistributedMember targetMember, int i, Map<String, Long> colocatedRegionBytes, 
      BucketOperator.Completion completion) {
    completion.onSuccess();
  }
  
  public boolean moveBucket(InternalDistributedMember source,
      InternalDistributedMember target, int id,
      Map<String, Long> colocatedRegionBytes) {
    return true;
  }

  public boolean movePrimary(InternalDistributedMember source,
      InternalDistributedMember target, int bucketId) {
    return true;
  }

  public boolean removeBucket(InternalDistributedMember memberId, int id,
      Map<String, Long> colocatedRegionSizes) {
    return true;
  }

  @Override
  public void waitForOperations() {
  }
}