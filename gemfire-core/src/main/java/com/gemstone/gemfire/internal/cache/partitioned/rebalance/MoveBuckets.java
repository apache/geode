/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Move;

/**
 * A director to move buckets to improve the load balance of a
 * PR. This is most commonly used as an element of the composite director.
 * @author dsmith
 *
 */
public class MoveBuckets extends RebalanceDirectorAdapter {

  private PartitionedRegionLoadModel model;

  @Override
  public void initialize(PartitionedRegionLoadModel model) {
    this.model = model;
  }

  @Override
  public void membershipChanged(PartitionedRegionLoadModel model) {
    initialize(model);
  }

  @Override
  public boolean nextStep() {
    // TODO Auto-generated method stub
    return moveBuckets();
  }

  /**
   * Move a single bucket from one member to another.
   * @return true if we could move the bucket
   */
  private boolean moveBuckets() {
    Move bestMove = model.findBestBucketMove();

    if (bestMove == null) {
      return false;
    }

    model.moveBucket(bestMove);
    
    return true;
  }

}
