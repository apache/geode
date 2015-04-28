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
 * A director to move primaries to improve the load balance of a
 * PR. This is most commonly used as an element of the composite director.
 * @author dsmith
 *
 */
public class MovePrimaries extends RebalanceDirectorAdapter {

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
    return movePrimaries();
  }
  
  /**
   * Move a single primary from one member to another
   * @return if we are able to move a primary.
   */
  private boolean movePrimaries() {
    Move bestMove = model.findBestPrimaryMove();

    if (bestMove == null) {
      return false;
    }

    model.movePrimary(bestMove);
    
    return true;
  }

}
