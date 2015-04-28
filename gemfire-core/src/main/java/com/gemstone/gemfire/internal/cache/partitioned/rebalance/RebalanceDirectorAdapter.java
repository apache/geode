/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

/**
 * A base class for rebalance directors that provides some
 * default implementations of methods on rebalance director.
 * 
 * @author dsmith
 *
 */
public abstract class RebalanceDirectorAdapter implements RebalanceDirector {

  @Override
  public boolean isRebalanceNecessary(boolean redundancyImpaired,
      boolean withPersistence) {
    return true;
  }

  @Override
  public void initialize(PartitionedRegionLoadModel model) {
    membershipChanged(model);

  }
}
