/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;



/**
 * A class that is responsible for directing all or
 * part of a rebalance operation. The director receives
 * a model of the current system, and requests changes
 * to the system such as creating buckets, moving buckets, 
 * or moving primaries.
 * 
 * @author dsmith
 *
 */
public interface RebalanceDirector {
  
  /**
   * A check to see if rebalancing is necessary on the given region.
   * This is called before the initialize method is called with the model
   * it is here so that directors can cause the rebalance to early
   * out before gathering extra information.
   * 
   * @param redundancyImpaired true if redundancy is impaired for the PR
   * @param withPersistence - true if persistence is enabled for the PR
   * 
   * @return true if the rebalance should continue.
   */
  public boolean isRebalanceNecessary(boolean redundancyImpaired,
      boolean withPersistence);
  
  /**
   * Initialize the director with the model of the PR. This model
   * is gathered from the peers and passed to this callback before
   * the rebalancing starts. The director can build any initial state it needs
   * from the model at this point.
   */
  public void initialize(PartitionedRegionLoadModel model);

  /**
   * Called when a membership change has invalidated the old model and
   * created a new model. The director should switch to the new model
   * and reinitialize it's state as appropriate.
   */
  public void membershipChanged(PartitionedRegionLoadModel model);
  
  /**
   * Perform one step of the rebalancing process. This step may be to create a
   * redundant bucket, move a bucket, or move a primary.
   * 
   * This method is expected to attempt one small unit of rebalancing work. When
   * the method returns, the system will check to see if the model is still
   * accurate. If true is returned, the PartitionedRegionRebalanceOperation will
   * come back and call nextStep again. It's not required that a move is actually
   * performed during the nextStep call, just that nextStep will return false once
   * there is no more work to be done.
   * 
   * It is expected that nextStep will eventually return false.
   * 
   * @return true we were able to make progress or at least attempt an operation, 
   * false if there is no more rebalancing work to be done.
   */
  public boolean nextStep();
}
