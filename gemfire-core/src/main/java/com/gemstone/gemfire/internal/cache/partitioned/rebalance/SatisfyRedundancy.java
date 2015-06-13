/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.BucketRollup;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Move;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * A director to create redundant copies for buckets that are low in redundancy
 * level. This is most commonly used as an element of the composite director.
 * @author dsmith
 *
 */
public class SatisfyRedundancy extends RebalanceDirectorAdapter {
  
  private static final Logger logger = LogService.getLogger();
  
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
    if(satisfyRedundancy()) {
      return true;
    }  else {
      model.waitForOperations();
      return satisfyRedundancy();
    }
  }

  /**
   * Try to satisfy redundancy for a single bucket.
   * @return true if we actually created a bucket somewhere.
   */
  private boolean satisfyRedundancy() {
    Move bestMove = null;
    BucketRollup first = null;
    while(bestMove == null) {
      if(model.getLowRedundancyBuckets().isEmpty()) {
        return false;
      } 

      first = model.getLowRedundancyBuckets().first();
      bestMove = model.findBestTarget(first, true);
      if (bestMove == null
          && !model.enforceUniqueZones()) {
        bestMove = model.findBestTarget(first, false);
      }
      if(bestMove == null) {
        if(logger.isDebugEnabled()) {
          logger.debug("Skipping low redundancy bucket {} because no member will accept it", first);
        }
        model.ignoreLowRedundancyBucket(first);
      }
    }
    
    model.createRedundantBucket(first, bestMove.getTarget());
    
    return true;
  }

  
  
}
