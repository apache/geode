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
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Member;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Move;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * A director to remove copies of buckets when the bucket exceeds the redundancy
 * level. This is most commonly used as an element of the composite director.
 * @author dsmith
 *
 */
public class RemoveOverRedundancy extends RebalanceDirectorAdapter {
  
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
    return removeOverRedundancy();
  }

  /**
   * Remove copies of buckets that have more
   * than the expected number of redundant copies.
   */
  private boolean removeOverRedundancy() {
    Move bestMove = null;
    BucketRollup first = null;
    while(bestMove == null) {
      if(model.getOverRedundancyBuckets().isEmpty()) {
        return false;
      } 

      first = model.getOverRedundancyBuckets().first();
      bestMove = model.findBestRemove(first);
      if(bestMove == null) {
        if(logger.isDebugEnabled()) {
          logger.debug("Skipping overredundancy bucket {} because couldn't find a member to remove from?", first);
        }
        model.ignoreOverRedundancyBucket(first);
      }
    }
    Member targetMember = bestMove.getTarget();
    
    model.remoteOverRedundancyBucket(first, targetMember);
    
    return true;
  }
}
