/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.BucketRollup;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Member;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Move;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * A director to create redundant copies for buckets in the correct place
 * for a fixed partitioned region. This is most commonly used as part
 * of the FPRDirector.
 *
 */

public class SatisfyRedundancyFPR extends RebalanceDirectorAdapter {

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
    createFPRBucketsForThisNode();
    return false;
  }
  
  public void createFPRBucketsForThisNode() {
    final Map<BucketRollup,Move> moves = new HashMap<BucketRollup,Move>();
    
    for (BucketRollup bucket : model.getLowRedundancyBuckets()) {
      Move move = model.findBestTargetForFPR(bucket, true);
      
      if (move == null
          && !model.enforceUniqueZones()) {
        move = model.findBestTargetForFPR(bucket, false);
      }
      
      if (move != null) {
        moves.put(bucket, move);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Skipping low redundancy bucket {} because no member will accept it", bucket);
        }
      }
    }
    // TODO: This can be done in a thread pool to speed things up as all buckets 
    // are different, there will not be any contention for lock
    for (Map.Entry<BucketRollup, Move> bucketMove : moves.entrySet()) {
      BucketRollup bucket = bucketMove.getKey();
      Move move = bucketMove.getValue();
      Member targetMember = move.getTarget();
      
      model.createRedundantBucket(bucket, targetMember);
    }
    model.waitForOperations();
  }
}
