/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.partitioned.rebalance;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.partitioned.rebalance.model.BucketRollup;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Member;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Move;
import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A director to create redundant copies for buckets in the correct place for a fixed partitioned
 * region. This is most commonly used as part of the FPRDirector.
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
    final Map<BucketRollup, Move> moves = new HashMap<>();

    for (BucketRollup bucket : model.getLowRedundancyBuckets()) {
      Move move = model.findBestTargetForFPR(bucket, true);

      if (move == null && !model.enforceUniqueZones()) {
        move = model.findBestTargetForFPR(bucket, false);
      }

      if (move != null) {
        moves.put(bucket, move);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Skipping low redundancy bucket {} because no member will accept it",
              bucket);
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
