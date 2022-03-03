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

import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.FixedPartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Bucket;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Member;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Move;
import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A director to move primaries to improve the load balance of a fixed partition region. This is
 * most commonly used as an FPRDirector
 *
 */
public class MovePrimariesFPR extends RebalanceDirectorAdapter {
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
    makeFPRPrimaryForThisNode();
    return false;
  }

  /**
   * Move all primary from other to this
   */
  private void makeFPRPrimaryForThisNode() {
    PartitionedRegion partitionedRegion = model.getPartitionedRegion();
    List<FixedPartitionAttributesImpl> FPAs = partitionedRegion.getFixedPartitionAttributesImpl();
    InternalDistributedMember targetId = partitionedRegion.getDistributionManager().getId();
    Member target = model.getMember(targetId);
    for (Bucket bucket : model.getBuckets()) {
      if (bucket != null) {
        for (FixedPartitionAttributesImpl fpa : FPAs) {
          if (fpa.hasBucket(bucket.getId()) && fpa.isPrimary()) {
            Member source = bucket.getPrimary();
            if (source != target) {
              // HACK: In case we don't know who is Primary at this time
              // we just set source as target too for stat purposes

              source = (source == null || source == PartitionedRegionLoadModel.INVALID_MEMBER)
                  ? target : source;
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "PRLM#movePrimariesForFPR: For Bucket#{}, moving primary from source {} to target {}",
                    bucket.getId(), bucket.getPrimary(), target);
              }

              boolean successfulMove = model.movePrimary(new Move(source, target, bucket));
              // We have to move the primary otherwise there is some problem!
              Assert.assertTrue(successfulMove,
                  " Fixed partitioned region not able to move the primary!");
              if (successfulMove) {
                if (logger.isDebugEnabled()) {
                  logger.debug(
                      "PRLM#movePrimariesForFPR: For Bucket#{}, moved primary from source {} to target {}",
                      bucket.getId(), bucket.getPrimary(), target);
                }

                bucket.setPrimary(target, bucket.getPrimaryLoad());
              }
            }
          }
        }
      }
    }
  }
}
