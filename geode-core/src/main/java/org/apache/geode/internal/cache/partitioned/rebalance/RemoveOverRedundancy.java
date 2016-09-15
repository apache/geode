/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.partitioned.rebalance;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.BucketRollup;
import org.apache.geode.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Member;
import org.apache.geode.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Move;
import org.apache.geode.internal.logging.LogService;

/**
 * A director to remove copies of buckets when the bucket exceeds the redundancy
 * level. This is most commonly used as an element of the composite director.
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
