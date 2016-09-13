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
import org.apache.geode.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Move;
import org.apache.geode.internal.logging.LogService;

/**
 * A director to create redundant copies for buckets that are low in redundancy
 * level. This is most commonly used as an element of the composite director.
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
