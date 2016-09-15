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

import org.apache.geode.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Move;

/**
 * A director to move primaries to improve the load balance of a
 * PR. This is most commonly used as an element of the composite director.
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
