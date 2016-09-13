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
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

/**
 * The FPR director performs rebalancing operations for a fixed partitioned
 * region. There two things a fixed partitioned region does during
 * rebalancing:
 *  - Create redundant buckets in a known location
 *  - move primaries to the appropriate member.
 *  
 *  Note that unlike other directors, this director performs all the work in
 *  a call to nextStep.
 *  
 *
 */
public class FPRDirector extends RebalanceDirectorAdapter {

  private boolean initialSatisfyRedundancy;
  private boolean initialMovePrimaries;
  
  private boolean satisfyRedundancy;
  private boolean movePrimaries;
  
  private final SatisfyRedundancyFPR satisfyRedundancyDirector = new SatisfyRedundancyFPR();
  private final MovePrimariesFPR movePrimariesDirector = new MovePrimariesFPR();
  
  private PartitionedRegionLoadModel model;

  public FPRDirector(boolean initialSatisfyRedundancy, 
      boolean initialMovePrimaries) {
    this.initialSatisfyRedundancy = initialSatisfyRedundancy;
    this.initialMovePrimaries = initialMovePrimaries;
  }

  @Override
  public void initialize(PartitionedRegionLoadModel model) {
    this.model = model;
    this.satisfyRedundancy = initialSatisfyRedundancy;
    this.movePrimaries = initialMovePrimaries;
    this.satisfyRedundancyDirector.initialize(model);
    this.movePrimariesDirector.initialize(model);
  }

  @Override
  public void membershipChanged(PartitionedRegionLoadModel model) {
    initialize(model);
  }

  @Override
  public boolean nextStep() {
    
    if(this.satisfyRedundancy) {
      satisfyRedundancyDirector.nextStep();
    }
    
    if(this.movePrimaries) {
      movePrimariesDirector.nextStep();
    }
    
    return false;
  }

}
