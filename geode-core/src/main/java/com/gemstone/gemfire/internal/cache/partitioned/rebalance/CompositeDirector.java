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
 * The composite director performs a complete rebalance, which can remove over
 * redundant buckets, satisfy redundancy, move buckets, and move primaries.
 * 
 * This is the most commonly used director.
 * 
 * 
 */
public class CompositeDirector extends RebalanceDirectorAdapter {

  private boolean initialRemoveOverRedundancy;
  private boolean initialSatisfyRedundancy;
  private boolean initialMoveBuckets;
  private boolean initialMovePrimaries;
  
  private boolean removeOverRedundancy;
  private boolean satisfyRedundancy;
  private boolean moveBuckets;
  private boolean movePrimaries;
  
  private final RemoveOverRedundancy removeOverRedundancyDirector = new RemoveOverRedundancy();
  private final SatisfyRedundancy satisfyRedundancyDirector = new SatisfyRedundancy();
  private final MovePrimaries movePrimariesDirector = new MovePrimaries();
  private final MoveBuckets moveBucketsDirector = new MoveBuckets();
  
  private PartitionedRegionLoadModel model;

  /**
  * @param removeOverRedundancy true to remove buckets that exceed redundancy
  * levels
  * @param satisfyRedundancy true to satisfy redundancy as part of the operation
  * @param moveBuckets true to move buckets as part of the operation
  * @param movePrimaries true to move primaries as part of the operation
  */
  public CompositeDirector(boolean removeOverRedundancy,
      boolean satisfyRedundancy, boolean moveBuckets,
      boolean movePrimaries) {
    this.initialRemoveOverRedundancy = removeOverRedundancy;
    this.initialSatisfyRedundancy = satisfyRedundancy;
    this.initialMoveBuckets = moveBuckets;
    this.initialMovePrimaries = movePrimaries;
  }
  
  

  @Override
  public boolean isRebalanceNecessary(boolean redundancyImpaired,
      boolean withPersistence) {
    //We can skip a rebalance is redundancy is not impaired and we
    //don't need to move primaries.
    return redundancyImpaired || (initialMovePrimaries && withPersistence);
  }



  @Override
  public void initialize(PartitionedRegionLoadModel model) {
    this.model = model;
    this.removeOverRedundancy = initialRemoveOverRedundancy;
    this.satisfyRedundancy = initialSatisfyRedundancy;
    this.moveBuckets = initialMoveBuckets;
    this.movePrimaries = initialMovePrimaries;
    this.removeOverRedundancyDirector.initialize(model);
    this.satisfyRedundancyDirector.initialize(model);
    this.moveBucketsDirector.initialize(model);
    this.movePrimariesDirector.initialize(model);
  }

  @Override
  public void membershipChanged(PartitionedRegionLoadModel model) {
    initialize(model);
  }

  @Override
  public boolean nextStep() {
    boolean attemptedOperation = false;
    if(this.removeOverRedundancy) {
      attemptedOperation = removeOverRedundancyDirector.nextStep();
    }
    if(!attemptedOperation) {
      this.removeOverRedundancy = false;
    }
    
    if(!attemptedOperation && this.satisfyRedundancy) {
      attemptedOperation = satisfyRedundancyDirector.nextStep();
    }
    if(!attemptedOperation) {
      this.satisfyRedundancy = false;
    }
    
    if(!attemptedOperation && this.moveBuckets) {
      attemptedOperation = moveBucketsDirector.nextStep();
    }
    if(!attemptedOperation) {
      this.moveBuckets = false;
    }
    
    if(!attemptedOperation && this.movePrimaries) {
      attemptedOperation = movePrimariesDirector.nextStep();
    }
    if(!attemptedOperation) {
      this.movePrimaries= false;
    }
      
    return attemptedOperation;
  }

}
