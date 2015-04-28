/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author dsmith
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
