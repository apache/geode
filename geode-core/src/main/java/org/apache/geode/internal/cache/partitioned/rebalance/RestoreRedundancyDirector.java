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

import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;

/**
 * This director is used when performing a Restore Redundancy operation. It removes
 * over-redundancy and creates redundant buckets as needed, and waits for GII to be completed before
 * returning success, ensuring that actual data redundancy has been achieved
 */
public class RestoreRedundancyDirector extends RebalanceDirectorAdapter {

  private final RemoveOverRedundancy removeOverRedundancyDirector;
  private final SatisfyRedundancy satisfyRedundancyDirector;
  private final MovePrimaries reassignPrimariesDirector;
  private boolean finishedOverRedundancy;
  private boolean finishedSatisfyRedundancy;
  private boolean finishedReassignPrimaries;
  private boolean reassignPrimaries;

  public RestoreRedundancyDirector(boolean shouldReassign) {
    reassignPrimaries = shouldReassign;
    removeOverRedundancyDirector = new RemoveOverRedundancy();
    satisfyRedundancyDirector = new SatisfyRedundancy();
    reassignPrimariesDirector = new MovePrimaries();
  }

  // Constructor to allow mocks to be passed in for testing
  RestoreRedundancyDirector(boolean shouldReassign,
      RemoveOverRedundancy removeOverRedundancyDirector,
      SatisfyRedundancy satisfyRedundancyDirector, MovePrimaries reassignPrimariesDirector) {
    this.reassignPrimaries = shouldReassign;
    this.removeOverRedundancyDirector = removeOverRedundancyDirector;
    this.satisfyRedundancyDirector = satisfyRedundancyDirector;
    this.reassignPrimariesDirector = reassignPrimariesDirector;
  }

  @Override
  public boolean isRebalanceNecessary(boolean redundancyImpaired, boolean withPersistence) {
    // We can skip restoring redundancy if redundancy is not impaired
    return redundancyImpaired || reassignPrimaries;
  }

  @Override
  public void initialize(PartitionedRegionLoadModel model) {
    this.finishedReassignPrimaries = !this.reassignPrimaries;
    this.removeOverRedundancyDirector.initialize(model);
    this.satisfyRedundancyDirector.initialize(model);
    this.reassignPrimariesDirector.initialize(model);
  }

  @Override
  public void membershipChanged(PartitionedRegionLoadModel model) {
    initialize(model);
  }

  @Override
  public boolean nextStep() {
    boolean attemptedOperation = false;
    if (!this.finishedOverRedundancy) {
      attemptedOperation = removeOverRedundancyDirector.nextStep();
    }
    if (!attemptedOperation) {
      this.finishedOverRedundancy = true;
    } else {
      return true;
    }

    if (!this.finishedSatisfyRedundancy) {
      attemptedOperation = satisfyRedundancyDirector.nextStep();
    }
    if (!attemptedOperation) {
      this.finishedSatisfyRedundancy = true;
    } else {
      return true;
    }

    if (!this.finishedReassignPrimaries) {
      attemptedOperation = reassignPrimariesDirector.nextStep();
    }
    if (!attemptedOperation) {
      this.finishedReassignPrimaries = true;
    } else {
      return true;
    }
    return false;
  }
}
