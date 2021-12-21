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
 * The composite director performs a complete rebalance, which can remove over redundant buckets,
 * satisfy redundancy, move buckets, and move primaries.
 *
 * This is the most commonly used director.
 *
 *
 */
public class CompositeDirector extends RebalanceDirectorAdapter {

  private final boolean initialRemoveOverRedundancy;
  private final boolean initialSatisfyRedundancy;
  private final boolean initialMoveBuckets;
  private final boolean initialMovePrimaries;

  private boolean removeOverRedundancy;
  private boolean satisfyRedundancy;
  private boolean moveBuckets;
  private boolean movePrimaries;

  private boolean isRestoreRedundancy;

  private final RemoveOverRedundancy removeOverRedundancyDirector = new RemoveOverRedundancy();
  private final SatisfyRedundancy satisfyRedundancyDirector = new SatisfyRedundancy();
  private final MovePrimaries movePrimariesDirector = new MovePrimaries();
  private final MoveBuckets moveBucketsDirector = new MoveBuckets();

  /**
   * @param removeOverRedundancy true to remove buckets that exceed redundancy levels
   * @param satisfyRedundancy true to satisfy redundancy as part of the operation
   * @param moveBuckets true to move buckets as part of the operation
   * @param movePrimaries true to move primaries as part of the operation
   */
  public CompositeDirector(boolean removeOverRedundancy, boolean satisfyRedundancy,
      boolean moveBuckets, boolean movePrimaries) {
    initialRemoveOverRedundancy = removeOverRedundancy;
    initialSatisfyRedundancy = satisfyRedundancy;
    initialMoveBuckets = moveBuckets;
    initialMovePrimaries = movePrimaries;
  }

  @Override
  public boolean isRebalanceNecessary(boolean redundancyImpaired, boolean withPersistence) {
    // Restoring redundancy does not require that persistence is enabled to consider the operation
    // necessary
    if (isRestoreRedundancy) {
      return redundancyImpaired || initialMovePrimaries;
    }
    // We can skip a rebalance is redundancy is not impaired and we
    // don't need to move primaries.
    return redundancyImpaired || (initialMovePrimaries && withPersistence);
  }

  @Override
  public void initialize(PartitionedRegionLoadModel model) {
    removeOverRedundancy = initialRemoveOverRedundancy;
    satisfyRedundancy = initialSatisfyRedundancy;
    moveBuckets = initialMoveBuckets;
    movePrimaries = initialMovePrimaries;
    removeOverRedundancyDirector.initialize(model);
    satisfyRedundancyDirector.initialize(model);
    moveBucketsDirector.initialize(model);
    movePrimariesDirector.initialize(model);
  }

  @Override
  public void membershipChanged(PartitionedRegionLoadModel model) {
    initialize(model);
  }

  @Override
  public boolean nextStep() {
    boolean attemptedOperation = false;
    if (removeOverRedundancy) {
      attemptedOperation = removeOverRedundancyDirector.nextStep();
    }
    if (!attemptedOperation) {
      removeOverRedundancy = false;
    }

    if (!attemptedOperation && satisfyRedundancy) {
      attemptedOperation = satisfyRedundancyDirector.nextStep();
    }
    if (!attemptedOperation) {
      satisfyRedundancy = false;
    }

    if (!attemptedOperation && moveBuckets) {
      attemptedOperation = moveBucketsDirector.nextStep();
    }
    if (!attemptedOperation) {
      moveBuckets = false;
    }

    if (!attemptedOperation && movePrimaries) {
      attemptedOperation = movePrimariesDirector.nextStep();
    }
    if (!attemptedOperation) {
      movePrimaries = false;
    }

    return attemptedOperation;
  }

  public boolean isRestoreRedundancy() {
    return isRestoreRedundancy;
  }

  public void setIsRestoreRedundancy(boolean restoreRedundancy) {
    isRestoreRedundancy = restoreRedundancy;
  }
}
