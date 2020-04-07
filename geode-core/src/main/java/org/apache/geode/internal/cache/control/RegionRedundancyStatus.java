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
package org.apache.geode.internal.cache.control;

import java.io.Serializable;

import org.apache.geode.internal.cache.PartitionedRegion;

/**
 * Used to calculate and store the redundancy status for a {@link PartitionedRegion}.
 */
public class RegionRedundancyStatus implements Serializable {
  private static final long serialVersionUID = 3407539362166634316L;

  public static final String OUTPUT_STRING =
      "%s redundancy status: %s. Desired redundancy is %s and actual redundancy is %s.";

  /**
   * The name of the region used to create this object.
   */
  private final String regionName;

  /**
   * The configured redundancy of the region used to create this object.
   */
  private final int desiredRedundancy;

  /**
   * The actual redundancy of the region used to create this object at time of creation.
   */
  private final int actualRedundancy;

  /**
   * The {@link RedundancyStatus} of the region used to create this object at time of creation.
   */
  private final RedundancyStatus status;

  /**
   * The redundancy status of the region used to create this object at time of creation.
   * {@link #SATISFIED} if every bucket in the region has the configured number of redundant copies
   * {@link #NOT_SATISFIED} if at least one bucket in the region has less than the configured number
   * of redundant copies
   * {@link #NO_REDUNDANT_COPIES} if at least one bucket in the region has zero redundant copies and
   * the region is not configured for zero redundancy
   */
  enum RedundancyStatus {
    SATISFIED,
    NOT_SATISFIED,
    NO_REDUNDANT_COPIES
  }

  public RegionRedundancyStatus(PartitionedRegion region) {
    regionName = region.getName();
    desiredRedundancy = region.getRedundantCopies();
    actualRedundancy = calculateLowestRedundancy(region);
    status = determineStatus(desiredRedundancy, actualRedundancy);
  }

  public String getRegionName() {
    return regionName;
  }

  public int getActualRedundancy() {
    return actualRedundancy;
  }

  public RedundancyStatus getStatus() {
    return status;
  }

  /**
   * Calculates the lowest redundancy for any bucket in the region.
   *
   * @param region The region for which the lowest redundancy should be calculated.
   * @return The redundancy of the least redundant bucket in the region.
   */
  private int calculateLowestRedundancy(PartitionedRegion region) {
    int numBuckets = region.getPartitionAttributes().getTotalNumBuckets();
    int minRedundancy = Integer.MAX_VALUE;
    for (int i = 0; i < numBuckets; i++) {
      int bucketRedundancy = region.getRegionAdvisor().getBucketRedundancy(i);
      if (bucketRedundancy < minRedundancy) {
        minRedundancy = bucketRedundancy;
      }
    }
    return minRedundancy;
  }

  /**
   * Determines the {@link RedundancyStatus} for the region. If redundancy is not configured (i.e.
   * configured redundancy = 0), this always returns {@link RedundancyStatus#SATISFIED}.
   *
   * @param desiredRedundancy The configured redundancy of the region.
   * @param actualRedundancy The actual redundancy of the region.
   * @return The {@link RedundancyStatus} for the region.
   */
  private RedundancyStatus determineStatus(int desiredRedundancy, int actualRedundancy) {
    boolean zeroRedundancy = desiredRedundancy == 0;
    if (actualRedundancy == 0) {
      return zeroRedundancy ? RedundancyStatus.SATISFIED : RedundancyStatus.NO_REDUNDANT_COPIES;
    }
    return desiredRedundancy == actualRedundancy ? RedundancyStatus.SATISFIED
        : RedundancyStatus.NOT_SATISFIED;
  }

  @Override
  public String toString() {
    return String.format(OUTPUT_STRING, regionName, status.name(), desiredRedundancy,
        actualRedundancy);
  }
}
