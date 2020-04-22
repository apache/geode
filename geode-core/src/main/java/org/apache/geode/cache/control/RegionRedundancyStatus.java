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
package org.apache.geode.cache.control;

/**
 * Used to calculate and store a snapshot of the redundancy status for a partitioned region.
 */
public interface RegionRedundancyStatus {

  /**
   * The redundancy status of the region used to create this object at time of creation.
   * {@link #SATISFIED} if every bucket in the region has the configured number of redundant copies.
   * {@link #NOT_SATISFIED} if at least one bucket in the region has less than the configured number
   * of redundant copies.
   * {@link #NO_REDUNDANT_COPIES} if at least one bucket in the region has zero redundant copies and
   * the region is not configured for zero redundancy.
   */
  enum RedundancyStatus {
    SATISFIED,
    NOT_SATISFIED,
    NO_REDUNDANT_COPIES
  }

  /**
   * Returns the name of the region used to create this RegionRedundancyStatus.
   *
   * @return The name of the region used to create this RegionRedundancyStatus.
   */
  String getRegionName();

  /**
   * Returns the configured redundancy level for the region used to create this
   * RegionRedundancyStatus.
   *
   * @return The configured redundancy level for the region used to create this
   *         RegionRedundancyStatus.
   */
  int getConfiguredRedundancy();

  /**
   * Returns the number of redundant copies for all buckets in the region used to create this
   * RegionRedundancyStatus, at the time of creation. If some buckets have fewer redundant copies
   * than others, the lower number is returned.
   *
   * @return The number of redundant copies for all buckets in the region used to create this
   *         RegionRedundancyStatus, at the time of creation.
   */
  int getActualRedundancy();

  /**
   * Returns the {@link RedundancyStatus} for the region used to create this RegionRedundancyStatus
   * at the time of creation.
   *
   * @return The {@link RedundancyStatus} for the region used to create this RegionRedundancyStatus.
   */
  RedundancyStatus getStatus();
}
