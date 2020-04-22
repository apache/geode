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

import java.time.Duration;
import java.util.Map;

/**
 * A class to collect the results of restore redundancy operations for one or more regions and
 * determine the success of failure of the operation.
 */
public interface RestoreRedundancyResults {

  /**
   * {@link #SUCCESS} is defined as every included region having fully satisfied redundancy.
   * {@link #FAILURE} is defined as at least one region that is configured to have redundant copies
   * having fewer than its configured number of redundant copies.
   * {@link #ERROR} is for cases when the restore redundancy operation was unable to begin or threw
   * an exception.
   */
  enum Status {
    SUCCESS,
    FAILURE,
    ERROR
  }

  /**
   * Returns the {@link Status} of this restore redundancy operation. Possible statuses are
   * {@link Status#SUCCESS}, {@link Status#FAILURE} and {@link Status#ERROR}.
   *
   * @return The {@link Status} of this restore redundancy operation.
   */
  Status getStatus();

  /**
   * Returns a message describing the results of this restore redundancy operation.
   *
   * @return A {@link String} describing the results of this restore redundancy operation.
   */
  String getMessage();

  /**
   * Returns the {@link RegionRedundancyStatus} for a specific region or null if that region
   * is not present in this {@link RestoreRedundancyResults}.
   *
   * @param regionName The region to which the {@link RegionRedundancyStatus} to be returned
   *        belongs.
   * @return A {@link RegionRedundancyStatus} for the specified region or null if that region is not
   *         present in this {@link RestoreRedundancyResults}.
   */
  RegionRedundancyStatus getRegionResult(String regionName);

  /**
   * Returns all the {@link RegionRedundancyStatus RegionRedundancyStatuses} for regions with
   * configured redundancy but zero actual redundant copies.
   *
   * @return A {@link Map} of {@link String} region name to {@link RegionRedundancyStatus} for every
   *         region contained in this {@link RestoreRedundancyResults} with configured redundancy
   *         but zero actual redundant copies.
   */
  Map<String, RegionRedundancyStatus> getZeroRedundancyRegionResults();

  /**
   * Returns all the {@link RegionRedundancyStatus RegionRedundancyStatuses} for regions with with
   * at least one redundant copy, but fewer than the configured number of redundant copies.
   *
   * @return A {@link Map} of {@link String} region name to {@link RegionRedundancyStatus} for every
   *         region contained in this {@link RestoreRedundancyResults} with at least one redundant
   *         copy, but fewer than the configured number of redundant copies.
   */
  Map<String, RegionRedundancyStatus> getUnderRedundancyRegionResults();

  /**
   * Returns all the {@link RegionRedundancyStatus RegionRedundancyStatuses} for regions with
   * redundancy satisfied.
   *
   * @return A {@link Map} of {@link String} region name to {@link RegionRedundancyStatus} for every
   *         region contained in this {@link RestoreRedundancyResults} with redundancy satisfied.
   */
  Map<String, RegionRedundancyStatus> getSatisfiedRedundancyRegionResults();

  /**
   * Returns all the {@link RegionRedundancyStatus RegionRedundancyStatuses} contained in this
   * {@link RestoreRedundancyResults}. This method may return the actual backing map depending on
   * implementation.
   *
   * @return A {@link Map} of {@link String} region name to {@link RegionRedundancyStatus} for every
   *         region contained in this {@link RestoreRedundancyResults}.
   */
  Map<String, RegionRedundancyStatus> getRegionResults();

  /**
   * Returns the total number of primaries that were transferred as part of the restore redundancy
   * operations.
   *
   * @return the total number of primaries that were transferred
   */
  int getTotalPrimaryTransfersCompleted();

  /**
   * Returns the total time spent transferring primaries as part of the restore redundancy
   * operations.
   *
   * @return A {@link Duration} representing the total time spent transferring primaries
   */
  Duration getTotalPrimaryTransferTime();
}
