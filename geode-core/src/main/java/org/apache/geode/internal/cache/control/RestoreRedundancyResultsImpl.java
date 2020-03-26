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

import static org.apache.geode.internal.cache.control.RestoreRedundancyRegionResult.RedundancyStatus.NO_REDUNDANT_COPIES;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;

public class RestoreRedundancyResultsImpl implements RestoreRedundancyResults, Serializable {
  private static final long serialVersionUID = -2558652878831737255L;
  public static final String NO_REDUNDANT_COPIES_FOR_REGIONS =
      "The following regions have redundancy configured but zero redundant copies: ";
  public static final String REDUNDANCY_NOT_SATISFIED_FOR_REGIONS =
      "Redundancy is partially satisfied for regions: ";
  public static final String REDUNDANCY_SATISFIED_FOR_REGIONS =
      "Redundancy is fully satisfied for regions: ";
  public static final String PRIMARY_TRANSFERS_COMPLETED = "Total primary transfers completed = ";
  public static final String PRIMARY_TRANSFER_TIME = "Total primary transfer time (ms) = ";

  Map<String, RestoreRedundancyRegionResult> regionResults = new HashMap<>();
  Map<String, RestoreRedundancyRegionResult> zeroRedundancyRegions = new HashMap<>();
  Map<String, RestoreRedundancyRegionResult> underRedundancyRegions = new HashMap<>();
  Map<String, RestoreRedundancyRegionResult> satisfiedRedundancyRegions = new HashMap<>();

  private int totalPrimaryTransfersCompleted;
  private long totalPrimaryTransferTime;

  public RestoreRedundancyResultsImpl() {}

  @Override
  public void addRegionResults(RestoreRedundancyResults results) {
    Map<String, RestoreRedundancyRegionResult> regionResults = results.getRegionResults();
    this.regionResults.putAll(regionResults);
    regionResults.values().forEach(this::addToFilteredMaps);

    this.totalPrimaryTransfersCompleted += results.getTotalPrimaryTransfersCompleted();
    this.totalPrimaryTransferTime += results.getTotalPrimaryTransferTime();
  }

  @Override
  public void addPrimaryReassignmentDetails(PartitionRebalanceInfo details) {
    this.totalPrimaryTransfersCompleted += details.getPrimaryTransfersCompleted();
    this.totalPrimaryTransferTime += details.getPrimaryTransferTime();
  }

  @Override
  public void addRegionResult(RestoreRedundancyRegionResult regionResult) {
    regionResults.put(regionResult.getRegionName(), regionResult);
    addToFilteredMaps(regionResult);
  }

  // Adds to the region result to the appropriate map depending on redundancy status
  private void addToFilteredMaps(RestoreRedundancyRegionResult regionResult) {
    switch (regionResult.getStatus()) {
      case NO_REDUNDANT_COPIES:
        zeroRedundancyRegions.put(regionResult.getRegionName(), regionResult);
        break;
      case NOT_SATISFIED:
        underRedundancyRegions.put(regionResult.getRegionName(), regionResult);
        break;
      case SATISFIED:
        satisfiedRedundancyRegions.put(regionResult.getRegionName(), regionResult);
        break;
    }
  }

  @Override
  public RestoreRedundancyResults.Status getStatus() {
    boolean noRedundantCopies = regionResults.values().stream()
        .anyMatch(result -> result.getStatus() == NO_REDUNDANT_COPIES);

    return noRedundantCopies ? Status.FAILURE : Status.SUCCESS;
  }

  @Override
  public String getMessage() {
    List<String> messages = new ArrayList<>();

    // List regions with redundancy configured but no redundant copies first
    if (zeroRedundancyRegions.size() != 0) {
      messages.add(getResultsMessage(zeroRedundancyRegions, NO_REDUNDANT_COPIES_FOR_REGIONS));
    }

    // List failures
    if (underRedundancyRegions.size() != 0) {
      messages.add(getResultsMessage(underRedundancyRegions, REDUNDANCY_NOT_SATISFIED_FOR_REGIONS));
    }

    // List successes
    if (satisfiedRedundancyRegions.size() != 0) {
      messages.add(getResultsMessage(satisfiedRedundancyRegions, REDUNDANCY_SATISFIED_FOR_REGIONS));
    }

    // Add info about primaries
    messages.add(PRIMARY_TRANSFERS_COMPLETED + totalPrimaryTransfersCompleted);
    messages.add(PRIMARY_TRANSFER_TIME + totalPrimaryTransferTime);

    return String.join("\n", messages);
  }

  private String getResultsMessage(Map<String, RestoreRedundancyRegionResult> regionResults,
      String baseMessage) {
    String message = baseMessage + "\n";
    message += regionResults.values().stream().map(RestoreRedundancyRegionResult::toString)
        .collect(Collectors.joining(",\n"));
    return message;
  }

  @Override
  public RestoreRedundancyRegionResult getRegionResult(String regionName) {
    return regionResults.get(regionName);
  }

  @Override
  public Map<String, RestoreRedundancyRegionResult> getZeroRedundancyRegionResults() {
    return zeroRedundancyRegions;
  }

  @Override
  public Map<String, RestoreRedundancyRegionResult> getUnderRedundancyRegionResults() {
    return underRedundancyRegions;
  }

  @Override
  public Map<String, RestoreRedundancyRegionResult> getSatisfiedRedundancyRegionResults() {
    return satisfiedRedundancyRegions;
  }

  @Override
  // This returns the actual backing map
  public Map<String, RestoreRedundancyRegionResult> getRegionResults() {
    return regionResults;
  }

  @Override
  public int getTotalPrimaryTransfersCompleted() {
    return this.totalPrimaryTransfersCompleted;
  }

  @Override
  public long getTotalPrimaryTransferTime() {
    return this.totalPrimaryTransferTime;
  }
}
