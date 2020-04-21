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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.geode.cache.control.RegionRedundancyStatus;
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

  Map<String, RegionRedundancyStatus> zeroRedundancyRegions = new HashMap<>();
  Map<String, RegionRedundancyStatus> underRedundancyRegions = new HashMap<>();
  Map<String, RegionRedundancyStatus> satisfiedRedundancyRegions = new HashMap<>();

  private int totalPrimaryTransfersCompleted;
  private long totalPrimaryTransferTime;

  public RestoreRedundancyResultsImpl() {}

  public void addRegionResults(RestoreRedundancyResults results) {
    this.satisfiedRedundancyRegions.putAll(results.getSatisfiedRedundancyRegionResults());
    this.underRedundancyRegions.putAll(results.getUnderRedundancyRegionResults());
    this.zeroRedundancyRegions.putAll(results.getZeroRedundancyRegionResults());
    this.totalPrimaryTransfersCompleted += results.getTotalPrimaryTransfersCompleted();
    this.totalPrimaryTransferTime += results.getTotalPrimaryTransferTime();
  }

  public void addPrimaryReassignmentDetails(PartitionRebalanceInfo details) {
    this.totalPrimaryTransfersCompleted += details.getPrimaryTransfersCompleted();
    this.totalPrimaryTransferTime += details.getPrimaryTransferTime();
  }

  public void addRegionResult(RegionRedundancyStatus regionResult) {
    addToFilteredMaps(regionResult);
  }

  // Adds to the region result to the appropriate map depending on redundancy status
  private void addToFilteredMaps(RegionRedundancyStatus regionResult) {
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
    boolean fullySatisfied = zeroRedundancyRegions.isEmpty() && underRedundancyRegions.isEmpty();

    return fullySatisfied ? Status.SUCCESS : Status.FAILURE;
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

  private String getResultsMessage(Map<String, RegionRedundancyStatus> regionResults,
      String baseMessage) {
    String message = baseMessage + "\n";
    message += regionResults.values().stream().map(RegionRedundancyStatus::toString)
        .collect(Collectors.joining(",\n"));
    return message;
  }

  @Override
  public RegionRedundancyStatus getRegionResult(String regionName) {
    RegionRedundancyStatus result = satisfiedRedundancyRegions.get(regionName);
    if (result == null) {
      result = underRedundancyRegions.get(regionName);
    }
    if (result == null) {
      result = zeroRedundancyRegions.get(regionName);
    }
    return result;
  }

  @Override
  public Map<String, RegionRedundancyStatus> getZeroRedundancyRegionResults() {
    return zeroRedundancyRegions;
  }

  @Override
  public Map<String, RegionRedundancyStatus> getUnderRedundancyRegionResults() {
    return underRedundancyRegions;
  }

  @Override
  public Map<String, RegionRedundancyStatus> getSatisfiedRedundancyRegionResults() {
    return satisfiedRedundancyRegions;
  }

  @Override
  public Map<String, RegionRedundancyStatus> getRegionResults() {
    Map<String, RegionRedundancyStatus> combinedResults =
        new HashMap<>(satisfiedRedundancyRegions);
    combinedResults.putAll(underRedundancyRegions);
    combinedResults.putAll(zeroRedundancyRegions);

    return combinedResults;
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
