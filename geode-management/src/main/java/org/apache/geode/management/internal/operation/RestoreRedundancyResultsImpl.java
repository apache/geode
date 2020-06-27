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
package org.apache.geode.management.internal.operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.geode.management.runtime.RegionRedundancyStatus;
import org.apache.geode.management.runtime.RestoreRedundancyResults;

/**
 * result object used by the cms that only needs to be json serializable
 */
public class RestoreRedundancyResultsImpl implements RestoreRedundancyResults {
  public static final String NO_REDUNDANT_COPIES_FOR_REGIONS =
      "The following regions have redundancy configured but zero redundant copies: ";
  public static final String REDUNDANCY_NOT_SATISFIED_FOR_REGIONS =
      "Redundancy is partially satisfied for regions: ";
  public static final String REDUNDANCY_SATISFIED_FOR_REGIONS =
      "Redundancy is fully satisfied for regions: ";
  public static final String PRIMARY_TRANSFERS_COMPLETED = "Total primary transfers completed = ";
  public static final String PRIMARY_TRANSFER_TIME = "Total primary transfer time (ms) = ";
  private static final long serialVersionUID = -1174735246756963521L;

  protected Map<String, RegionRedundancyStatus> zeroRedundancyRegionsResults = new HashMap<>();
  protected Map<String, RegionRedundancyStatus> underRedundancyRegionsResults = new HashMap<>();
  protected Map<String, RegionRedundancyStatus> satisfiedRedundancyRegionsResults = new HashMap<>();

  protected int totalPrimaryTransfersCompleted;
  protected long totalPrimaryTransferTime = 0;
  protected boolean success = true;
  protected String statusMessage;
  protected final List<String> includedRegionsWithNoMembers = new ArrayList<>();
  private RegionRedundancyStatus regionResult;

  public void setZeroRedundancyRegionsResults(
      Map<String, RegionRedundancyStatus> zeroRedundancyRegionsResults) {
    this.zeroRedundancyRegionsResults = zeroRedundancyRegionsResults;
  }

  public void setUnderRedundancyRegionsResults(
      Map<String, RegionRedundancyStatus> underRedundancyRegionsResults) {
    this.underRedundancyRegionsResults = underRedundancyRegionsResults;
  }

  public void setSatisfiedRedundancyRegionsResults(
      Map<String, RegionRedundancyStatus> satisfiedRedundancyRegionsResults) {
    this.satisfiedRedundancyRegionsResults = satisfiedRedundancyRegionsResults;
  }

  public void setTotalPrimaryTransfersCompleted(int totalPrimaryTransfersCompleted) {
    this.totalPrimaryTransfersCompleted = totalPrimaryTransfersCompleted;
  }

  public void setTotalPrimaryTransferTime(long totalPrimaryTransferTime) {
    this.totalPrimaryTransferTime = totalPrimaryTransferTime;
  }

  public void setRegionResult(RegionRedundancyStatus regionResult) {
    this.regionResult = regionResult;
  }


  public RestoreRedundancyResultsImpl() {}


  public void addRegionResults(RestoreRedundancyResults results) {
    satisfiedRedundancyRegionsResults.putAll(results.getSatisfiedRedundancyRegionResults());
    underRedundancyRegionsResults.putAll(results.getUnderRedundancyRegionResults());
    zeroRedundancyRegionsResults.putAll(results.getZeroRedundancyRegionResults());
    totalPrimaryTransfersCompleted += results.getTotalPrimaryTransfersCompleted();
    totalPrimaryTransferTime += results.getTotalPrimaryTransferTime();
  }

  public void addRegionResult(RegionRedundancyStatus regionResult) {
    addToFilteredMaps(regionResult);
  }

  // Adds to the region result to the appropriate map depending on redundancy status
  private void addToFilteredMaps(RegionRedundancyStatus regionResult) {
    switch (regionResult.getStatus()) {
      case NO_REDUNDANT_COPIES:
        zeroRedundancyRegionsResults.put(regionResult.getRegionName(), regionResult);
        break;
      case NOT_SATISFIED:
        underRedundancyRegionsResults.put(regionResult.getRegionName(), regionResult);
        break;
      case SATISFIED:
        satisfiedRedundancyRegionsResults.put(regionResult.getRegionName(), regionResult);
        break;
    }
  }

  @Override
  public Status getRegionOperationStatus() {
    boolean fullySatisfied =
        zeroRedundancyRegionsResults.isEmpty() && underRedundancyRegionsResults.isEmpty();
    return fullySatisfied ? Status.SUCCESS : Status.FAILURE;
  }

  @Override
  public String getRegionOperationMessage() {
    List<String> messages = new ArrayList<>();

    // List regions with redundancy configured but no redundant copies first
    if (!zeroRedundancyRegionsResults.isEmpty()) {
      messages
          .add(getResultsMessage(zeroRedundancyRegionsResults, NO_REDUNDANT_COPIES_FOR_REGIONS));
    }

    // List failures
    if (!underRedundancyRegionsResults.isEmpty()) {
      messages.add(
          getResultsMessage(underRedundancyRegionsResults, REDUNDANCY_NOT_SATISFIED_FOR_REGIONS));
    }

    // List successes
    if (!satisfiedRedundancyRegionsResults.isEmpty()) {
      messages.add(
          getResultsMessage(satisfiedRedundancyRegionsResults, REDUNDANCY_SATISFIED_FOR_REGIONS));
    }

    // Add info about primaries
    messages.add(PRIMARY_TRANSFERS_COMPLETED + totalPrimaryTransfersCompleted);
    messages.add(PRIMARY_TRANSFER_TIME + totalPrimaryTransferTime);

    return String.join(System.lineSeparator(), messages);
  }

  private String getResultsMessage(Map<String, RegionRedundancyStatus> regionResults,
      String baseMessage) {
    String message = baseMessage + System.lineSeparator();
    message += regionResults.values().stream().map(RegionRedundancyStatus::toString)
        .collect(Collectors.joining("," + System.lineSeparator()));
    return message;
  }

  @Override
  public RegionRedundancyStatus getRegionResult(String regionName) {
    regionResult = satisfiedRedundancyRegionsResults.get(regionName);
    if (regionResult == null) {
      regionResult = underRedundancyRegionsResults.get(regionName);
    }
    if (regionResult == null) {
      regionResult = zeroRedundancyRegionsResults.get(regionName);
    }
    return regionResult;
  }

  @Override
  public Map<String, RegionRedundancyStatus> getZeroRedundancyRegionResults() {
    return zeroRedundancyRegionsResults;
  }

  @Override
  public Map<String, RegionRedundancyStatus> getUnderRedundancyRegionResults() {
    return underRedundancyRegionsResults;
  }

  @Override
  public Map<String, RegionRedundancyStatus> getSatisfiedRedundancyRegionResults() {
    return satisfiedRedundancyRegionsResults;
  }

  @Override
  public Map<String, RegionRedundancyStatus> getRegionResults() {
    Map<String, RegionRedundancyStatus> combinedResults =
        new HashMap<>(satisfiedRedundancyRegionsResults);
    combinedResults.putAll(underRedundancyRegionsResults);
    combinedResults.putAll(zeroRedundancyRegionsResults);

    return combinedResults;
  }

  @Override
  public int getTotalPrimaryTransfersCompleted() {
    return totalPrimaryTransfersCompleted;
  }

  @Override
  public long getTotalPrimaryTransferTime() {
    return totalPrimaryTransferTime;
  }

  @Override
  public List<String> getIncludedRegionsWithNoMembers() {
    return includedRegionsWithNoMembers;
  }

  @Override
  public boolean getSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  @Override
  public String getStatusMessage() {
    return statusMessage;
  }

  public void setStatusMessage(String message) {
    statusMessage = message;
  }

  public void addIncludedRegionsWithNoMembers(List<String> regions) {
    includedRegionsWithNoMembers.addAll(regions);
  }

  @Override
  public String toString() {
    return "RestoreRedundancyResultsImpl{" +
        "zeroRedundancyRegions=" + zeroRedundancyRegionsResults +
        ", underRedundancyRegions=" + underRedundancyRegionsResults +
        ", satisfiedRedundancyRegions=" + satisfiedRedundancyRegionsResults +
        ", totalPrimaryTransfersCompleted=" + totalPrimaryTransfersCompleted +
        ", totalPrimaryTransferTime=" + totalPrimaryTransferTime +
        ", success=" + success +
        ", message='" + statusMessage + '\'' +
        ", includedRegionsWithNoMembers=" + includedRegionsWithNoMembers +
        '}';
  }
}
