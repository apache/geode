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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.PRIMARY_TRANSFERS_COMPLETED;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.PRIMARY_TRANSFER_TIME;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.operation.RestoreRedundancyPerformer;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.management.runtime.RegionRedundancyStatus;
import org.apache.geode.management.runtime.RestoreRedundancyResults;

public class RedundancyCommand extends GfshCommand {

  public static final String NO_MEMBERS_SECTION = "no-members";
  public static final String NO_MEMBERS_HEADER =
      "No partitioned regions were found.";
  public static final String NO_MEMBERS_FOR_REGION_SECTION = "no-members-for-region";
  public static final String NO_MEMBERS_FOR_REGION_HEADER =
      "No members hosting the following regions were found: ";
  public static final String ERROR_SECTION = "errors";
  public static final String ERROR_SECTION_HEADER =
      "The following errors or exceptions were encountered: ";
  public static final String SUMMARY_SECTION = "summary-section";
  public static final String ZERO_REDUNDANT_COPIES =
      "Number of regions with zero redundant copies = ";
  public static final String PARTIALLY_SATISFIED_REDUNDANCY =
      "Number of regions with partially satisfied redundancy = ";
  public static final String FULLY_SATISFIED_REDUNDANCY =
      "Number of regions with fully satisfied redundancy = ";
  public static final String ZERO_REDUNDANCY_SECTION = "zero-redundancy";
  public static final String UNDER_REDUNDANCY_SECTION = "under-redundancy";
  public static final String SATISFIED_REDUNDANCY_SECTION = "satisfied-redundancy";
  public static final String PRIMARIES_INFO_SECTION = "primaries-info";

  public static final String INDENT = "  ";

  ResultModel execute(String[] includeRegions, String[] excludeRegions, boolean reassignPrimaries,
      boolean isStatusCommand) {
    RestoreRedundancyPerformer performer = new RestoreRedundancyPerformer();
    RestoreRedundancyRequest request = new RestoreRedundancyRequest();
    if (includeRegions != null) {
      request.setIncludeRegions(Arrays.asList(includeRegions));
    }
    if (excludeRegions != null) {
      request.setExcludeRegions(Arrays.asList(excludeRegions));
    }
    request.setReassignPrimaries(reassignPrimaries);

    // build this list
    RestoreRedundancyResults results = performer.perform(getCache(), request, isStatusCommand);
    return buildResultModelFromFunctionResults(results, isStatusCommand);

  }

  ResultModel buildResultModelFromFunctionResults(RestoreRedundancyResults results,
      boolean isStatusCommand) {
    // No members hosting partitioned regions were found, but no regions were explicitly included,
    // so return OK status
    if (results.getRegionResults().isEmpty()
        && results.getIncludedRegionsWithNoMembers().isEmpty()) {
      return createNoMembersResultModel();
    }

    if (!results.getSuccess()) {
      return createErrorResultModel(results.getStatusMessage());
    }

    ResultModel result = new ResultModel();
    // At least one explicitly included region was not found, so return error status along with the
    // results for the regions that were found
    if (!results.getIncludedRegionsWithNoMembers().isEmpty()) {
      addRegionsWithNoMembersSection(results.getIncludedRegionsWithNoMembers(), result);
    }

    addSummarySection(result, results);
    addZeroRedundancySection(result, results);
    addUnderRedundancySection(result, results);
    addSatisfiedRedundancySection(result, results);

    // Status command output does not include info on reassigning primaries
    if (!isStatusCommand) {
      addPrimariesSection(result, results);

      // If redundancy was not fully restored, return error status
      if (results.getRegionOperationStatus() == RestoreRedundancyResults.Status.FAILURE) {
        result.setStatus(Result.Status.ERROR);
      }
    }
    return result;
  }

  private ResultModel createNoMembersResultModel() {
    ResultModel result = new ResultModel();
    InfoResultModel noMembersSection = result.addInfo(NO_MEMBERS_SECTION);
    noMembersSection.setHeader(NO_MEMBERS_HEADER);
    return result;
  }

  private ResultModel createErrorResultModel(String errorString) {
    ResultModel result = new ResultModel();
    InfoResultModel errorSection = result.addInfo(ERROR_SECTION);
    errorSection.setHeader(ERROR_SECTION_HEADER);
    errorSection.addLine(errorString);
    result.setStatus(Result.Status.ERROR);
    return result;
  }

  private void addRegionsWithNoMembersSection(List<String> regionsWithNoMembers,
      ResultModel result) {
    InfoResultModel noMembersSection = result.addInfo(NO_MEMBERS_FOR_REGION_SECTION);
    noMembersSection.setHeader(NO_MEMBERS_FOR_REGION_HEADER);
    regionsWithNoMembers.forEach(noMembersSection::addLine);
    result.setStatus(Result.Status.ERROR);
  }

  private void addSummarySection(ResultModel result, RestoreRedundancyResults resultCollector) {
    InfoResultModel summary = result.addInfo(SUMMARY_SECTION);
    int satisfied = resultCollector.getSatisfiedRedundancyRegionResults().size();
    int notSatisfied = resultCollector.getUnderRedundancyRegionResults().size();
    int zeroRedundancy = resultCollector.getZeroRedundancyRegionResults().size();
    summary.addLine(ZERO_REDUNDANT_COPIES + zeroRedundancy);
    summary.addLine(PARTIALLY_SATISFIED_REDUNDANCY + notSatisfied);
    summary.addLine(FULLY_SATISFIED_REDUNDANCY + satisfied);
  }

  private void addZeroRedundancySection(ResultModel result,
      RestoreRedundancyResults resultCollector) {
    Map<String, RegionRedundancyStatus> zeroRedundancyResults =
        resultCollector.getZeroRedundancyRegionResults();
    if (!zeroRedundancyResults.isEmpty()) {
      InfoResultModel zeroRedundancy = result.addInfo(ZERO_REDUNDANCY_SECTION);
      zeroRedundancy.setHeader(NO_REDUNDANT_COPIES_FOR_REGIONS);
      zeroRedundancyResults.values().stream().map(RegionRedundancyStatus::toString)
          .forEach(string -> zeroRedundancy.addLine(INDENT + string));
    }
  }

  private void addUnderRedundancySection(ResultModel result,
      RestoreRedundancyResults resultCollector) {
    Map<String, RegionRedundancyStatus> underRedundancyResults =
        resultCollector.getUnderRedundancyRegionResults();
    if (!underRedundancyResults.isEmpty()) {
      InfoResultModel underRedundancy = result.addInfo(UNDER_REDUNDANCY_SECTION);
      underRedundancy.setHeader(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS);
      underRedundancyResults.values().stream().map(RegionRedundancyStatus::toString)
          .forEach(string -> underRedundancy.addLine(INDENT + string));
    }
  }

  private void addSatisfiedRedundancySection(ResultModel result,
      RestoreRedundancyResults resultCollector) {
    Map<String, RegionRedundancyStatus> satisfiedRedundancyResults =
        resultCollector.getSatisfiedRedundancyRegionResults();
    if (!satisfiedRedundancyResults.isEmpty()) {
      InfoResultModel satisfiedRedundancy = result.addInfo(SATISFIED_REDUNDANCY_SECTION);
      satisfiedRedundancy.setHeader(REDUNDANCY_SATISFIED_FOR_REGIONS);
      satisfiedRedundancyResults.values().stream().map(RegionRedundancyStatus::toString)
          .forEach(string -> satisfiedRedundancy.addLine(INDENT + string));
    }
  }

  private void addPrimariesSection(ResultModel result,
      RestoreRedundancyResults resultCollector) {
    InfoResultModel primaries = result.addInfo(PRIMARIES_INFO_SECTION);
    primaries
        .addLine(PRIMARY_TRANSFERS_COMPLETED + resultCollector.getTotalPrimaryTransfersCompleted());
    primaries
        .addLine(PRIMARY_TRANSFER_TIME + resultCollector.getTotalPrimaryTransferTime());
  }
}
