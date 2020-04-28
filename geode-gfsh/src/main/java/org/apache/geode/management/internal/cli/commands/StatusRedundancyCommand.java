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

import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.REDUNDANCY_COMMAND_ADDED_VERSION;
import static org.apache.geode.management.internal.functions.CliFunctionResult.StatusState.ERROR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.RedundancyCommandFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.operation.RebalanceOperationPerformer;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class StatusRedundancyCommand extends GfshCommand {
  static final String COMMAND_NAME = "status redundancy";
  private static final String COMMAND_HELP =
      "Report the redundancy status for partitioned regions in connected members. The default is to report status for all regions.";
  static final String INCLUDE_REGION = "include-region";
  private static final String INCLUDE_REGION_HELP =
      "Partitioned regions to be included when reporting redundancy status. Includes take precedence over excludes.";
  static final String EXCLUDE_REGION = "exclude-region";
  private static final String EXCLUDE_REGION_HELP =
      "Partitioned regions to be excluded when reporting redundancy status.";

  @CliCommand(value = COMMAND_NAME, help = COMMAND_HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.READ)
  public ResultModel execute(
      @CliOption(key = INCLUDE_REGION, help = INCLUDE_REGION_HELP) String[] includeRegions,
      @CliOption(key = EXCLUDE_REGION, help = EXCLUDE_REGION_HELP) String[] excludeRegions) {
    RedundancyCommandUtils utils = getUtils();

    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    List<String> includedRegionsWithNoMembers = new ArrayList<>();

    utils.populateLists(membersForEachRegion, includedRegionsWithNoMembers, includeRegions,
        excludeRegions, (InternalCache) getCache());

    for (RebalanceOperationPerformer.MemberPRInfo prInfo : membersForEachRegion) {
      // Filter out any members using older versions of Geode
      List<DistributedMember> viableMembers =
          utils.filterViableMembersForVersion(prInfo, REDUNDANCY_COMMAND_ADDED_VERSION);

      if (viableMembers.size() == 0) {
        // If no viable members were found, return with error status
        return utils.getNoViableMembersResult(REDUNDANCY_COMMAND_ADDED_VERSION, prInfo.region);
      } else {
        // Update the MemberPRInfo with the viable members
        prInfo.dsMemberList = (ArrayList<DistributedMember>) viableMembers;
      }
    }

    List<CliFunctionResult> functionResults =
        executeFunctionOnMembers(includeRegions, excludeRegions, membersForEachRegion);

    return utils.buildResultModelFromFunctionResults(functionResults, includedRegionsWithNoMembers,
        true);
  }

  List<CliFunctionResult> executeFunctionOnMembers(String[] includeRegions, String[] excludeRegions,
      List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion) {

    List<CliFunctionResult> functionResults = new ArrayList<>();
    boolean shouldNotReassignPrimaries = false;
    boolean isStatusCommand = true;
    Object[] functionArgs =
        new Object[] {includeRegions, excludeRegions, shouldNotReassignPrimaries, isStatusCommand};
    List<DistributedMember> completedMembers = new ArrayList<>();
    for (RebalanceOperationPerformer.MemberPRInfo memberPRInfo : membersForEachRegion) {
      // Check to see if an earlier function execution has already targeted a member hosting this
      // region. If one has, there is no point sending a function for this region as it has already
      // had redundancy restored
      if (!Collections.disjoint(completedMembers, memberPRInfo.dsMemberList)) {
        continue;
      }
      // Try the function on the first member for this region
      DistributedMember targetMember = memberPRInfo.dsMemberList.get(0);
      CliFunctionResult functionResult = executeFunctionAndGetFunctionResult(
          new RedundancyCommandFunction(), functionArgs, targetMember);
      if (functionResult.getStatus().equals(ERROR.name())) {
        // Record the error and then give up
        functionResults.add(functionResult);
        break;
      }
      functionResults.add(functionResult);
      completedMembers.add(targetMember);
    }
    return functionResults;
  }

  // Extracted for testing
  RedundancyCommandUtils getUtils() {
    return new RedundancyCommandUtils();
  }
}
