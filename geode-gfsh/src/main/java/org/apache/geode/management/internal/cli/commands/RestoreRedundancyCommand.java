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

import static org.apache.geode.management.internal.functions.CliFunctionResult.StatusState.ERROR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.RestoreRedundancyFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.operation.RebalanceOperationPerformer;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class RestoreRedundancyCommand extends SingleGfshCommand {
  static final String COMMAND_NAME = "restore redundancy";
  private static final String COMMAND_HELP =
      "Restore redundancy and optionally reassign primary bucket hosting for partitioned regions in connected members. The default is for all regions to have redundancy restored and for primary buckets to be reassigned for better load balance.";
  static final String INCLUDE_REGION = "include-region";
  private static final String INCLUDE_REGION_HELP =
      "Partitioned regions to be included when restoring redundancy. If a colocated region is included, all regions colocated with that region will also be included automatically. Includes take precedence over excludes.";
  static final String EXCLUDE_REGION = "exclude-region";
  private static final String EXCLUDE_REGION_HELP =
      "Partitioned regions to be excluded when restoring redundancy.";
  static final String DONT_REASSIGN_PRIMARIES = "dont-reassign-primaries";
  private static final String DONT_REASSIGN_PRIMARIES_HELP =
      "If true, this operation will not attempt to reassign which members host primary buckets.";

  @CliCommand(value = COMMAND_NAME, help = COMMAND_HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel execute(
      @CliOption(key = INCLUDE_REGION, help = INCLUDE_REGION_HELP) String[] includeRegions,
      @CliOption(key = EXCLUDE_REGION, help = EXCLUDE_REGION_HELP) String[] excludeRegions,
      @CliOption(key = DONT_REASSIGN_PRIMARIES, help = DONT_REASSIGN_PRIMARIES_HELP,
          specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean doNotReassignPrimaries) {

    RedundancyCommandUtils utils = getUtils();

    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    List<String> includedRegionsWithNoMembers = new ArrayList<>();

    utils.populateLists(membersForEachRegion, includedRegionsWithNoMembers, includeRegions,
        excludeRegions,
        (InternalCache) getCache());

    List<CliFunctionResult> functionResults = executeFunctionOnMembers(includeRegions,
        excludeRegions, doNotReassignPrimaries, membersForEachRegion);

    return utils.buildResultModelFromFunctionResults(functionResults, includedRegionsWithNoMembers,
        false);
  }

  List<CliFunctionResult> executeFunctionOnMembers(String[] includeRegions, String[] excludeRegions,
      boolean shouldNotReassignPrimaries,
      List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion) {

    List<CliFunctionResult> functionResults = new ArrayList<>();
    Object[] functionArgs =
        new Object[] {includeRegions, excludeRegions, shouldNotReassignPrimaries};
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
          new RestoreRedundancyFunction(), functionArgs, targetMember);
      if (functionResult.getStatus().equals(ERROR.name())) {
        // Record the error and then give up
        functionResults.add(functionResult);
        break;
      }
      functionResults.add(functionResult);
      completedMembers.addAll(memberPRInfo.dsMemberList);
    }
    return functionResults;
  }

  // Extracted for testing
  RedundancyCommandUtils getUtils() {
    return new RedundancyCommandUtils();
  }
}
