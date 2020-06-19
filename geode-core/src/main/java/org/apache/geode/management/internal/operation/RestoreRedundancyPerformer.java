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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.functions.RestoreRedundancyFunction;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.management.runtime.RestoreRedundancyResults;

public class RestoreRedundancyPerformer
    implements OperationPerformer<RestoreRedundancyRequest, RestoreRedundancyResults> {
  @Immutable
  @VisibleForTesting
  static final Version ADDED_VERSION = Version.GEODE_1_13_0;
  private static final String NO_MEMBERS_WITH_VERSION_FOR_REGION =
      "No members with a version greater than or equal to %s were found for region %s";
  private static final String EXCEPTION_MEMBER_MESSAGE = "Exception occurred on member %s: %s";
  public static final String SUCCESS_STATUS_MESSAGE = "Success";

  @Override
  public RestoreRedundancyResults perform(Cache cache, RestoreRedundancyRequest operation) {
    return perform(cache, operation, false);
  }

  public RestoreRedundancyResults perform(Cache cache, RestoreRedundancyRequest operation,
      boolean isStatusCommand) {
    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    List<String> includedRegionsWithNoMembers = new ArrayList<>();

    populateLists(membersForEachRegion, includedRegionsWithNoMembers, operation.getIncludeRegions(),
        operation.getExcludeRegions(), (InternalCache) cache);

    for (RebalanceOperationPerformer.MemberPRInfo prInfo : membersForEachRegion) {
      // Filter out any members using older versions of Geode
      List<DistributedMember> viableMembers = filterViableMembers(prInfo);

      if (!viableMembers.isEmpty()) {
        // Update the MemberPRInfo with the viable members
        prInfo.dsMemberList = viableMembers;
      } else {
        RestoreRedundancyResultsImpl results = new RestoreRedundancyResultsImpl();
        results.setStatusMessage(String.format(NO_MEMBERS_WITH_VERSION_FOR_REGION,
            ADDED_VERSION.getName(), prInfo.region));
        results.setSuccess(false);
        return results;
      }
    }

    List<RestoreRedundancyResults> functionResults = new ArrayList<>();
    Object[] functionArgs = {operation, isStatusCommand};
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
      RestoreRedundancyResults functionResult = executeFunctionAndGetFunctionResult(
          new RestoreRedundancyFunction(), functionArgs, targetMember);
      if (!functionResult.getSuccess()) {
        // Record the error and then give up
        RestoreRedundancyResultsImpl results = new RestoreRedundancyResultsImpl();
        String errorString =
            String.format(EXCEPTION_MEMBER_MESSAGE, targetMember.getName(),
                functionResult.getStatusMessage());
        results.setStatusMessage(errorString);
        results.setSuccess(false);
        return results;
      }
      functionResults.add(functionResult);
      completedMembers.add(targetMember);
    }

    RestoreRedundancyResultsImpl finalResult = new RestoreRedundancyResultsImpl();
    finalResult.addIncludedRegionsWithNoMembers(includedRegionsWithNoMembers);
    for (RestoreRedundancyResults functionResult : functionResults) {
      finalResult.addRegionResults(functionResult);
    }
    finalResult.setSuccess(true);
    finalResult.setStatusMessage(SUCCESS_STATUS_MESSAGE);
    return finalResult;
  }

  public RestoreRedundancyResults executeFunctionAndGetFunctionResult(Function<?> function,
      Object args,
      DistributedMember targetMember) {
    ResultCollector<?, ?> rc =
        ManagementUtils.executeFunction(function, args, Collections.singleton(targetMember));
    List<RestoreRedundancyResults> results = uncheckedCast(rc.getResult());
    return results.isEmpty() ? null : results.get(0);
  }


  List<DistributedMember> filterViableMembers(
      RebalanceOperationPerformer.MemberPRInfo prInfo) {
    return prInfo.dsMemberList.stream()
        .map(InternalDistributedMember.class::cast)
        .filter(member -> member.getVersionObject().compareTo(ADDED_VERSION) >= 0)
        .collect(Collectors.toList());
  }

  void populateLists(List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion,
      List<String> noMemberRegions, List<String> includeRegions, List<String> excludeRegions,
      InternalCache cache) {
    // Include all regions
    if (includeRegions == null) {
      // Exclude these regions
      List<RebalanceOperationPerformer.MemberPRInfo> memberRegionList =
          getMembersForEachRegion(cache, excludeRegions);
      membersForEachRegion.addAll(memberRegionList);
    } else {
      for (String regionName : includeRegions) {
        DistributedMember memberForRegion = getOneMemberForRegion(cache, regionName);

        // If we did not find a member for this region name, add it to the list of regions with no
        // members
        if (memberForRegion == null) {
          noMemberRegions.add(regionName);
        } else {
          RebalanceOperationPerformer.MemberPRInfo memberPRInfo =
              new RebalanceOperationPerformer.MemberPRInfo();
          memberPRInfo.region = regionName;
          memberPRInfo.dsMemberList.add(memberForRegion);
          membersForEachRegion.add(memberPRInfo);
        }
      }
    }
  }

  // Extracted for testing
  private List<RebalanceOperationPerformer.MemberPRInfo> getMembersForEachRegion(
      InternalCache cache,
      List<String> excludedRegionList) {
    return RebalanceOperationPerformer.getMemberRegionList(
        ManagementService.getManagementService(cache), cache, excludedRegionList);
  }

  // Extracted for testing
  private DistributedMember getOneMemberForRegion(InternalCache cache, String regionName) {
    String regionNameWithSeparator = regionName;
    // The getAssociatedMembers method requires region names start with '/'
    if (!regionName.startsWith(SEPARATOR)) {
      regionNameWithSeparator = SEPARATOR + regionName;
    }
    return RebalanceOperationPerformer.getAssociatedMembers(regionNameWithSeparator, cache);
  }

}
