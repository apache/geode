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

import static org.apache.geode.management.cli.Result.Status.ERROR;
import static org.apache.geode.management.cli.Result.Status.OK;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.REDUNDANCY_COMMAND_ADDED_VERSION;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.operation.RebalanceOperationPerformer;

public class StatusRedundancyCommandTest {

  private StatusRedundancyCommand command;
  private final String includeRegion1 = "include1";
  private final String includeRegion2 = "include2";
  private String[] includeRegions = {includeRegion1, includeRegion2};
  private final String excludeRegion1 = "exclude1";
  private final String excludeRegion2 = "exclude2";
  private String[] excludeRegions = {excludeRegion1, excludeRegion2};
  private DistributedMember mockMember1;
  private DistributedMember mockMember2;
  private CliFunctionResult successFunctionResult;
  private CliFunctionResult errorFunctionResult;
  private RedundancyCommandUtils mockUtils;
  private Object[] expectedArguments;

  @Before
  public void setUp() {
    mockMember1 = mock(DistributedMember.class);
    mockMember2 = mock(DistributedMember.class);
    boolean shouldNotReassignPrimaries = false;
    boolean isStatusCommand = true;
    expectedArguments = new Object[] {includeRegions, excludeRegions, shouldNotReassignPrimaries,
        isStatusCommand};

    successFunctionResult = mock(CliFunctionResult.class);
    when(successFunctionResult.getStatus()).thenReturn(OK.name());

    errorFunctionResult = mock(CliFunctionResult.class);
    when(errorFunctionResult.getStatus()).thenReturn(ERROR.name());

    mockUtils = mock(RedundancyCommandUtils.class);
    Cache mockCache = mock(InternalCache.class);

    command = spy(new StatusRedundancyCommand());
    doReturn(mockUtils).when(command).getUtils();
    doReturn(mockCache).when(command).getCache();
  }

  @Test
  public void executeCallsGetNoViableMembersResultWhenNoViableMembersAreFoundForAtLeastOneRegion() {
    RebalanceOperationPerformer.MemberPRInfo firstMemberPRInfo =
        new RebalanceOperationPerformer.MemberPRInfo();
    String region1 = "region1";
    firstMemberPRInfo.region = region1;
    RebalanceOperationPerformer.MemberPRInfo secondMemberPRInfo =
        new RebalanceOperationPerformer.MemberPRInfo();
    String region2 = "region2";
    secondMemberPRInfo.region = region2;

    // Put something in the members list when we call populateLists
    doAnswer(invocation -> {
      List<RebalanceOperationPerformer.MemberPRInfo> memberPRInfoList = invocation.getArgument(0);
      memberPRInfoList.add(firstMemberPRInfo);
      memberPRInfoList.add(secondMemberPRInfo);
      return null;
    }).when(mockUtils).populateLists(any(), any(), any(), any(), any());

    List<DistributedMember> viableMembers = new ArrayList<>();
    viableMembers.add(mock(DistributedMember.class));

    when(mockUtils.filterViableMembersForVersion(eq(firstMemberPRInfo), any()))
        .thenReturn(viableMembers);
    when(mockUtils.filterViableMembersForVersion(eq(secondMemberPRInfo), any()))
        .thenReturn(Collections.emptyList());

    command.execute(includeRegions, excludeRegions);

    verify(mockUtils, times(1)).getNoViableMembersResult(any(), eq(region2));
    verify(mockUtils, times(0)).getNoViableMembersResult(any(), eq(region1));
  }

  @Test
  public void executeCallsMethodsWithCorrectArgumentsWhenViableMembersAreFoundForAllRegions() {
    RebalanceOperationPerformer.MemberPRInfo firstMemberPRInfo =
        mock(RebalanceOperationPerformer.MemberPRInfo.class);
    RebalanceOperationPerformer.MemberPRInfo secondMemberPRInfo =
        mock(RebalanceOperationPerformer.MemberPRInfo.class);
    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    membersForEachRegion.add(firstMemberPRInfo);
    membersForEachRegion.add(secondMemberPRInfo);

    // Put something in the members list when we call populateLists
    doAnswer(invocation -> {
      List<RebalanceOperationPerformer.MemberPRInfo> memberPRInfoList = invocation.getArgument(0);
      memberPRInfoList.add(firstMemberPRInfo);
      memberPRInfoList.add(secondMemberPRInfo);
      return null;
    }).when(mockUtils).populateLists(any(), any(), any(), any(), any());

    // Allow us to get past the filter viable members step
    List<DistributedMember> viableMembers = new ArrayList<>();
    viableMembers.add(mock(DistributedMember.class));
    when(mockUtils.filterViableMembersForVersion(any(), any())).thenReturn(viableMembers);

    // Prevent the executeFunctionOnMembers method from doing anything
    List<CliFunctionResult> returnedList = new ArrayList<>();
    doReturn(returnedList).when(command).executeFunctionOnMembers(any(), any(), any());

    command.execute(includeRegions, excludeRegions);

    // Confirm we set the correct viable members on the memberPRInfos
    verify(mockUtils, times(1)).filterViableMembersForVersion(firstMemberPRInfo,
        REDUNDANCY_COMMAND_ADDED_VERSION);
    verify(mockUtils, times(1)).filterViableMembersForVersion(secondMemberPRInfo,
        REDUNDANCY_COMMAND_ADDED_VERSION);
    assertThat(firstMemberPRInfo.dsMemberList, is(viableMembers));
    assertThat(secondMemberPRInfo.dsMemberList, is(viableMembers));

    // Confirm we called executeFunctionOnMembers and buildResultModelFromFunctionResults with the
    // correct arguments
    verify(command, times(1)).executeFunctionOnMembers(includeRegions, excludeRegions,
        membersForEachRegion);
    boolean isStatusCommand = true;
    verify(mockUtils, times(1)).buildResultModelFromFunctionResults(eq(returnedList), any(),
        eq(isStatusCommand));
  }

  @Test
  public void executeFunctionOnMembersDoesNotExecuteFunctionForRegionsThatHaveHadRedundancyRestoredAlready() {
    // Since both regions exist on both members, restoring redundancy for either of the regions will
    // also cause the other region to have redundancy restored, since the function restores
    // redundancy for all non-excluded regions on the target member
    RebalanceOperationPerformer.MemberPRInfo firstRegionInfo =
        createMemberPRInfo(includeRegion1, mockMember1, mockMember2);
    RebalanceOperationPerformer.MemberPRInfo secondRegionInfo =
        createMemberPRInfo(includeRegion2, mockMember1, mockMember2);

    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    membersForEachRegion.add(firstRegionInfo);
    membersForEachRegion.add(secondRegionInfo);

    doReturn(successFunctionResult).when(command).executeFunctionAndGetFunctionResult(any(), any(),
        any());

    List<CliFunctionResult> functionResults =
        command.executeFunctionOnMembers(includeRegions, excludeRegions, membersForEachRegion);
    assertThat(functionResults, is(Collections.singletonList(successFunctionResult)));

    verify(command, times(1)).executeFunctionAndGetFunctionResult(any(), eq(expectedArguments),
        any());
  }

  @Test
  public void executeFunctionOnMembersExecutesFunctionForAllRegionsWithNoMembersInCommon() {
    // Region1 exists on member1 only, region2 exists on member 2 only. Two function executions are
    // necessary.
    RebalanceOperationPerformer.MemberPRInfo firstRegionInfo =
        createMemberPRInfo(excludeRegion1, mockMember1);
    RebalanceOperationPerformer.MemberPRInfo secondRegionInfo =
        createMemberPRInfo(excludeRegion2, mockMember2);

    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    membersForEachRegion.add(firstRegionInfo);
    membersForEachRegion.add(secondRegionInfo);

    doReturn(successFunctionResult).when(command).executeFunctionAndGetFunctionResult(any(), any(),
        eq(mockMember1));
    doReturn(successFunctionResult).when(command).executeFunctionAndGetFunctionResult(any(), any(),
        eq(mockMember2));

    List<CliFunctionResult> functionResults =
        command.executeFunctionOnMembers(includeRegions, excludeRegions, membersForEachRegion);
    assertThat(functionResults.size(), is(2));
    assertThat(functionResults, everyItem(is(successFunctionResult)));

    verify(command, times(1)).executeFunctionAndGetFunctionResult(any(), eq(expectedArguments),
        eq(mockMember1));
    verify(command, times(1)).executeFunctionAndGetFunctionResult(any(), eq(expectedArguments),
        eq(mockMember2));
  }

  @Test
  public void executeFunctionOnMembersReturnsEarlyIfFunctionResultIsError() {
    // Region1 exists on member1 only, region2 exists on member 2 only. Two function executions are
    // necessary.
    RebalanceOperationPerformer.MemberPRInfo firstRegionInfo =
        createMemberPRInfo(excludeRegion1, mockMember1);
    RebalanceOperationPerformer.MemberPRInfo secondRegionInfo =
        createMemberPRInfo(excludeRegion2, mockMember2);

    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    membersForEachRegion.add(firstRegionInfo);
    membersForEachRegion.add(secondRegionInfo);

    // Return error result first, then success result on subsequent calls
    doReturn(errorFunctionResult).doReturn(successFunctionResult).when(command)
        .executeFunctionAndGetFunctionResult(any(), any(), any());

    List<CliFunctionResult> functionResults =
        command.executeFunctionOnMembers(includeRegions, excludeRegions, membersForEachRegion);
    assertThat(functionResults.size(), is(1));
    assertThat(functionResults, is(Collections.singletonList(errorFunctionResult)));

    verify(command, times(1)).executeFunctionAndGetFunctionResult(any(), eq(expectedArguments),
        any());
  }

  private RebalanceOperationPerformer.MemberPRInfo createMemberPRInfo(String region,
      DistributedMember... members) {
    RebalanceOperationPerformer.MemberPRInfo firstRegionInfo =
        new RebalanceOperationPerformer.MemberPRInfo();
    firstRegionInfo.region = region;
    firstRegionInfo.dsMemberList.addAll(Arrays.asList(members));
    return firstRegionInfo;
  }
}
