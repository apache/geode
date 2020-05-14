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

import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.FAILURE;
import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.SUCCESS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFERS_COMPLETED;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFER_TIME;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;
import static org.apache.geode.management.cli.Result.Status.ERROR;
import static org.apache.geode.management.cli.Result.Status.OK;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.ADDED_VERSION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.ERROR_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.ERROR_SECTION_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.EXCEPTION_MEMBER_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.FULLY_SATISFIED_REDUNDANCY;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.INDENT;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.NO_MEMBERS_FOR_REGION_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.NO_MEMBERS_FOR_REGION_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.NO_MEMBERS_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.NO_MEMBERS_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.NO_MEMBERS_WITH_VERSION_FOR_REGION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.PARTIALLY_SATISFIED_REDUNDANCY;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.PRIMARIES_INFO_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.SATISFIED_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.SUMMARY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.UNDER_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.ZERO_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.ZERO_REDUNDANT_COPIES;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.control.RegionRedundancyStatus;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.operation.RebalanceOperationPerformer;

public class RedundancyCommandTest {
  private final String includeRegion1 = "include1";
  private final String includeRegion2 = "include2";
  private final String[] includeRegions = {includeRegion1, includeRegion2};
  private final String excludeRegion1 = "exclude1";
  private final String excludeRegion2 = "exclude2";
  private final String[] excludeRegions = {excludeRegion1, excludeRegion2};
  private final boolean shouldReassignPrimaries = true;
  private final boolean isStatusCommand = true;
  private final Object[] expectedArguments =
      {includeRegions, excludeRegions, shouldReassignPrimaries, isStatusCommand};
  private DistributedMember mockMember1;
  private DistributedMember mockMember2;
  private CliFunctionResult successFunctionResult;
  private CliFunctionResult errorFunctionResult;
  private RestoreRedundancyResultsImpl mockResultCollector;
  public final int transfersCompleted = 5;
  public final long transferTime = 1234;
  private RedundancyCommand command;

  @Before
  public void setUp() {
    command = spy(new RedundancyCommand());
    Cache mockCache = mock(InternalCache.class);
    doReturn(mockCache).when(command).getCache();

    mockMember1 = mock(DistributedMember.class);
    mockMember2 = mock(DistributedMember.class);

    RestoreRedundancyResults mockResultObject = mock(RestoreRedundancyResults.class);
    String regionName = "regionName";
    RegionRedundancyStatus mockRegionResult = mock(RegionRedundancyStatus.class);
    Map<String, RegionRedundancyStatus> regionResults =
        Collections.singletonMap(regionName, mockRegionResult);
    when(mockResultObject.getRegionResults()).thenReturn(regionResults);

    successFunctionResult = mock(CliFunctionResult.class);
    when(successFunctionResult.getStatus()).thenReturn(OK.name());
    when(successFunctionResult.getResultObject()).thenReturn(mockResultObject);

    errorFunctionResult = mock(CliFunctionResult.class);
    when(errorFunctionResult.getStatus()).thenReturn(ERROR.name());

    mockResultCollector = mock(RestoreRedundancyResultsImpl.class);
    when(mockResultCollector.getStatus()).thenReturn(SUCCESS);
    when(mockResultCollector.getTotalPrimaryTransfersCompleted()).thenReturn(transfersCompleted);
    when(mockResultCollector.getTotalPrimaryTransferTime())
        .thenReturn(Duration.ofMillis(transferTime));
    doReturn(mockResultCollector).when(command).getNewRestoreRedundancyResults();
  }

  @Test
  public void executeReturnsErrorResultModelWhenNoViableMembersAreFoundForAtLeastOneRegion() {
    RebalanceOperationPerformer.MemberPRInfo viableMembersPRInfo =
        createMemberPRInfo("region1", mockMember1);
    RebalanceOperationPerformer.MemberPRInfo noViableMembersPRInfo =
        createMemberPRInfo("region2", mockMember1);

    // Put something in the members list when we call populateLists
    doAnswer(invocation -> {
      List<RebalanceOperationPerformer.MemberPRInfo> memberPRInfoList = invocation.getArgument(0);
      memberPRInfoList.add(viableMembersPRInfo);
      memberPRInfoList.add(noViableMembersPRInfo);
      return null;
    }).when(command).populateLists(any(), any(), any(), any());

    List<DistributedMember> viableMembers = new ArrayList<>();
    viableMembers.add(mockMember1);

    doReturn(viableMembers).when(command).filterViableMembers(eq(viableMembersPRInfo));
    doReturn(Collections.emptyList()).when(command).filterViableMembers(eq(noViableMembersPRInfo));

    ResultModel result =
        command.execute(includeRegions, excludeRegions, shouldReassignPrimaries, isStatusCommand);

    assertThat(result.getStatus(), is(ERROR));
    InfoResultModel errorSection = result.getInfoSection(ERROR_SECTION);
    assertThat(errorSection.getHeader(), is(ERROR_SECTION_HEADER));
    assertThat(errorSection.getContent().size(), is(1));
    assertThat(errorSection.getContent(), hasItem(String.format(NO_MEMBERS_WITH_VERSION_FOR_REGION,
        ADDED_VERSION.getName(), noViableMembersPRInfo.region)));

    // Confirm we returned early
    verify(command, times(0)).executeFunctionOnMembers(any(), any(), anyBoolean(), anyBoolean(),
        any());
    verify(command, times(0)).buildResultModelFromFunctionResults(any(), any(), anyBoolean());
  }

  @Test
  public void executeCallsMethodsWithCorrectArgumentsWhenViableMembersAreFoundForAllRegions() {
    RebalanceOperationPerformer.MemberPRInfo firstMemberPRInfo =
        createMemberPRInfo("region1", mockMember1);
    RebalanceOperationPerformer.MemberPRInfo secondMemberPRInfo =
        createMemberPRInfo("region2", mockMember1);

    List<RebalanceOperationPerformer.MemberPRInfo> expectedMembersForEachRegion =
        Arrays.asList(firstMemberPRInfo, secondMemberPRInfo);

    // Put something in the members list when we call populateLists
    doAnswer(invocation -> {
      List<RebalanceOperationPerformer.MemberPRInfo> memberPRInfoList = invocation.getArgument(0);
      memberPRInfoList.addAll(expectedMembersForEachRegion);
      return null;
    }).when(command).populateLists(any(), any(), any(), any());

    // Allow us to get past the filter viable members step
    List<DistributedMember> viableMembers = Collections.singletonList(mockMember1);
    doReturn(viableMembers).when(command).filterViableMembers(any());

    // Prevent the executeFunctionOnMembers method from attempting to actually execute the function
    List<CliFunctionResult> functionResults = new ArrayList<>();
    doReturn(functionResults).when(command).executeFunctionOnMembers(any(), any(), anyBoolean(),
        anyBoolean(),
        any());

    // Prevent the buildResultModelFromFunctionResults method from doing anything
    doReturn(mock(ResultModel.class)).when(command).buildResultModelFromFunctionResults(any(),
        any(), anyBoolean());

    command.execute(includeRegions, excludeRegions, shouldReassignPrimaries, isStatusCommand);

    // Confirm we set the correct viable members on the memberPRInfos
    verify(command, times(1)).filterViableMembers(firstMemberPRInfo);
    verify(command, times(1)).filterViableMembers(secondMemberPRInfo);
    assertThat(firstMemberPRInfo.dsMemberList, is(viableMembers));
    assertThat(secondMemberPRInfo.dsMemberList, is(viableMembers));

    // Confirm we called executeFunctionOnMembers and buildResultModelFromFunctionResults with the
    // correct arguments
    verify(command, times(1)).executeFunctionOnMembers(includeRegions, excludeRegions,
        shouldReassignPrimaries, isStatusCommand, expectedMembersForEachRegion);
    verify(command, times(1)).buildResultModelFromFunctionResults(eq(functionResults), any(),
        eq(isStatusCommand));
  }

  @Test
  public void filterViableMembersReturnsCorrectly() {
    InternalDistributedMember oldMember = mock(InternalDistributedMember.class);
    when(oldMember.getVersionObject()).thenReturn(Version.GEODE_1_1_0);

    InternalDistributedMember viableMember = mock(InternalDistributedMember.class);
    when(viableMember.getVersionObject()).thenReturn(ADDED_VERSION);

    RebalanceOperationPerformer.MemberPRInfo twoViableMembers =
        createMemberPRInfo("", viableMember, viableMember);
    RebalanceOperationPerformer.MemberPRInfo oneViableMember =
        createMemberPRInfo("", oldMember, viableMember);
    RebalanceOperationPerformer.MemberPRInfo noViableMembers =
        createMemberPRInfo("", oldMember, oldMember);

    assertThat(command.filterViableMembers(twoViableMembers).size(), is(2));
    assertThat(command.filterViableMembers(oneViableMember).size(), is(1));
    assertThat(command.filterViableMembers(noViableMembers).size(), is(0));
  }

  @Test
  public void populateListsCorrectlyPopulatesListsWhenIncludeRegionsIsNull() {
    List<RebalanceOperationPerformer.MemberPRInfo> expectedListContents = new ArrayList<>();
    expectedListContents.add(new RebalanceOperationPerformer.MemberPRInfo());
    doReturn(expectedListContents).when(command).getMembersForEachRegion(any());

    List<String> expectedExcludedRegions = Arrays.asList(excludeRegions);

    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    List<String> noMemberRegions = new ArrayList<>();
    command.populateLists(membersForEachRegion, noMemberRegions, null, excludeRegions);

    assertThat(membersForEachRegion, is(expectedListContents));
    assertThat(noMemberRegions.size(), is(0));

    // Confirm that the correct excluded regions were passed as arguments
    verify(command, times(1)).getMembersForEachRegion(eq(expectedExcludedRegions));
  }

  @Test
  public void populateListsCorrectlyPopulatesListsWhenIncludeRegionsIsNotNull() {
    String noMemberRegionName = "noMemberRegion";
    List<String> expectedNoMemberRegions = Collections.singletonList(noMemberRegionName);
    String regionName = "testRegion";
    DistributedMember mockMember = mock(DistributedMember.class);

    doReturn(null).when(command).getOneMemberForRegion(eq(noMemberRegionName));
    doReturn(mockMember).when(command).getOneMemberForRegion(eq(regionName));

    String[] includedRegions = {noMemberRegionName, regionName};

    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    List<String> noMemberRegions = new ArrayList<>();
    command.populateLists(membersForEachRegion, noMemberRegions, includedRegions, null);

    assertThat(noMemberRegions, is(expectedNoMemberRegions));
    assertThat(membersForEachRegion.size(), is(1));
    assertThat(membersForEachRegion.get(0).region, is(regionName));
    assertThat(membersForEachRegion.get(0).dsMemberList, hasItem(mockMember));
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
        command.executeFunctionOnMembers(includeRegions, excludeRegions, shouldReassignPrimaries,
            isStatusCommand,
            membersForEachRegion);
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
        command.executeFunctionOnMembers(includeRegions, excludeRegions, shouldReassignPrimaries,
            isStatusCommand,
            membersForEachRegion);
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
        command.executeFunctionOnMembers(includeRegions, excludeRegions, shouldReassignPrimaries,
            isStatusCommand,
            membersForEachRegion);
    assertThat(functionResults.size(), is(1));
    assertThat(functionResults, is(Collections.singletonList(errorFunctionResult)));

    verify(command, times(1)).executeFunctionAndGetFunctionResult(any(), eq(expectedArguments),
        any());
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsErrorWhenFunctionResultListIsEmpty() {
    ResultModel result =
        command.buildResultModelFromFunctionResults(new ArrayList<>(), new ArrayList<>(), false);

    assertThat(result.getStatus(), is(OK));
    assertThat(result.getInfoSection(NO_MEMBERS_SECTION).getHeader(),
        is(NO_MEMBERS_HEADER));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsErrorWhenAFunctionResultHasNoResultObject() {
    CliFunctionResult errorResult = mock(CliFunctionResult.class);
    String errorMessage = "error";
    String memberName = "member";
    when(errorResult.getStatusMessage()).thenReturn(errorMessage);
    when(errorResult.getMemberIdOrName()).thenReturn(memberName);

    List<CliFunctionResult> resultsWithError = Arrays.asList(successFunctionResult, errorResult);

    String expectedMessage = String.format(EXCEPTION_MEMBER_MESSAGE, memberName, errorMessage);

    ResultModel result =
        command.buildResultModelFromFunctionResults(resultsWithError, new ArrayList<>(), false);

    assertThat(result.getStatus(), is(ERROR));
    InfoResultModel errorSection = result.getInfoSection(ERROR_SECTION);
    assertThat(errorSection.getHeader(), is(ERROR_SECTION_HEADER));
    assertThat(errorSection.getContent().size(), is(1));
    assertThat(errorSection.getContent(), hasItem(expectedMessage));
  }

  @Test
  public void buildResultModelFromFunctionResultsPopulatesResultCollectorWhenFunctionResultHasResultObject() {
    CliFunctionResult functionResult = mock(CliFunctionResult.class);
    RestoreRedundancyResults mockResultObject = mock(RestoreRedundancyResults.class);
    when(functionResult.getResultObject()).thenReturn(mockResultObject);

    command.buildResultModelFromFunctionResults(Collections.singletonList(functionResult),
        new ArrayList<>(), false);

    verify(mockResultCollector, times(1)).addRegionResults(mockResultObject);
  }

  @Test
  public void buildResultModelFromFunctionResultsIncludesRegionsWithNoMembersSectionWhenSomeRegionsHaveNoMembers() {
    List<String> regionsWithNoMembers = new ArrayList<>();

    String noMembersRegion1 = "region1";
    String noMembersRegion2 = "region2";
    regionsWithNoMembers.add(noMembersRegion1);
    regionsWithNoMembers.add(noMembersRegion2);

    ResultModel result =
        command.buildResultModelFromFunctionResults(
            Collections.singletonList(successFunctionResult), regionsWithNoMembers, false);

    InfoResultModel noMembersSection = result.getInfoSection(NO_MEMBERS_FOR_REGION_SECTION);
    assertThat(noMembersSection.getHeader(), is(NO_MEMBERS_FOR_REGION_HEADER));
    assertThat(noMembersSection.getContent(), hasItems(noMembersRegion1, noMembersRegion2));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsErrorWhenIsStatusCommandIsFalseAndResultCollectorStatusIsFailure() {
    when(mockResultCollector.getStatus()).thenReturn(FAILURE);

    ResultModel result =
        command.buildResultModelFromFunctionResults(
            Collections.singletonList(successFunctionResult), new ArrayList<>(), false);

    assertThat(result.getStatus(), is(ERROR));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsOkWhenIsStatusCommandIsFalseAndResultCollectorStatusIsSuccess() {
    ResultModel result =
        command.buildResultModelFromFunctionResults(
            Collections.singletonList(successFunctionResult), new ArrayList<>(), false);

    assertThat(result.getStatus(), is(OK));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsOkWhenIsStatusCommandIsTrueAndResultCollectorStatusIsFailure() {
    when(mockResultCollector.getStatus()).thenReturn(FAILURE);

    ResultModel result =
        command.buildResultModelFromFunctionResults(
            Collections.singletonList(successFunctionResult), new ArrayList<>(), true);

    assertThat(result.getStatus(), is(OK));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsInfoSectionsForEachRegionResultStatusAndPrimaryInfoWhenIsStatusCommandIsFalse() {
    RegionRedundancyStatus zeroRedundancy = mock(RegionRedundancyStatus.class);
    Map<String, RegionRedundancyStatus> zeroRedundancyResults =
        Collections.singletonMap("zeroRedundancy", zeroRedundancy);
    when(mockResultCollector.getZeroRedundancyRegionResults()).thenReturn(zeroRedundancyResults);

    RegionRedundancyStatus underRedundancy = mock(RegionRedundancyStatus.class);
    Map<String, RegionRedundancyStatus> underRedundancyResults =
        Collections.singletonMap("underRedundancy", underRedundancy);
    when(mockResultCollector.getUnderRedundancyRegionResults()).thenReturn(underRedundancyResults);

    RegionRedundancyStatus satisfiedRedundancy = mock(RegionRedundancyStatus.class);
    Map<String, RegionRedundancyStatus> satisfiedRedundancyResults =
        Collections.singletonMap("satisfiedRedundancy", satisfiedRedundancy);
    when(mockResultCollector.getSatisfiedRedundancyRegionResults())
        .thenReturn(satisfiedRedundancyResults);

    ResultModel result =
        command.buildResultModelFromFunctionResults(
            Collections.singletonList(successFunctionResult), new ArrayList<>(), false);

    InfoResultModel summarySection = result.getInfoSection(SUMMARY_SECTION);
    assertThat(summarySection.getContent(), hasItem(ZERO_REDUNDANT_COPIES + 1));
    assertThat(summarySection.getContent(), hasItem(PARTIALLY_SATISFIED_REDUNDANCY + 1));
    assertThat(summarySection.getContent(), hasItem(FULLY_SATISFIED_REDUNDANCY + 1));

    InfoResultModel zeroRedundancySection = result.getInfoSection(ZERO_REDUNDANCY_SECTION);
    assertThat(zeroRedundancySection.getHeader(), is(NO_REDUNDANT_COPIES_FOR_REGIONS));
    assertThat(zeroRedundancySection.getContent(), hasItem(INDENT + zeroRedundancy.toString()));

    InfoResultModel underRedundancySection = result.getInfoSection(UNDER_REDUNDANCY_SECTION);
    assertThat(underRedundancySection.getHeader(), is(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS));
    assertThat(underRedundancySection.getContent(), hasItem(INDENT + underRedundancy.toString()));

    InfoResultModel satisfiedRedundancySection =
        result.getInfoSection(SATISFIED_REDUNDANCY_SECTION);
    assertThat(satisfiedRedundancySection.getHeader(), is(REDUNDANCY_SATISFIED_FOR_REGIONS));
    assertThat(satisfiedRedundancySection.getContent(),
        hasItem(INDENT + satisfiedRedundancy.toString()));

    InfoResultModel primariesSection = result.getInfoSection(PRIMARIES_INFO_SECTION);
    assertThat(primariesSection.getContent(),
        hasItem(PRIMARY_TRANSFERS_COMPLETED + transfersCompleted));
    assertThat(primariesSection.getContent(), hasItem(PRIMARY_TRANSFER_TIME + transferTime));
  }

  @Test
  public void buildResultModelFromFunctionResultsDoesNotReturnPrimaryInfoWhenIsStatusCommandIsTrue() {
    ResultModel result =
        command.buildResultModelFromFunctionResults(
            Collections.singletonList(successFunctionResult), new ArrayList<>(), true);

    assertThat(result.getInfoSection(PRIMARIES_INFO_SECTION), nullValue());
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
