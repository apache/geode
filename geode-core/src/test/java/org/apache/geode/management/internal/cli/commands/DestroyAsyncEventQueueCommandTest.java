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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState;
import org.apache.geode.management.internal.cli.functions.DestroyAsyncEventQueueFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class DestroyAsyncEventQueueCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private final String member1Name = "member1";
  private final String member2Name = "member2";
  private DestroyAsyncEventQueueCommand command;
  private DistributedMember member1 = mock(DistributedMember.class);
  private DistributedMember member2 = mock(DistributedMember.class);
  private Set<DistributedMember> members;
  private InternalCache cache;
  private List<CliFunctionResult> functionResults;
  private String aeqId = "mock-aeq-id";
  private String mockExceptionMessage = "A exception occurred during queue shutdown.";

  @Before
  public void setUp() throws Exception {
    command = spy(DestroyAsyncEventQueueCommand.class);

    cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();

    functionResults = new ArrayList<>();
    doReturn(functionResults).when(command).executeAndGetFunctionResult(
        any(DestroyAsyncEventQueueFunction.class), any(String.class), any(Set.class));

    members = new HashSet<>();
    members.add(member1);
    members.add(member2);
    doReturn(members).when(command).getMembers(any(), any());
  }

  @Test
  public void idIsAMandatoryOption() {
    gfsh.executeAndAssertThat(command, "destroy async-event-queue").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void commandReportsWhenNoMembersAreFound() {
    members.clear();
    gfsh.executeAndAssertThat(command, "destroy async-event-queue --id=" + aeqId)
        .containsOutput(CliStrings.NO_MEMBERS_FOUND_MESSAGE).statusIsError();
  }

  @Test
  public void noOptionalGroup_successful() {
    functionResults.add(getQueueDestroyedResult(member1Name));
    functionResults.add(getQueueDestroyedResult(member2Name));

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --id=" + aeqId).statusIsSuccess()
        .containsOutput("\\\"" + aeqId + "\\\" destroyed").tableHasRowCount("Member", 2);
  }

  @Test
  public void ifExistsSpecified_defaultIsTrue() {
    functionResults.add(getQueueNotFoundResult(member1Name));
    functionResults.add(getQueueNotFoundResult(member2Name));

    ArgumentCaptor<String> argCaptor = ArgumentCaptor.forClass(String.class);

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --if-exists --id=" + aeqId)
        .tableHasRowCount("Member", 2).statusIsSuccess();

    verify(command).executeAndGetFunctionResult(any(), argCaptor.capture(), eq(members));
  }

  @Test
  public void ifExistsNotSpecified_isFalse() {
    functionResults.add(getQueueNotFoundResult(member1Name));
    functionResults.add(getQueueNotFoundResult(member2Name));

    ArgumentCaptor<String> argCaptor = ArgumentCaptor.forClass(String.class);

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --id=" + aeqId).statusIsError()
        .tableHasRowCount("Member", 2);
    verify(command).executeAndGetFunctionResult(any(), argCaptor.capture(), eq(members));
  }

  @Test
  public void ifExistsSpecifiedFalse() {
    ArgumentCaptor<String> argCaptor = ArgumentCaptor.forClass(String.class);
    gfsh.executeAndAssertThat(command, "destroy async-event-queue --if-exists=false --id=" + aeqId);

    verify(command).executeAndGetFunctionResult(any(), argCaptor.capture(), eq(members));
  }

  @Test
  public void ifExistsSpecifiedTrue() {
    functionResults.add(getQueueNotFoundResult(member1Name));
    functionResults.add(getQueueNotFoundResult(member2Name));

    ArgumentCaptor<String> argCaptor = ArgumentCaptor.forClass(String.class);

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --if-exists=true --id=" + aeqId);
    verify(command).executeAndGetFunctionResult(any(), argCaptor.capture(), eq(members));
  }

  @Test
  public void mixedFunctionResults_returnsSuccess() {
    functionResults.add(getQueueNotFoundResult(member1Name));
    functionResults.add(getQueueDestroyedResult(member2Name));

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --id=" + aeqId).statusIsSuccess();
  }

  @Test
  public void mixedFunctionResultsWithIfExists_returnsSuccess() {
    functionResults.add(getQueueNotFoundResult(member1Name));
    functionResults.add(getQueueDestroyedResult(member2Name));

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --if-exists --id=" + aeqId)
        .statusIsSuccess();
  }

  @Test
  public void errorsReturnErrorResult() {
    functionResults.add(getUnexpectedErrorResult(member1Name));
    functionResults.add(getUnexpectedErrorResult(member2Name));

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --id=" + aeqId).statusIsError();
  }

  @Test
  public void errorsReturnErrorResultEvenWithIfExists() {
    functionResults.add(getUnexpectedErrorResult(member1Name));
    functionResults.add(getUnexpectedErrorResult(member2Name));

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --if-exists --id=" + aeqId)
        .statusIsError();
  }

  @Test
  public void mixedResultSuccessAndError_returnsSuccess() {
    functionResults.add(getQueueDestroyedResult(member1Name));
    functionResults.add(getUnexpectedErrorResult(member2Name));

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --if-exists --id=" + aeqId)
        .statusIsSuccess();
  }

  private CliFunctionResult getQueueNotFoundResult(String memberName) {
    return new CliFunctionResult(memberName, StatusState.IGNORABLE, String
        .format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND, aeqId));
  }

  private CliFunctionResult getQueueDestroyedResult(String memberName) {
    return new CliFunctionResult(memberName, StatusState.OK, String
        .format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, aeqId));
  }

  private CliFunctionResult getUnexpectedErrorResult(String memberName) {
    return new CliFunctionResult(memberName, new RuntimeException(mockExceptionMessage));
  }
}
