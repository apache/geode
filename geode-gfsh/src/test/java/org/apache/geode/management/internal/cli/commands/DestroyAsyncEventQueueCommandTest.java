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

import static org.assertj.core.api.Assertions.assertThat;
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
import org.mockito.ArgumentCaptor;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.DestroyAsyncEventQueueFunction;
import org.apache.geode.management.internal.cli.functions.DestroyAsyncEventQueueFunctionArgs;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DestroyAsyncEventQueueCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private DestroyAsyncEventQueueCommand command;
  private DistributedMember member1 = mock(DistributedMember.class);
  private DistributedMember member2 = mock(DistributedMember.class);
  private Set<DistributedMember> members;
  private List<CliFunctionResult> functionResults;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    command = spy(DestroyAsyncEventQueueCommand.class);

    InternalCache cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();


    functionResults = new ArrayList<>();
    doReturn(functionResults).when(command).executeAndGetFunctionResult(
        any(DestroyAsyncEventQueueFunction.class), any(DestroyAsyncEventQueueFunctionArgs.class),
        any(Set.class));

    members = new HashSet<>();
    doReturn(members).when(command).getMembers(any(), any());
  }

  @Test
  public void mandatoryOption() {
    gfsh.executeAndAssertThat(command, "destroy async-event-queue").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void noOptionalGroup_successful() {
    members.add(member1);
    members.add(member2);
    functionResults.add(new CliFunctionResult("member2", true, String.format(
        DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, "queue1")));
    functionResults.add(new CliFunctionResult("member1", true, String.format(
        DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, "queue1")));

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --id=queue1").statusIsSuccess()
        .containsOutput("\"queue1\" destroyed").tableHasRowCount(2);
  }

  @Test
  public void ifExistsSpecified_defaultIsTrue() {
    members.add(member1);
    members.add(member2);
    functionResults.add(new CliFunctionResult("member1", true,
        String.format(
            "Skipping: " + DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
            "nonexistentQueue")));
    functionResults.add(new CliFunctionResult("member2", true,
        String.format(
            "Skipping: " + DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
            "nonexistentQueue")));
    ArgumentCaptor<DestroyAsyncEventQueueFunctionArgs> argCaptor =
        ArgumentCaptor.forClass(DestroyAsyncEventQueueFunctionArgs.class);

    gfsh.executeAndAssertThat(command,
        "destroy async-event-queue --id=nonexistentQueue --if-exists")
        .tableHasRowCount(2);
    verify(command).executeAndGetFunctionResult(any(), argCaptor.capture(), eq(members));
    assertThat(argCaptor.getValue().isIfExists()).isEqualTo(true);
  }

  @Test
  public void ifExistsNotSpecified_isFalse() {
    members.add(member1);
    members.add(member2);
    functionResults.add(new CliFunctionResult("member1", false,
        String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
            "nonexistentQueue")));
    functionResults.add(new CliFunctionResult("member2", false,
        String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
            "nonexistentQueue")));
    ArgumentCaptor<DestroyAsyncEventQueueFunctionArgs> argCaptor =
        ArgumentCaptor.forClass(DestroyAsyncEventQueueFunctionArgs.class);

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --id=nonexistentQueue")
        .statusIsError().tableHasRowCount(2);
    verify(command).executeAndGetFunctionResult(any(), argCaptor.capture(), eq(members));
    assertThat(argCaptor.getValue().isIfExists()).isEqualTo(false);
  }

  @Test
  public void ifExistsSpecifiedFalse() {
    members.add(member1);
    members.add(member2);
    ArgumentCaptor<DestroyAsyncEventQueueFunctionArgs> argCaptor =
        ArgumentCaptor.forClass(DestroyAsyncEventQueueFunctionArgs.class);
    gfsh.executeAndAssertThat(command,
        "destroy async-event-queue --id=nonexistentQueue --if-exists=false");

    verify(command).executeAndGetFunctionResult(any(), argCaptor.capture(), eq(members));
    assertThat(argCaptor.getValue().isIfExists()).isEqualTo(false);
  }

  @Test
  public void ifExistsSpecifiedTrue() {
    members.add(member1);
    members.add(member2);
    functionResults.add(new CliFunctionResult("member1", false,
        String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
            "nonexistentQueue")));
    functionResults.add(new CliFunctionResult("member2", false,
        String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
            "nonexistentQueue")));
    ArgumentCaptor<DestroyAsyncEventQueueFunctionArgs> argCaptor =
        ArgumentCaptor.forClass(DestroyAsyncEventQueueFunctionArgs.class);

    gfsh.executeAndAssertThat(command,
        "destroy async-event-queue --id=nonexistentQueue --if-exists=true");
    verify(command).executeAndGetFunctionResult(any(), argCaptor.capture(), eq(members));
    assertThat(argCaptor.getValue().isIfExists()).isEqualTo(true);
  }

  @Test
  public void mixedFunctionResults_returnsSuccess() {
    members.add(member1);
    members.add(member2);
    functionResults.add(new CliFunctionResult("member2", false, String.format(
        DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND, "queue1")));
    functionResults.add(new CliFunctionResult("member1", true, String.format(
        DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, "queue1")));

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --id=queue1").statusIsSuccess();
  }

  @Test
  public void mixedFunctionResultsWithIfExists_returnsSuccess() {
    members.add(member1);
    members.add(member2);
    functionResults.add(new CliFunctionResult("member1", true,
        String.format(
            "Skipping: " + DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
            "queue1")));
    functionResults.add(new CliFunctionResult("member1", true, String.format(
        DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, "queue1")));

    gfsh.executeAndAssertThat(command, "destroy async-event-queue --id=queue1 --if-exists")
        .statusIsSuccess();
  }
}
