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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class StopGatewaySenderCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private StopGatewaySenderCommand command;
  private List<CliFunctionResult> functionResults;
  private final ExecutorService executorService = mock(ExecutorService.class);

  @Before
  public void before() {
    command = spy(StopGatewaySenderCommand.class);
    functionResults = new ArrayList<>();
    doReturn(functionResults).when(command).executeAndGetFunctionResult(any(), any(), any());
    doReturn(executorService).when(command).getExecutorService();
  }

  @Test
  public void whenMissingSenderIdCommandOutputsInvalidCommandError() {
    gfsh.executeAndAssertThat(command, "stop gateway-sender")
        .statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void whenNoMembersCommandOutputsNoMembersError() {
    // arrange
    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());

    // act and assert
    gfsh.executeAndAssertThat(command, "stop gateway-sender --id=sender1")
        .statusIsError().containsOutput("No Members Found");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void stopGatewaySenderStartsOneThreadPerMember() throws InterruptedException {
    // arrange
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(mock(ManagementService.class)).when(command).getManagementService();

    int membersSize = 3;
    Set<DistributedMember> members =
        Stream.generate(() -> mock(DistributedMember.class)).limit(membersSize)
            .collect(Collectors.toSet());

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(mock(SystemManagementService.class)).when(command).getManagementService();
    CliFunctionResult cliFunctionResult = new CliFunctionResult("member",
        CliFunctionResult.StatusState.OK, "cliFunctionResult");
    functionResults.add(cliFunctionResult);

    // act
    gfsh.executeAndAssertThat(command,
        "stop gateway-sender --id=sender2")
        .statusIsSuccess();

    // assert
    ArgumentCaptor<Collection> callablesCaptor =
        ArgumentCaptor.forClass(Collection.class);
    verify(executorService, times(1)).invokeAll((callablesCaptor.capture()));
    assertThat(callablesCaptor.getValue().size()).isEqualTo(members.size());
    verify(executorService, times(1)).shutdown();
  }
}
