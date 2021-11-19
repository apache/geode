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

import static org.apache.geode.management.internal.cli.commands.StopGatewaySenderCommand.StopGatewaySenderCommandDelegate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class StopGatewaySenderCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Test
  public void whenMissingSenderIdCommandReturnsInvalidCommandError() {
    StopGatewaySenderCommand command = new StopGatewaySenderCommand();
    gfsh.executeAndAssertThat(command, "stop gateway-sender")
        .statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void whenNoMembersCommandReturnsNoMembersError() {
    // arrange
    @SuppressWarnings("unchecked")
    BiFunction<String[], String[], Set<DistributedMember>> findMembers = mock(BiFunction.class);
    Set<DistributedMember> emptySet = new HashSet<>();
    when(findMembers.apply(any(), any())).thenReturn(emptySet);
    StopGatewaySenderCommand command = new StopGatewaySenderCommand(null, findMembers);

    // act and assert
    gfsh.executeAndAssertThat(command, "stop gateway-sender --id=sender1")
        .statusIsError().containsOutput("No Members Found");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void whenParamsPassedOkCommandReturnsResultFromDelegate() {
    // arrange
    Set<DistributedMember> members =
        Stream.generate(() -> mock(DistributedMember.class)).limit(3)
            .collect(Collectors.toSet());
    BiFunction<String[], String[], Set<DistributedMember>> findMembers = mock(BiFunction.class);
    when(findMembers.apply(any(), any())).thenReturn(members);
    StopGatewaySenderCommandDelegate commandDelegate = mock(StopGatewaySenderCommandDelegate.class);
    Supplier<StopGatewaySenderCommandDelegate> commandDelegateSupplier = mock(Supplier.class);
    when(commandDelegateSupplier.get()).thenReturn(commandDelegate);
    ResultModel expectedResult = mock(ResultModel.class);
    doReturn(expectedResult).when(commandDelegate).executeStopGatewaySender(any(), any(), any());

    StopGatewaySenderCommand command =
        new StopGatewaySenderCommand(commandDelegateSupplier, findMembers);

    // act
    ResultModel commandResult =
        gfsh.executeAndAssertThat(command, "stop gateway-sender --id=sender1").getCommandResult()
            .getResultData();

    // assert
    assertThat(commandResult).isEqualTo(expectedResult);
  }
}
