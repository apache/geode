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
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.GatewaySenderFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class CreateGatewaySenderCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateGatewaySenderCommand command;
  private InternalCache cache;
  private List<CliFunctionResult> functionResults;
  private CliFunctionResult cliFunctionResult;
  ArgumentCaptor<GatewaySenderFunctionArgs> argsArgumentCaptor =
      ArgumentCaptor.forClass(GatewaySenderFunctionArgs.class);

  @Before
  public void before() {
    command = spy(CreateGatewaySenderCommand.class);
    cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();
    functionResults = new ArrayList<>();
    doReturn(functionResults).when(command).executeAndGetFunctionResult(any(),
        any(), any(Set.class));
  }

  @Test
  public void missingId() {
    gfsh.executeAndAssertThat(command, "create gateway-sender --remote-distributed-system-id=1")
        .statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void missingRemoteId() {
    gfsh.executeAndAssertThat(command, "create gateway-sender --id=ln").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void missingOrderPolicy() {
    gfsh.executeAndAssertThat(command,
        "create gateway-sender --id=ln --remote-distributed-system-id=1 "
            + "--dispatcher-threads=2")
        .statusIsError()
        .containsOutput("Must specify --order-policy when --dispatcher-threads is larger than 1");

    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());
    gfsh.executeAndAssertThat(command,
        "create gateway-sender --id=ln --remote-distributed-system-id=1 "
            + "--dispatcher-threads=1")
        .statusIsError().containsOutput("No Members Found");
  }

  @Test
  public void paralleAndThreadOrderPolicy() {
    gfsh.executeAndAssertThat(command,
        "create gateway-sender --id=ln --remote-distributed-system-id=1 "
            + "--parallel --order-policy=THREAD")
        .statusIsError()
        .containsOutput("Parallel Gateway Sender can not be created with THREAD OrderPolicy");
  }

  @Test
  public void orderPolicyAutoComplete() {
    String command =
        "create gateway-sender --id=ln --remote-distributed-system-id=1 --order-policy";
    GfshParserRule.CommandCandidate candidate = gfsh.complete(command);
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getFirstCandidate()).isEqualTo(command + "=KEY");
  }

  @Test
  public void whenCommandOnMember() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    cliFunctionResult = new CliFunctionResult("member",
        CliFunctionResult.StatusState.OK, "cliFunctionResult");
    functionResults.add(cliFunctionResult);
    gfsh.executeAndAssertThat(command,
        "create gateway-sender --member=xyz --id=1 --remote-distributed-system-id=1")
        .statusIsSuccess();
  }

  @Test
  public void testFunctionArgs() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    cliFunctionResult = new CliFunctionResult("member",
        CliFunctionResult.StatusState.OK, "cliFunctionResult");
    functionResults.add(cliFunctionResult);
    gfsh.executeAndAssertThat(command,
        "create gateway-sender --member=xyz --id=1 --remote-distributed-system-id=1"
            + " --order-policy=thread --dispatcher-threads=2 "
            + "--gateway-event-filter=test1,test2 --gateway-transport-filter=test1,test2")
        .statusIsSuccess();
    verify(command).executeAndGetFunctionResult(any(), argsArgumentCaptor.capture(),
        any(Set.class));
    assertThat(argsArgumentCaptor.getValue().getOrderPolicy()).isEqualTo(
        GatewaySender.OrderPolicy.THREAD.toString());
    assertThat(argsArgumentCaptor.getValue().getRemoteDistributedSystemId()).isEqualTo(1);
    assertThat(argsArgumentCaptor.getValue().getDispatcherThreads()).isEqualTo(2);
    assertThat(argsArgumentCaptor.getValue().getGatewayEventFilter()).containsExactly("test1",
        "test2");
    assertThat(argsArgumentCaptor.getValue().getGatewayTransportFilter()).containsExactly("test1",
        "test2");
  }

  @Test
  public void testReturnsConfigInResultModel() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    cliFunctionResult = new CliFunctionResult("member", CliFunctionResult.StatusState.OK,
        "cliFunctionResult");
    functionResults.add(cliFunctionResult);
    ResultModel resultModel = gfsh.executeAndAssertThat(command,
        "create gateway-sender --group=xyz --id=1 --remote-distributed-system-id=1"
            + " --order-policy=thread --dispatcher-threads=2 "
            + "--gateway-event-filter=test1,test2 --gateway-transport-filter=test1,test2")
        .getResultModel();

    assertThat(resultModel.getConfigObject()).isNotNull();
    CacheConfig.GatewaySender sender = (CacheConfig.GatewaySender) resultModel.getConfigObject();
    assertThat(sender.getId()).isEqualTo("1");
    assertThat(sender.getRemoteDistributedSystemId()).isEqualTo("1");
    assertThat(sender.getOrderPolicy()).isEqualTo("THREAD");
  }

  @Test
  public void whenMembersAreDifferentVersions() {
    // Create a set of mixed version members
    Set<DistributedMember> members = new HashSet<>();
    InternalDistributedMember currentVersionMember = mock(InternalDistributedMember.class);
    doReturn(Version.CURRENT).when(currentVersionMember).getVersionObject();
    InternalDistributedMember oldVersionMember = mock(InternalDistributedMember.class);
    doReturn(Version.GEODE_140).when(oldVersionMember).getVersionObject();
    members.add(currentVersionMember);
    members.add(oldVersionMember);
    doReturn(members).when(command).getMembers(any(), any());

    // Verify executing the command fails
    gfsh.executeAndAssertThat(command,
        "create gateway-sender --id=1 --remote-distributed-system-id=1").statusIsError()
        .containsOutput(CliStrings.CREATE_GATEWAYSENDER__MSG__CAN_NOT_CREATE_DIFFERENT_VERSIONS);
  }
}
