package org.apache.geode.management.internal.cli.commands;

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
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class ListGatewayCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private final Map<String, GatewayReceiverMXBean> receiverBeans = new HashMap<>();
  private ListGatewayCommand command;
  private GatewayReceiverMXBean receiverMXBean;
  private GatewaySenderMXBean senderMXBean;

  @Before
  public void setup() {
    command = spy(ListGatewayCommand.class);
    DistributedMember member = mock(DistributedMember.class);
    SystemManagementService service = mock(SystemManagementService.class);

    doReturn(Stream.of(member).collect(toSet())).when(command).findMembers(any(), any());
    doReturn(service).when(command).getManagementService();

    receiverMXBean = mock(GatewayReceiverMXBean.class);
    doReturn(5407).when(receiverMXBean).getPort();
    doReturn(7).when(receiverMXBean).getClientConnectionCount();
    receiverBeans.put("10.118.19.46(server-ln-1:31527)<v1>:1026", receiverMXBean);

    senderMXBean = mock(GatewaySenderMXBean.class);
  }

  @Test
  public void listGatewaysDisplaysGatewaySendersAndReceivers() {
    ResultModel crd = new ResultModel();
    crd.setHeader(CliStrings.HEADER_GATEWAYS);

    doReturn(new String[] {"10.118.19.31(server-ny-2:33256)<v2>:1029",
        "10.118.19.31(server-ny-1:33206)<v1>:1028"}).when(receiverMXBean)
            .getConnectedGatewaySenders();

    command.accumulateListGatewayResult(crd, Collections.emptyMap(), receiverBeans);
    new CommandResultAssert(new CommandResult(crd))
        .hasTableSection("gatewayReceivers")
        .hasColumn("Senders Connected")
        .containsExactly(
            "10.118.19.31(server-ny-2:33256)<v2>:1029, 10.118.19.31(server-ny-1:33206)<v1>:1028");
  }

  @Test
  public void listGatewaysDisplaysGatewayReceiversWhenEmpty() {
    ResultModel crd = new ResultModel();

    doReturn(new String[0]).when(receiverMXBean).getConnectedGatewaySenders();

    command.accumulateListGatewayResult(crd, Collections.emptyMap(), receiverBeans);
    new CommandResultAssert(new CommandResult(crd))
        .hasTableSection("gatewayReceivers")
        .hasColumn("Senders Connected").containsExactly("");
  }

  @Test
  public void listGatewaysDisplaysGatewayReceiversWhenNull() {
    ResultModel crd = new ResultModel();

    doReturn(null).when(receiverMXBean).getConnectedGatewaySenders();

    command.accumulateListGatewayResult(crd, Collections.emptyMap(), receiverBeans);

    new CommandResultAssert(new CommandResult(crd))
        .hasTableSection("gatewayReceivers")
        .hasColumn("Senders Connected").containsExactly("");
  }

  @Test
  public void getGatewaySenderStatus() {
    when(senderMXBean.isRunning()).thenReturn(false);
    assertThat(ListGatewayCommand.getStatus(senderMXBean)).isEqualTo("Not Running");

    when(senderMXBean.isRunning()).thenReturn(true);
    when(senderMXBean.isPaused()).thenReturn(true);
    assertThat(ListGatewayCommand.getStatus(senderMXBean)).isEqualTo("Paused");

    when(senderMXBean.isPaused()).thenReturn(false);
    when(senderMXBean.isConnected()).thenReturn(false);
    assertThat(ListGatewayCommand.getStatus(senderMXBean)).isEqualTo("Running, not Connected");

    when(senderMXBean.isConnected()).thenReturn(true);
    assertThat(ListGatewayCommand.getStatus(senderMXBean)).isEqualTo("Running and Connected");
  }
}
