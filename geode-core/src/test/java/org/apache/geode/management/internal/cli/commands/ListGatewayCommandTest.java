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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class ListGatewayCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private ListGatewayCommand command;
  private static String COMMAND = "list gateways";
  private DistributedMember member;
  private SystemManagementService service;
  private DistributedSystemMXBean mxBean;
  private GatewayReceiverMXBean receiverMXBean;
  private Map<String, GatewayReceiverMXBean> receiverBeans = new HashMap<>();;

  @Before
  public void setup() {
    command = spy(ListGatewayCommand.class);
    member = mock(DistributedMember.class);
    service = mock(SystemManagementService.class);
    mxBean = mock(DistributedSystemMXBean.class);

    doReturn(Stream.of(member).collect(toSet())).when(command).findMembers(any(), any());
    doReturn(service).when(command).getManagementService();

    receiverMXBean = mock(GatewayReceiverMXBean.class);
    doReturn(5407).when(receiverMXBean).getPort();
    doReturn(7).when(receiverMXBean).getClientConnectionCount();
    receiverBeans.put("10.118.19.46(server-ln-1:31527)<v1>:1026", receiverMXBean);
  }

  @Test
  public void listGatewaysDisplaysGatewaySendersAndReceivers() {
    CompositeResultData crd = ResultBuilder.createCompositeResultData();
    crd.setHeader(CliStrings.HEADER_GATEWAYS);

    doReturn(new String[] {"10.118.19.31(server-ny-2:33256)<v2>:1029",
        "10.118.19.31(server-ny-1:33206)<v1>:1028"}).when(receiverMXBean)
            .getConnectedGatewaySenders();

    command.accumulateListGatewayResult(crd, Collections.EMPTY_MAP, receiverBeans);
    JSONObject tableContent = (JSONObject) crd.retrieveSectionByIndex(0).getSectionGfJsonObject()
        .get("__tables__-GatewayReceiver Table");

    assertThat(tableContent.get("content").toString()).contains(
        "[\"10.118.19.31(server-ny-2:33256)<v2>:1029, 10.118.19.31(server-ny-1:33206)<v1>:1028\"]");
  }

  @Test
  public void listGatewaysDisplaysGatewayReceiversWhenEmpty() {
    CompositeResultData crd = ResultBuilder.createCompositeResultData();
    crd.setHeader(CliStrings.HEADER_GATEWAYS);

    doReturn(new String[0]).when(receiverMXBean).getConnectedGatewaySenders();

    command.accumulateListGatewayResult(crd, Collections.EMPTY_MAP, receiverBeans);
    JSONObject tableContent = (JSONObject) crd.retrieveSectionByIndex(0).getSectionGfJsonObject()
        .get("__tables__-GatewayReceiver Table");

    assertThat(tableContent.get("content").toString()).contains("[\"\"]");
  }

  @Test
  public void listGatewaysDisplaysGatewayReceiversWhenNull() {
    CompositeResultData crd = ResultBuilder.createCompositeResultData();
    crd.setHeader(CliStrings.HEADER_GATEWAYS);

    doReturn(null).when(receiverMXBean).getConnectedGatewaySenders();

    command.accumulateListGatewayResult(crd, Collections.EMPTY_MAP, receiverBeans);
    JSONObject tableContent = (JSONObject) crd.retrieveSectionByIndex(0).getSectionGfJsonObject()
        .get("__tables__-GatewayReceiver Table");

    assertThat(tableContent.get("content").toString()).contains("[\"\"]");
  }
}
