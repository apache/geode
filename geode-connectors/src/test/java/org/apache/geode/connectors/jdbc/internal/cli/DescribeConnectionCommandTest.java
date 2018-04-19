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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ClusterConfigurationService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class DescribeConnectionCommandTest {
  public static final String COMMAND = "describe jdbc-connection --name=name ";
  private DescribeConnectionCommand command;
  private ClusterConfigurationService ccService;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() throws Exception {
    command = spy(DescribeConnectionCommand.class);
    ccService = mock(InternalClusterConfigurationService.class);
    doReturn(ccService).when(command).getConfigurationService();
  }

  @Test
  public void requiredParameter() {
    gfsh.executeAndAssertThat(command, "describe jdbc-connection").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void whenCCServiceIsNotAvailable() {
    doReturn(null).when(command).getConfigurationService();
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("cluster configuration service is not running");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectorServiceFound() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("connection named 'name' not found");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectionFound() {
    ConnectorService connectorService = mock(ConnectorService.class);
    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("connection named 'name' not found");
  }

  @Test
  public void successfulResult() {
    // connection found in CC
    ConnectorService.Connection connection = new ConnectorService.Connection("name1", "url1",
        "user1", "password1", "param1:value1,param2:value2");

    ConnectorService connectorService = mock(ConnectorService.class);
    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("name", "name1")
        .containsOutput("url", "url1").containsOutput("user", "user1")
        .containsOutput("password", "********").containsOutput("param1", "value1")
        .containsOutput("param2", "value2");
  }

  @Test
  public void whenCCIsNotAvailableAndMemberIsNotSpecified() {
    doReturn(null).when(command).getConfigurationService();
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("Use --member option to describe connections");
  }

  @Test
  public void whenNonExistingMemberIsSpecified() {
    doReturn(null).when(command).getMember(any());
    gfsh.executeAndAssertThat(command, COMMAND + " --member=member1").statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void whenExistingMemberIsSpecified() {
    doReturn(mock(DistributedMember.class)).when(command).getMember(any());

    ConnectorService.Connection connection =
        new ConnectorService.Connection("name", "url1", "user1", "password1", "p1:v1,p2:v2");
    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(DistributedMember.class));
    when(rc.getResult()).thenReturn(Collections.singletonList(connection));

    gfsh.executeAndAssertThat(command, COMMAND + " --member=member1").statusIsSuccess()
        .containsOutput("name", "name").containsOutput("url", "url1")
        .containsOutput("user", "user1").containsOutput("password", "********")
        .containsOutput("p1", "v1").containsOutput("p2", "v2");
  }
}
