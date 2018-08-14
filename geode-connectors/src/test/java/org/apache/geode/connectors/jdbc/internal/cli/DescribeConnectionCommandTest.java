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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DescribeConnectionCommandTest {
  public static final String COMMAND = "describe jdbc-connection --name=name";
  private DescribeConnectionCommand command;
  private ConfigurationPersistenceService ccService;
  private CacheConfig cacheConfig;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(DescribeConnectionCommand.class);
    ccService = mock(InternalConfigurationPersistenceService.class);
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    cacheConfig = mock(CacheConfig.class);
    when(ccService.getCacheConfig("cluster")).thenReturn(cacheConfig);
  }

  @Test
  public void requiredParameter() {
    gfsh.executeAndAssertThat(command, "describe jdbc-connection").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectorServiceFound() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("connection named 'name' not found");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectionFound() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();

    ConnectorService connectorService = mock(ConnectorService.class);
    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("connection named 'name' not found");
  }

  @Test
  public void whenCCIsAvailable() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();

    // connection found in CC
    ConnectorService.Connection connection = new ConnectorService.Connection("name", "url1",
        "user1", "password1", "param1:value1,param2:value2");
    List<ConnectorService.Connection> connections = new ArrayList<>();
    connections.add(connection);

    ConnectorService connectorService = mock(ConnectorService.class);
    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);
    when(connectorService.getConnection()).thenReturn(connections);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("name", "name")
        .containsOutput("url", "url1").containsOutput("user", "user1")
        .containsOutput("password", "********").containsOutput("param1", "value1")
        .containsOutput("param2", "value2");
  }

  @Test
  public void whenCCIsNotAvailableAndNoMemberExists() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.emptySet()).when(command).findMembers(null, null);

    doReturn(null).when(command).getMember(any());
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("connection named 'name' not found");
  }

  @Test
  public void whenCCIsNotAvailableAndMemberExists() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    ConnectorService.Connection connection =
        new ConnectorService.Connection("name", "url1", "user1", "password1", "p1:v1,p2:v2");
    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(
        Collections.singletonList(new CliFunctionResult("server-1", connection, "success")));

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("name", "name")
        .containsOutput("url", "url1").containsOutput("user", "user1")
        .containsOutput("password", "********").containsOutput("p1", "v1")
        .containsOutput("p2", "v2");
  }

  @Test
  public void whenCCIsNotAvailableAndNoConnectionFoundOnMember() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.emptyList());

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("connection named 'name' not found");
  }
}
