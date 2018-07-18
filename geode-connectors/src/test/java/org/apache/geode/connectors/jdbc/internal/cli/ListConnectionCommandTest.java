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

import static java.util.stream.Collectors.toSet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class ListConnectionCommandTest {
  public static final String COMMAND = "list jdbc-connections ";
  private ListConnectionCommand command;
  private ConfigurationPersistenceService ccService;
  private CacheConfig cacheConfig;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(ListConnectionCommand.class);
    ccService = mock(InternalConfigurationPersistenceService.class);
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    cacheConfig = mock(CacheConfig.class);
    when(ccService.getCacheConfig("cluster")).thenReturn(cacheConfig);
  }

  @Test
  public void whenCCServiceIsNotAvailable() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("No connections found");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectorServiceFound() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("(Experimental) \\nNo connections found");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectionFound() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();

    ConnectorService connectorService = mock(ConnectorService.class);
    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("(Experimental) \\nNo connections found");
  }

  @Test
  public void whenCCIsAvailable() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();

    // connections found in CC
    ConnectorService.Connection connection1 = new ConnectorService.Connection("name1", "url1",
        "user1", "password1", "param1:value1,param2:value2");
    ConnectorService.Connection connection2 = new ConnectorService.Connection("name2", "url2",
        "user2", "password2", "param3:value3,param4:value4");
    ConnectorService.Connection connection3 = new ConnectorService.Connection("name3", "url3",
        "user3", "password3", "param5:value5,param6:value6");

    ConnectorService connectorService = new ConnectorService();
    connectorService.getConnection().add(connection1);
    connectorService.getConnection().add(connection2);
    connectorService.getConnection().add(connection3);

    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("name1", "name2",
        "name3");
  }

  @Test
  public void whenCCIsNotAvailableAndNoMemberExists() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.emptySet()).when(command).findMembers(null, null);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("No connections found");
  }

  @Test
  public void whenCCIsNotAvailableAndMemberExists() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    ConnectorService.Connection connection1 =
        new ConnectorService.Connection("name1", "url1", "user1", "password1", "p1:v1,p2:v2");
    ConnectorService.Connection connection2 =
        new ConnectorService.Connection("name2", "url2", "user2", "password2", "p3:v3,p4:v4");
    ConnectorService.Connection connection3 =
        new ConnectorService.Connection("name3", "url3", "user3", "password3", "p5:v5,p6:v6");

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.singletonList(new CliFunctionResult("server-1",
        Stream.of(connection1, connection2, connection3).collect(toSet()), "success")));

    gfsh.executeAndAssertThat(command, COMMAND + " --member=member1").statusIsSuccess()
        .containsOutput("name1", "name2", "name3");
  }

  @Test
  public void whenCCIsNotAvailableAndNoConnectionFoundOnMember() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.emptyList());

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("No connections found");
  }
}
