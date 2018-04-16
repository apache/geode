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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)

public class ListConnectionCommandTest {
  public static final String COMMAND = "list jdbc-connection";
  private ListConnectionCommand command;
  private ClusterConfigurationService ccService;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(ListConnectionCommand.class);
    ccService = mock(InternalClusterConfigurationService.class);
    doReturn(ccService).when(command).getConfigurationService();
  }

  @Test
  public void whenCCServiceIsNotAvailable() {
    doReturn(null).when(command).getConfigurationService();
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("cluster configuration service is not running");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectorServiceFound() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("(Experimental) \\nNo connections found");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectionFound() {
    ConnectorService connectorService = mock(ConnectorService.class);
    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("(Experimental) \\nNo connections found");
  }

  @Test
  public void successfulResult() {
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

    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("name1", "name2",
        "name3");
  }
}
