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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliFunctionResult;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class AlterConnectionCommandTest {
  public static final String COMMAND = "alter jdbc-connection --name=name ";
  private AlterConnectionCommand command;
  private List<CliFunctionResult> results;
  private CliFunctionResult result;
  private ConfigurationPersistenceService ccService;
  private ConnectorService.Connection connection;
  private List<ConnectorService.Connection> connections;
  private ConnectorService connectorService;
  private CacheConfig cacheConfig;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() throws Exception {
    results = new ArrayList<>();
    command = spy(AlterConnectionCommand.class);
    doReturn(Collections.EMPTY_SET).when(command).getMembers(any(), any());
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());
    result = mock(CliFunctionResult.class);
    when(result.isSuccessful()).thenReturn(true);
    when(result.getMemberIdOrName()).thenReturn("memberName");
    when(result.getStatusMessage()).thenReturn("message");
    ccService = mock(InternalConfigurationPersistenceService.class);
    cacheConfig = mock(CacheConfig.class);
    when(ccService.getCacheConfig("cluster")).thenReturn(cacheConfig);
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    connectorService = mock(ConnectorService.class);
    connections = new ArrayList<>();
    connection = new ConnectorService.Connection();
    connection.setName("name");
  }

  @Test
  public void requiredParameter() {
    gfsh.executeAndAssertThat(command, "alter jdbc-connection").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void whenValuesNotSpecified() {
    GfshParseResult result = gfsh.parse(COMMAND);
    assertThat(result.getParamValue("url")).isNull();
    assertThat(result.getParamValue("user")).isNull();
    assertThat(result.getParamValue("password")).isNull();
    assertThat(result.getParamValue("params")).isNull();
  }

  @Test
  public void whenValuesSpecifiedAsEmptyString() {
    GfshParseResult result = gfsh.parse(COMMAND + " --url='' --user='' --password='' --params=''");
    assertThat(result.getParamValue("url")).isEqualTo("");
    assertThat(result.getParamValue("user")).isEqualTo("");
    assertThat(result.getParamValue("password")).isEqualTo("");
    assertThat(result.getParamValue("params")).isEqualTo(new String[] {""});
  }

  @Test
  public void whenCCServiceIsNotAvailable() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    results.add(result);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess();
    verify(command).executeAndGetFunctionResult(any(), any(), any());
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectorServiceFound() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("connection with name 'name' does not exist.");
    verify(command, times(0)).executeAndGetFunctionResult(any(), any(), any());
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectionFound() {
    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("connection with name 'name' does not exist.");
    verify(command, times(0)).executeAndGetFunctionResult(any(), any(), any());
  }

  @Test
  public void noSuccessfulResult() {
    // connection found in CC
    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);
    when(connectorService.getConnection()).thenReturn(connections);
    connections.add(connection);
    // result is not successful
    when(result.isSuccessful()).thenReturn(false);
    results.add(result);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError();
    verify(command).executeAndGetFunctionResult(any(), any(), any());
  }

  @Test
  public void successfulResult() {
    // connection found in CC
    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);
    when(connectorService.getConnection()).thenReturn(connections);
    connections.add(connection);

    // result is not successful
    when(result.isSuccessful()).thenReturn(true);
    results.add(result);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess();
    verify(command).executeAndGetFunctionResult(any(), any(), any());
  }
}
