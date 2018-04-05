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
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionExecutionResult;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class AlterMappingCommandTest {
  public static final String COMMAND = "alter jdbc-mapping --region='region' ";
  private AlterMappingCommand command;
  private List<CliFunctionExecutionResult> results;
  private CliFunctionExecutionResult result;
  private ClusterConfigurationService ccService;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() throws Exception {
    command = spy(AlterMappingCommand.class);
    results = new ArrayList<>();
    doReturn(Collections.EMPTY_SET).when(command).getMembers(any(), any());
    doReturn(results).when(command).executeAndGetFunctionExecutionResult(any(), any(), any());
    result = mock(CliFunctionExecutionResult.class);
    when(result.isSuccessful()).thenReturn(true);
    when(result.getMemberName()).thenReturn("memberName");
    when(result.getMessage()).thenReturn("message");
    ccService = mock(InternalClusterConfigurationService.class);
    doReturn(ccService).when(command).getConfigurationService();
  }

  @Test
  public void requiredParameter() {
    gfsh.executeAndAssertThat(command, "alter jdbc-mapping").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void valuesAreParsedAsExpected() {
    GfshParseResult parseResult = gfsh.parse("alter jdbc-mapping --region='region' --connection='' "
        + "--table='' --pdx-class-name='' " + "--field-mapping=''");

    String[] mappings = (String[]) parseResult.getParamValue("field-mapping");
    assertThat(mappings).hasSize(1);
    assertThat(mappings[0]).isEqualTo("");
    assertThat(parseResult.getParamValue("region")).isEqualTo("region");
    assertThat(parseResult.getParamValue("connection")).isEqualTo("");
    assertThat(parseResult.getParamValue("table")).isEqualTo("");
    assertThat(parseResult.getParamValue("pdx-class-name")).isEqualTo("");

    parseResult = gfsh.parse("alter jdbc-mapping --region=testRegion-1 --connection=connection "
        + "--table=myTable --pdx-class-name=myPdxClass " + "--field-mapping");
    mappings = (String[]) parseResult.getParamValue("field-mapping");
    assertThat(mappings).hasSize(1);
    assertThat(mappings[0]).isEqualTo("");

    parseResult = gfsh.parse("alter jdbc-mapping --region=testRegion-1 --connection=connection "
        + "--table=myTable --pdx-class-name=myPdxClass ");
    mappings = (String[]) parseResult.getParamValue("field-mapping");
    assertThat(mappings).isNull();
  }

  @Test
  public void whenCCServiceIsNotAvailable() {
    doReturn(null).when(command).getConfigurationService();
    results.add(result);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess();
    verify(command).executeAndGetFunctionExecutionResult(any(), any(), any());
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectorServiceFound() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("mapping with name 'region' does not exist.");
    verify(command, times(0)).executeAndGetFunctionExecutionResult(any(), any(), any());
  }

  @Test
  public void whenCCServiceIsRunningAndNoMappingFound() {
    ConnectorService connectorService = mock(ConnectorService.class);
    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("mapping with name 'region' does not exist.");
    verify(command, times(0)).executeAndGetFunctionExecutionResult(any(), any(), any());
  }

  @Test
  public void noSuccessfulResult() {
    // mapping found in CC
    ConnectorService connectorService = mock(ConnectorService.class);
    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);
    when(ccService.findIdentifiable(any(), any()))
        .thenReturn(mock(ConnectorService.RegionMapping.class));

    // result is not successful
    when(result.isSuccessful()).thenReturn(false);
    results.add(result);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError();
    verify(command).executeAndGetFunctionExecutionResult(any(), any(), any());
  }

  @Test
  public void successfulResult() {
    // mapping found in CC
    ConnectorService connectorService = mock(ConnectorService.class);
    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);
    when(ccService.findIdentifiable(any(), any()))
        .thenReturn(mock(ConnectorService.RegionMapping.class));

    // result is not successful
    when(result.isSuccessful()).thenReturn(true);
    results.add(result);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess();
    verify(command).executeAndGetFunctionExecutionResult(any(), any(), any());
  }
}
