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

public class DescribeMappingCommandTest {
  public static final String COMMAND = "describe jdbc-mapping --region=region ";
  private DescribeMappingCommand command;
  private ClusterConfigurationService ccService;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(DescribeMappingCommand.class);
    ccService = mock(InternalClusterConfigurationService.class);
    doReturn(ccService).when(command).getConfigurationService();
  }

  @Test
  public void requiredParameter() {
    gfsh.executeAndAssertThat(command, "describe jdbc-mapping").statusIsError()
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
        .containsOutput("mapping for region 'region' not found");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectionFound() {
    ConnectorService connectorService = mock(ConnectorService.class);
    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("mapping for region 'region' not found");
  }

  @Test
  public void successfulResult() {
    // mapping found in CC
    ConnectorService.RegionMapping mapping =
        new ConnectorService.RegionMapping("region", "class1", "table1", "name1", true);
    mapping.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field1", "value1"));
    mapping.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field2", "value2"));

    ConnectorService connectorService = mock(ConnectorService.class);
    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);
    when(ccService.findIdentifiable(any(), any())).thenReturn(mapping);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("region", "region")
        .containsOutput("connection", "name1").containsOutput("table", "table1")
        .containsOutput("pdx-class-name", "class1")
        .containsOutput("value-contains-primary-key", "true").containsOutput("field1", "value1")
        .containsOutput("field2", "value2");
  }

  @Test
  public void whenCCIsNotAvailableAndMemberIsNotSpecified() {
    doReturn(null).when(command).getConfigurationService();
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("Use --member option to describe mappings");
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

    ConnectorService.RegionMapping mapping =
        new ConnectorService.RegionMapping("region", "class1", "table1", "name1", true);
    mapping.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field1", "value1"));
    mapping.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field2", "value2"));

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(DistributedMember.class));
    when(rc.getResult()).thenReturn(Collections.singletonList(mapping));

    gfsh.executeAndAssertThat(command, COMMAND + " --member=member1").statusIsSuccess()
        .containsOutput("region", "region").containsOutput("connection", "name1")
        .containsOutput("table", "table1").containsOutput("pdx-class-name", "class1")
        .containsOutput("value-contains-primary-key", "true").containsOutput("field1", "value1")
        .containsOutput("field2", "value2");
  }
}
