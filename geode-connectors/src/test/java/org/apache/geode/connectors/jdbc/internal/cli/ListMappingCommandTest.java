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

public class ListMappingCommandTest {
  public static final String COMMAND = "list jdbc-mappings";
  private ListMappingCommand command;
  private ClusterConfigurationService ccService;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(ListMappingCommand.class);
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
        .containsOutput("No mappings found");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectionFound() {
    ConnectorService connectorService = mock(ConnectorService.class);
    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("No mappings found");
  }

  @Test
  public void successfulResult() {
    // mappings found in CC
    ConnectorService.RegionMapping mapping1 =
        new ConnectorService.RegionMapping("region1", "class1", "table1", "name1", true);
    mapping1.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field1", "value1"));
    ConnectorService.RegionMapping mapping2 =
        new ConnectorService.RegionMapping("region2", "class2", "table2", "name2", true);
    mapping2.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field2", "value2"));

    ConnectorService connectorService = new ConnectorService();
    connectorService.getRegionMapping().add(mapping1);
    connectorService.getRegionMapping().add(mapping2);

    when(ccService.getCustomCacheElement(any(), any(), any())).thenReturn(connectorService);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("region1",
        "region2");
  }
}
