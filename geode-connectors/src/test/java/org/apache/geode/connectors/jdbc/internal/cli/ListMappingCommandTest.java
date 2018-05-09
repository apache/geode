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
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)

public class ListMappingCommandTest {
  public static final String COMMAND = "list jdbc-mappings";
  private ListMappingCommand command;
  private ConfigurationPersistenceService ccService;
  private CacheConfig cacheConfig;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(ListMappingCommand.class);
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
        .containsOutput("No mappings found");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectorServiceFound() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("(Experimental) \\nNo mappings found");
  }

  @Test
  public void whenCCServiceIsRunningAndNoConnectionFound() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();

    ConnectorService connectorService = mock(ConnectorService.class);
    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("(Experimental) \\nNo mappings found");
  }

  @Test
  public void whenCCIsAvailable() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();

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

    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("region1",
        "region2");
  }

  @Test
  public void whenCCIsNotAvailableAndNoMemberExists() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.emptySet()).when(command).findMembers(null, null);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("No mappings found");
  }

  @Test
  public void whenCCIsNotAvailableAndMemberExists() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    ConnectorService.RegionMapping mapping1 =
        new ConnectorService.RegionMapping("region1", "class1", "table1", "name1", true);
    mapping1.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field1", "value1"));
    mapping1.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field2", "value2"));

    ConnectorService.RegionMapping mapping2 =
        new ConnectorService.RegionMapping("region2", "class2", "table2", "name2", true);
    mapping2.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field3", "value3"));
    mapping2.getFieldMapping()
        .add(new ConnectorService.RegionMapping.FieldMapping("field4", "value4"));

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.singletonList(new CliFunctionResult("server-1",
        Stream.of(mapping1, mapping2).collect(toSet()), "success")));

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("region1",
        "region2");
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
        .containsOutput("No mappings found");
  }
}
