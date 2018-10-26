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
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class DescribeMappingCommandTest {
  public static final String COMMAND = "describe jdbc-mapping --region=region ";
  private DescribeMappingCommand command;
  private ConfigurationPersistenceService ccService;
  private CacheConfig cacheConfig;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(DescribeMappingCommand.class);
    ccService = mock(InternalConfigurationPersistenceService.class);
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    cacheConfig = mock(CacheConfig.class);
    when(ccService.getCacheConfig("cluster")).thenReturn(cacheConfig);
  }

  @Test
  public void requiredParameter() {
    gfsh.executeAndAssertThat(command, "describe jdbc-mapping").statusIsError()
        .containsOutput("Invalid command");
  }

//  @Test
//  public void whenCCServiceIsRunningAndNoConnectorServiceFound() {
//    doReturn(ccService).when(command).getConfigurationPersistenceService();
//    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
//        .containsOutput("mapping for region 'region' not found");
//  }

//  @Test
//  public void whenCCServiceIsRunningAndNoConnectionFound() {
//    doReturn(ccService).when(command).getConfigurationPersistenceService();
//
//    ConnectorService connectorService = mock(ConnectorService.class);
//    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);
//    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
//        .containsOutput("mapping for region 'region' not found");
//  }

//  @Test
//  public void whenCCIsAvailable() {
//    doReturn(ccService).when(command).getConfigurationPersistenceService();
//
//    // mapping found in CC
//    RegionMapping mapping =
//        new RegionMapping("region", "class1", "table1", "name1", true);
//    mapping.getFieldMapping()
//        .add(new RegionMapping.FieldMapping("field1", "value1"));
//    mapping.getFieldMapping()
//        .add(new RegionMapping.FieldMapping("field2", "value2"));
//
//    ConnectorService connectorService = mock(ConnectorService.class);
//    List<RegionMapping> mappings = new ArrayList<>();
//    when(connectorService.getRegionMapping()).thenReturn(mappings);
//    mappings.add(mapping);
//    when(cacheConfig.findCustomCacheElement(any(), any())).thenReturn(connectorService);
//
//    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("region", "region")
//        .containsOutput("connection", "name1").containsOutput("table", "table1")
//        .containsOutput("pdx-class-name", "class1")
//        .containsOutput("value-contains-primary-key", "true").containsOutput("field1", "value1")
//        .containsOutput("field2", "value2");
//  }

  @Test
  public void whenCCIsNotAvailableAndNoMemberExists() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.emptySet()).when(command).findMembers(null, null);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("mapping for region 'region' not found");
  }

  @Test
  public void whenCCIsNotAvailableAndMemberExists() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    RegionMapping mapping =
        new RegionMapping("region", "class1", "table1", "name1", true);
    mapping.getFieldMapping()
        .add(new RegionMapping.FieldMapping("field1", "value1"));
    mapping.getFieldMapping()
        .add(new RegionMapping.FieldMapping("field2", "value2"));

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(
        Collections.singletonList(new CliFunctionResult("server-1", mapping, "success")));

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("region", "region")
        .containsOutput("connection", "name1").containsOutput("table", "table1")
        .containsOutput("pdx-class-name", "class1")
        .containsOutput("value-contains-primary-key", "true").containsOutput("field1", "value1")
        .containsOutput("field2", "value2");
  }

  @Test
  public void whenCCIsNotAvailableAndNoMappingFoundOnMember() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.emptyList());

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("mapping for region 'region' not found");
  }
}
