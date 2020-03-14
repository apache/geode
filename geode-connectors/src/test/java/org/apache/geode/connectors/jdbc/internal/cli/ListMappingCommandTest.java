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
import static org.apache.geode.connectors.jdbc.internal.cli.ListMappingCommand.NO_MAPPINGS_FOUND;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class ListMappingCommandTest {
  final String TEST_GROUP1 = "testGroup1";
  public final String COMMAND = "list jdbc-mappings";
  public final String COMMAND_FOR_GROUP = COMMAND + " --groups=" + TEST_GROUP1;
  private ListMappingCommand command;
  private ConfigurationPersistenceService configService =
      mock(ConfigurationPersistenceService.class);
  private CacheConfig cacheConfig = mock(CacheConfig.class);
  RegionConfig region1Config = mock(RegionConfig.class);
  RegionConfig region2Config = mock(RegionConfig.class);

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(ListMappingCommand.class);
    doReturn(configService).when(command).getConfigurationPersistenceService();
  }

  @Test
  public void whenNoCacheConfig() {
    when(configService.getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG))
        .thenReturn(null);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("Cache Configuration not found.");
  }

  @Test
  public void whenClusterConfigDisabled() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("Cluster Configuration must be enabled.");
  }

  @Test
  public void whenNoMappingExists() {
    when(configService.getCacheConfig(eq(ConfigurationPersistenceService.CLUSTER_CONFIG)))
        .thenReturn(cacheConfig);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput(NO_MAPPINGS_FOUND);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void whenMappingExists() {
    when(configService.getCacheConfig(eq(ConfigurationPersistenceService.CLUSTER_CONFIG)))
        .thenReturn(cacheConfig);
    List<RegionConfig> regions = new ArrayList<>();
    regions.add(region1Config);
    regions.add(region2Config);
    when(region1Config.getName()).thenReturn("region1");
    when(region2Config.getName()).thenReturn("region2");

    when(cacheConfig.getRegions()).thenReturn(regions);
    RegionMapping mapping1 =
        new RegionMapping("region1", "class1", "table1", "name1", null, null, null);
    RegionMapping mapping2 =
        new RegionMapping("region2", "class2", "table2", "name2", null, null, null);
    List<CacheElement> mappingList1 = new ArrayList<>();
    mappingList1.add(mapping1);
    List<CacheElement> mappingList2 = new ArrayList<>();
    mappingList2.add(mapping2);
    when(region1Config.getCustomRegionElements()).thenReturn(mappingList1);
    when(region2Config.getCustomRegionElements()).thenReturn(mappingList2);

    ResultCollector<CliFunctionResult, List<CliFunctionResult>> rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.singletonList(new CliFunctionResult("server-1",
        Stream.of(mapping1, mapping2).collect(toSet()), "success")));

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("region1",
        "region2");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void whenMappingExistsForServerGroup() {
    when(configService.getCacheConfig(TEST_GROUP1)).thenReturn(cacheConfig);
    List<RegionConfig> regions = new ArrayList<>();
    regions.add(region1Config);
    regions.add(region2Config);
    when(region1Config.getName()).thenReturn("region1");
    when(region2Config.getName()).thenReturn("region2");

    when(cacheConfig.getRegions()).thenReturn(regions);
    RegionMapping mapping1 =
        new RegionMapping("region1", "class1", "table1", "name1", null, null, null);
    RegionMapping mapping2 =
        new RegionMapping("region2", "class2", "table2", "name2", null, null, null);
    List<CacheElement> mappingList1 = new ArrayList<>();
    mappingList1.add(mapping1);
    List<CacheElement> mappingList2 = new ArrayList<>();
    mappingList2.add(mapping2);
    when(region1Config.getCustomRegionElements()).thenReturn(mappingList1);
    when(region2Config.getCustomRegionElements()).thenReturn(mappingList2);

    ResultCollector<CliFunctionResult, List<CliFunctionResult>> rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.singletonList(new CliFunctionResult("server-1",
        Stream.of(mapping1, mapping2).collect(toSet()), "success")));

    gfsh.executeAndAssertThat(command, COMMAND_FOR_GROUP).statusIsSuccess().containsOutput(
        "region1",
        "region2");
  }
}
