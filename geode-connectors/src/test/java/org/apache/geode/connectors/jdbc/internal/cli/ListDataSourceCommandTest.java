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
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand.DATASOURCE_TYPE;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class ListDataSourceCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private ListDataSourceCommand command;

  private JndiBindingsType.JndiBinding binding;
  private List<JndiBindingsType.JndiBinding> bindings;
  private InternalConfigurationPersistenceService clusterConfigService;
  private CacheConfig cacheConfig;
  private List<RegionConfig> regionConfigs;

  private static final String COMMAND = "list data-source";
  private static final String DATA_SOURCE_NAME = "myDataSource";

  @Before
  public void setUp() {
    command = spy(ListDataSourceCommand.class);

    binding = new JndiBindingsType.JndiBinding();
    binding.setJndiName(DATA_SOURCE_NAME);
    binding.setType(DATASOURCE_TYPE.POOLED.getType());
    binding.setConnectionUrl("myURL");
    bindings = new ArrayList<>();
    clusterConfigService = mock(InternalConfigurationPersistenceService.class);
    cacheConfig = mock(CacheConfig.class);
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);
    regionConfigs = new ArrayList<>();
    when(cacheConfig.getRegions()).thenReturn(regionConfigs);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
  }

  @Test
  public void noArgsReturnsSuccess() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess();
  }

  @Test
  public void listDataSourceWithNoClusterConfigurationServerFails() {
    doReturn(null).when(command).getConfigurationPersistenceService();

    ResultModel result = command.listDataSources();

    assertThat(result.getStatus()).isEqualTo(Status.ERROR);
    assertThat(result.toString()).contains("Cluster configuration service must be enabled.");
  }

  @Test
  public void listDataSourceWithNoClusterConfigIsOkWithNoDataSources() {
    doReturn(null).when(clusterConfigService).getCacheConfig(any());

    ResultModel result = command.listDataSources();

    assertThat(result.getStatus()).isEqualTo(Status.OK);
    assertThat(result.toString()).contains("No data sources found");
  }

  @Test
  public void listDataSourceWithUnsupportedTypeIgnoresIt() {
    binding.setType(DATASOURCE_TYPE.MANAGED.getType());

    ResultModel result = command.listDataSources();

    assertThat(result.getStatus()).isEqualTo(Status.OK);
    assertThat(result.toString()).doesNotContain(DATA_SOURCE_NAME);
  }

  @Test
  public void listDataSourceWithSimpleTypeReturnsPooledFalse() {
    binding.setType(DATASOURCE_TYPE.SIMPLE.getType());

    ResultModel result = command.listDataSources();

    TabularResultModel section =
        result.getTableSection(ListDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(0))
        .isEqualTo(Arrays.asList(DATA_SOURCE_NAME, "false", "false", "myURL"));
  }

  @Test
  public void listDataSourceWithPooledTypeReturnsPooledTrue() {
    binding.setType(DATASOURCE_TYPE.POOLED.getType());

    ResultModel result = command.listDataSources();

    TabularResultModel section =
        result.getTableSection(ListDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(0))
        .isEqualTo(Arrays.asList(DATA_SOURCE_NAME, "true", "false", "myURL"));
  }

  @Test
  public void listDataSourcesReturnsInfoOnSingleDataSource() {
    ResultModel result = command.listDataSources();

    TabularResultModel section =
        result.getTableSection(ListDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(0))
        .isEqualTo(Arrays.asList(DATA_SOURCE_NAME, "true", "false", "myURL"));
  }

  @Test
  public void listDataSourcesReturnsInfoOnMultipleDataSources() {
    JndiBinding binding2 = new JndiBindingsType.JndiBinding();
    binding2.setJndiName("myDataSource2");
    binding2.setType(DATASOURCE_TYPE.SIMPLE.getType());
    binding2.setConnectionUrl("myURL2");
    bindings.add(binding2);

    ResultModel result = command.listDataSources();

    TabularResultModel section =
        result.getTableSection(ListDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(0))
        .isEqualTo(Arrays.asList(DATA_SOURCE_NAME, "true", "false", "myURL"));
    assertThat(section.getValuesInRow(1))
        .isEqualTo(Arrays.asList("myDataSource2", "false", "false", "myURL2"));
  }

  @Test
  public void isDataSourceUsedByRegionGivenNoRegionsReturnsFalse() {
    regionConfigs.clear();

    boolean result = command.isDataSourceUsedByRegion(cacheConfig, "");

    assertThat(result).isFalse();
  }

  @Test
  public void isDataSourceUsedByRegionGivenRegionConfigWithNoCustomRegionElementsReturnsFalse() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getCustomRegionElements()).thenReturn(Collections.emptyList());
    regionConfigs.add(regionConfig);

    boolean result = command.isDataSourceUsedByRegion(cacheConfig, "");

    assertThat(result).isFalse();
  }

  @Test
  public void isDataSourceUsedByRegionGivenRegionConfigWithNonRegionMappingElementReturnsFalse() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getCustomRegionElements())
        .thenReturn(Collections.singletonList(mock(CacheElement.class)));
    regionConfigs.add(regionConfig);

    boolean result = command.isDataSourceUsedByRegion(cacheConfig, "");

    assertThat(result).isFalse();
  }

  @Test
  public void isDataSourceUsedByRegionGivenRegionConfigWithRegionMappingForOtherDataSourceReturnsFalse() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getCustomRegionElements())
        .thenReturn(Collections.singletonList(mock(RegionMapping.class)));
    regionConfigs.add(regionConfig);

    boolean result = command.isDataSourceUsedByRegion(cacheConfig, "bogusDataSource");

    assertThat(result).isFalse();
  }

  @Test
  public void isDataSourceUsedByRegionGivenRegionConfigWithRegionMappingForDataSourceReturnsTrue() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getName()).thenReturn("regionName");
    RegionMapping regionMapping = mock(RegionMapping.class);
    when(regionMapping.getDataSourceName()).thenReturn("dataSourceName");
    when(regionConfig.getCustomRegionElements())
        .thenReturn(Collections.singletonList(regionMapping));
    regionConfigs.add(regionConfig);

    boolean result = command.isDataSourceUsedByRegion(cacheConfig, "dataSourceName");

    assertThat(result).isTrue();
  }

  @Test
  public void isDataSourceUsedByRegionGivenMultipleRegionConfigsReturnsTrue() {
    RegionMapping regionMapping;
    {
      RegionConfig regionConfig1 = mock(RegionConfig.class, "regionConfig1");
      when(regionConfig1.getName()).thenReturn("regionName1");
      regionMapping = mock(RegionMapping.class, "regionMapping1");
      when(regionMapping.getDataSourceName()).thenReturn("dataSourceName");
      when(regionConfig1.getCustomRegionElements())
          .thenReturn(Arrays.asList(regionMapping));
      regionConfigs.add(regionConfig1);
    }
    {
      RegionConfig regionConfig2 = mock(RegionConfig.class, "regionConfig2");
      when(regionConfig2.getName()).thenReturn("regionName2");
      regionMapping = mock(RegionMapping.class, "regionMapping2");
      when(regionMapping.getDataSourceName()).thenReturn("otherDataSourceName");
      when(regionConfig2.getCustomRegionElements())
          .thenReturn(Arrays.asList(regionMapping));
      regionConfigs.add(regionConfig2);
    }
    {
      RegionConfig regionConfig3 = mock(RegionConfig.class, "regionConfig3");
      when(regionConfig3.getName()).thenReturn("regionName3");
      regionMapping = mock(RegionMapping.class, "regionMapping3");
      when(regionMapping.getDataSourceName()).thenReturn("dataSourceName");
      when(regionConfig3.getCustomRegionElements())
          .thenReturn(Arrays.asList(regionMapping));
      regionConfigs.add(regionConfig3);
    }

    boolean result = command.isDataSourceUsedByRegion(cacheConfig, "dataSourceName");

    assertThat(result).isTrue();
  }

  @Test
  public void listDataSourcesWithNoRegionsUsingItReturnsResultWithInUseFalse() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getCustomRegionElements())
        .thenReturn(Collections.singletonList(mock(RegionMapping.class)));
    regionConfigs.add(regionConfig);

    ResultModel result = command.listDataSources();

    TabularResultModel section =
        result.getTableSection(ListDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(0))
        .isEqualTo(Arrays.asList(DATA_SOURCE_NAME, "true", "false", "myURL"));
  }

  @Test
  public void listDataSourcesWithRegionsUsingItReturnsResultWithInUseTrue() {
    RegionMapping regionMapping;
    {
      RegionConfig regionConfig1 = mock(RegionConfig.class, "regionConfig1");
      when(regionConfig1.getName()).thenReturn("regionName1");
      regionMapping = mock(RegionMapping.class, "regionMapping1");
      when(regionMapping.getDataSourceName()).thenReturn(DATA_SOURCE_NAME);
      when(regionConfig1.getCustomRegionElements())
          .thenReturn(Arrays.asList(regionMapping));
      regionConfigs.add(regionConfig1);
    }
    {
      RegionConfig regionConfig2 = mock(RegionConfig.class, "regionConfig2");
      when(regionConfig2.getName()).thenReturn("regionName2");
      regionMapping = mock(RegionMapping.class, "regionMapping2");
      when(regionMapping.getDataSourceName()).thenReturn("otherDataSourceName");
      when(regionConfig2.getCustomRegionElements())
          .thenReturn(Arrays.asList(regionMapping));
      regionConfigs.add(regionConfig2);
    }
    {
      RegionConfig regionConfig3 = mock(RegionConfig.class, "regionConfig3");
      when(regionConfig3.getName()).thenReturn("regionName3");
      regionMapping = mock(RegionMapping.class, "regionMapping3");
      when(regionMapping.getDataSourceName()).thenReturn(DATA_SOURCE_NAME);
      when(regionConfig3.getCustomRegionElements())
          .thenReturn(Arrays.asList(regionMapping));
      regionConfigs.add(regionConfig3);
    }

    ResultModel result = command.listDataSources();

    TabularResultModel section =
        result.getTableSection(ListDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(0))
        .isEqualTo(Arrays.asList(DATA_SOURCE_NAME, "true", "true", "myURL"));
  }
}
