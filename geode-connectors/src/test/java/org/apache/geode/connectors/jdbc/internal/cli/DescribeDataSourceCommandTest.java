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
import org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding.ConfigProperty;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand.DATASOURCE_TYPE;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DescribeDataSourceCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private DescribeDataSourceCommand command;

  private JndiBindingsType.JndiBinding binding;
  private List<JndiBindingsType.JndiBinding> bindings;
  private InternalConfigurationPersistenceService clusterConfigService;
  private CacheConfig cacheConfig;
  private List<RegionConfig> regionConfigs;

  private static final String COMMAND = "describe data-source";
  private static final String DATA_SOURCE_NAME = "myDataSource";

  @Before
  public void setUp() {
    command = spy(DescribeDataSourceCommand.class);

    binding = new JndiBindingsType.JndiBinding();
    binding.setJndiName(DATA_SOURCE_NAME);
    binding.setType(DATASOURCE_TYPE.POOLED.getType());
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
  public void missingMandatory() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("Invalid command: describe data-source");
  }

  @Test
  public void nameWorks() {
    gfsh.executeAndAssertThat(command, COMMAND + " --name=" + DATA_SOURCE_NAME).statusIsSuccess();
  }

  @Test
  public void describeDataSourceWithNoClusterConfigurationServerFails() {
    doReturn(null).when(command).getConfigurationPersistenceService();

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    assertThat(result.getStatus()).isEqualTo(Status.ERROR);
    assertThat(result.toString()).contains("Cluster configuration service must be enabled.");
  }

  @Test
  public void describeDataSourceWithNoClusterConfigFails() {
    doReturn(null).when(clusterConfigService).getCacheConfig(any());

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    assertThat(result.getStatus()).isEqualTo(Status.ERROR);
    assertThat(result.toString()).contains("Data source: " + DATA_SOURCE_NAME + " not found");
  }

  @Test
  public void describeDataSourceWithWrongNameFails() {
    ResultModel result = command.describeDataSource("bogusName");

    assertThat(result.getStatus()).isEqualTo(Status.ERROR);
    assertThat(result.toString()).contains("Data source: bogusName not found");
  }

  @Test
  public void describeDataSourceWithUnsupportedTypeFails() {
    binding.setType(DATASOURCE_TYPE.MANAGED.getType());

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    assertThat(result.getStatus()).isEqualTo(Status.ERROR);
    assertThat(result.toString()).contains("Unknown data source type: ManagedDataSource");
  }

  @Test
  public void describeDataSourceWithSimpleTypeReturnsPooledFalse() {
    binding.setType(DATASOURCE_TYPE.SIMPLE.getType());

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    TabularResultModel section =
        result.getTableSection(DescribeDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(3)).isEqualTo(Arrays.asList("pooled", "false"));
  }

  @Test
  public void describeDataSourceWithPooledTypeReturnsPooledTrue() {
    binding.setType(DATASOURCE_TYPE.POOLED.getType());

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    TabularResultModel section =
        result.getTableSection(DescribeDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(3)).isEqualTo(Arrays.asList("pooled", "true"));
  }

  @Test
  public void describeDataSourceTypeReturnsName() {
    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    TabularResultModel section =
        result.getTableSection(DescribeDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(0)).isEqualTo(Arrays.asList("name", DATA_SOURCE_NAME));
  }

  @Test
  public void describeDataSourceWithUrlReturnsUrl() {
    binding.setConnectionUrl("myUrl");

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    TabularResultModel section =
        result.getTableSection(DescribeDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(1)).isEqualTo(Arrays.asList("url", "myUrl"));
  }

  @Test
  public void describeDataSourceWithUsernameReturnsUsername() {
    binding.setUserName("myUserName");

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    TabularResultModel section =
        result.getTableSection(DescribeDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(2)).isEqualTo(Arrays.asList("username", "myUserName"));
  }

  @Test
  public void describeDataSourceWithPooledDataSourceFactoryClassShowsItInTheResult() {
    binding.setType(DATASOURCE_TYPE.POOLED.getType());
    binding.setConnPooledDatasourceClass("myPooledDataSourceFactoryClass");

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    TabularResultModel section =
        result.getTableSection(DescribeDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(4)).isEqualTo(
        Arrays.asList("pooled-data-source-factory-class", "myPooledDataSourceFactoryClass"));
  }

  @Test
  public void describeDataSourceWithPasswordDoesNotShowPasswordInResult() {
    binding.setType(DATASOURCE_TYPE.POOLED.getType());
    binding.setPassword("myPassword");

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    assertThat(result.toString()).doesNotContain("myPassword");
  }

  @Test
  public void describeDataSourceWithPoolPropertiesDoesNotShowsItInTheResult() {
    binding.setType(DATASOURCE_TYPE.SIMPLE.getType());
    List<ConfigProperty> configProperties = binding.getConfigProperties();
    configProperties.add(new ConfigProperty("name1", "value1"));

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    assertThat(result.toString()).doesNotContain("name1");
    assertThat(result.toString()).doesNotContain("value1");
  }

  @Test
  public void describeDataSourceWithPoolPropertiesShowsItInTheResult() {
    binding.setType(DATASOURCE_TYPE.POOLED.getType());
    List<ConfigProperty> configProperties = binding.getConfigProperties();
    configProperties.add(new ConfigProperty("name1", "value1"));
    configProperties.add(new ConfigProperty("name2", "value2"));

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    TabularResultModel section =
        result.getTableSection(DescribeDataSourceCommand.DATA_SOURCE_PROPERTIES_SECTION);
    assertThat(section.getValuesInRow(5)).isEqualTo(Arrays.asList("name1", "value1"));
    assertThat(section.getValuesInRow(6)).isEqualTo(Arrays.asList("name2", "value2"));
  }

  @Test
  public void getRegionsThatUseDataSourceGivenNoRegionsReturnsEmptyList() {
    regionConfigs.clear();

    List<String> result = command.getRegionsThatUseDataSource(cacheConfig, "");

    assertThat(result).isEmpty();
  }

  @Test
  public void getRegionsThatUseDataSourceGivenRegionConfigWithNoCustomRegionElementsReturnsEmptyList() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getCustomRegionElements()).thenReturn(Collections.emptyList());
    regionConfigs.add(regionConfig);

    List<String> result = command.getRegionsThatUseDataSource(cacheConfig, "");

    assertThat(result).isEmpty();
  }

  @Test
  public void getRegionsThatUseDataSourceGivenRegionConfigWithNonRegionMappingElementReturnsEmptyList() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getCustomRegionElements())
        .thenReturn(Collections.singletonList(mock(CacheElement.class)));
    regionConfigs.add(regionConfig);

    List<String> result = command.getRegionsThatUseDataSource(cacheConfig, "");

    assertThat(result).isEmpty();
  }

  @Test
  public void getRegionsThatUseDataSourceGivenRegionConfigWithRegionMappingForOtherDataSourceReturnsEmptyList() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getCustomRegionElements())
        .thenReturn(Collections.singletonList(mock(RegionMapping.class)));
    regionConfigs.add(regionConfig);

    List<String> result = command.getRegionsThatUseDataSource(cacheConfig, "bogusDataSource");

    assertThat(result).isEmpty();
  }

  @Test
  public void getRegionsThatUseDataSourceGivenRegionConfigWithRegionMappingForDataSourceReturnsRegionName() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getName()).thenReturn("regionName");
    RegionMapping regionMapping = mock(RegionMapping.class);
    when(regionMapping.getDataSourceName()).thenReturn("dataSourceName");
    when(regionConfig.getCustomRegionElements())
        .thenReturn(Collections.singletonList(regionMapping));
    regionConfigs.add(regionConfig);

    List<String> result = command.getRegionsThatUseDataSource(cacheConfig, "dataSourceName");

    assertThat(result).isEqualTo(Collections.singletonList("regionName"));
  }

  @Test
  public void getRegionsThatUseDataSourceGivenMultipleRegionConfigsReturnsAllRegionNames() {
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

    List<String> result = command.getRegionsThatUseDataSource(cacheConfig, "dataSourceName");

    assertThat(result).isEqualTo(Arrays.asList("regionName1", "regionName3"));
  }

  @Test
  public void describeDataSourceWithNoRegionsUsingItReturnsResultWithNoRegionsUsingIt() {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getCustomRegionElements())
        .thenReturn(Collections.singletonList(mock(RegionMapping.class)));
    regionConfigs.add(regionConfig);

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    InfoResultModel regionsUsingSection = (InfoResultModel) result
        .getSection(DescribeDataSourceCommand.REGIONS_USING_DATA_SOURCE_SECTION);
    assertThat(regionsUsingSection.getContent())
        .isEqualTo(Arrays.asList("no regions are using " + DATA_SOURCE_NAME));
  }

  @Test
  public void describeDataSourceWithRegionsUsingItReturnsResultWithRegionNames() {
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

    ResultModel result = command.describeDataSource(DATA_SOURCE_NAME);

    InfoResultModel regionsUsingSection = (InfoResultModel) result
        .getSection(DescribeDataSourceCommand.REGIONS_USING_DATA_SOURCE_SECTION);
    assertThat(regionsUsingSection.getContent())
        .isEqualTo(Arrays.asList("regionName1", "regionName3"));
  }
}
