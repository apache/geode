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

import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.CATALOG_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SPECIFIED_ID_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.TABLE_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class DescribeMappingCommandTest {
  public static final String COMMAND = "describe jdbc-mapping --region=region1";
  private DescribeMappingCommand command;

  @Mock
  ConfigurationPersistenceService configurationPersistenceService;

  @Mock
  CacheConfig clusterConfig;

  @Mock
  RegionConfig regionConfig;

  @Mock
  RegionAttributesType regionAttributesType;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(DescribeMappingCommand.class);
    configurationPersistenceService = mock(ConfigurationPersistenceService.class);
    clusterConfig = mock(CacheConfig.class);
    regionConfig = mock(RegionConfig.class);
    regionAttributesType = mock(RegionAttributesType.class);
    when(regionConfig.getRegionAttributes()).thenReturn(regionAttributesType);
    when(command.getConfigurationPersistenceService()).thenReturn(configurationPersistenceService);
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(clusterConfig);
    ArrayList<RegionConfig> regionConfigList = new ArrayList<>();
    regionConfigList.add(regionConfig);
    when(clusterConfig.getRegions()).thenReturn(regionConfigList);
    when(regionConfig.getName()).thenReturn("region1");
  }

  @Test
  public void requiredParameter() {
    gfsh.executeAndAssertThat(command, "describe jdbc-mapping").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void commandFailureWhenClusterConfigServiceNotEnabled() {
    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(true);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    ArrayList<CacheElement> elements = new ArrayList<>();
    elements.add(regionMapping);
    when(command.getConfigurationPersistenceService()).thenReturn(null);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("Cluster Configuration must be enabled.");
  }

  @Test
  public void commandFailureWhenClusterConfigServiceEnabledAndCacheConfigNotFound() {
    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(true);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    ArrayList<CacheElement> elements = new ArrayList<>();
    elements.add(regionMapping);
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(null);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("Cache Configuration not found.");
  }

  @Test
  public void commandFailureWhenClusterConfigServiceEnabledAndCacheConfigNotFoundWithGroup() {
    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(true);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    ArrayList<CacheElement> elements = new ArrayList<>();
    elements.add(regionMapping);
    when(configurationPersistenceService.getCacheConfig("group1")).thenReturn(null);

    gfsh.executeAndAssertThat(command, COMMAND + " --group=group1").statusIsError()
        .containsOutput("Cache Configuration not found for group group1.");
  }

  @Test
  public void commandFailureWhenCacheConfigFoundAndRegionConfigNotFound() {
    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(true);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    when(clusterConfig.getRegions()).thenReturn(new ArrayList<>());

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("A region named region1 must already exist.");
  }

  @Test
  public void commandFailureWhenCacheConfigFoundAndRegionConfigNotFoundWithGroup() {
    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(true);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    when(clusterConfig.getRegions()).thenReturn(new ArrayList<>());
    when(configurationPersistenceService.getCacheConfig("group1")).thenReturn(clusterConfig);

    gfsh.executeAndAssertThat(command, COMMAND + " --groups=group1").statusIsError()
        .containsOutput("A region named region1 must already exist for group group1.");
  }

  @Test
  public void commandSuccessWhenClusterConfigFoundAndRegionConfigFound() {
    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(true);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    ArrayList<CacheElement> elements = new ArrayList<>();
    elements.add(regionMapping);
    when(regionConfig.getCustomRegionElements()).thenReturn(elements);
    when(regionConfig.getRegionAttributes().getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.REPLICATE);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .hasDataSection()
        .hasContent()
        .containsEntry(REGION_NAME, "region1")
        .containsEntry(DATA_SOURCE_NAME, "name1")
        .containsEntry(TABLE_NAME, "table1")
        .containsEntry(PDX_NAME, "class1")
        .containsEntry(ID_NAME, "myId")
        .containsEntry(SPECIFIED_ID_NAME, "true")
        .containsEntry(SCHEMA_NAME, "mySchema")
        .containsEntry(CATALOG_NAME, "myCatalog")
        .containsEntry(SYNCHRONOUS_NAME, "true");
  }

  @Test
  public void commandSuccessWhenClusterConfigFoundAndRegionConfigFoundIdNotSpecified() {
    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(false);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    ArrayList<CacheElement> elements = new ArrayList<>();
    elements.add(regionMapping);
    when(regionConfig.getCustomRegionElements()).thenReturn(elements);
    when(regionConfig.getRegionAttributes().getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.REPLICATE);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .hasDataSection()
        .hasContent()
        .containsEntry(REGION_NAME, "region1")
        .containsEntry(DATA_SOURCE_NAME, "name1").containsEntry(TABLE_NAME, "table1")
        .containsEntry(PDX_NAME, "class1").containsEntry(ID_NAME, "myId")
        .containsEntry(SPECIFIED_ID_NAME, "false")
        .containsEntry(SCHEMA_NAME, "mySchema").containsEntry(CATALOG_NAME, "myCatalog")
        .containsEntry(SYNCHRONOUS_NAME, "true");
  }

  @Test
  public void commandSuccessWhenClusterConfigFoundAndRegionConfigFoundAsync() {

    CacheConfig.AsyncEventQueue asyncEventQueue = mock(CacheConfig.AsyncEventQueue.class);
    ArrayList<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    // Adding multiple mocked objects to the list to demonstrate the ability to distinguish the
    // correct queue later on
    queueList.add(asyncEventQueue);
    queueList.add(asyncEventQueue);
    queueList.add(asyncEventQueue);

    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(true);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    ArrayList<CacheElement> elements = new ArrayList<>();
    elements.add(regionMapping);
    when(regionConfig.getCustomRegionElements()).thenReturn(elements);
    when(regionConfig.getRegionAttributes().getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.REPLICATE);
    when(clusterConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(asyncEventQueue.getId())
        .thenReturn(MappingCommandUtils.createAsyncEventQueueName("region2"))
        .thenReturn(MappingCommandUtils.createAsyncEventQueueName("region1"))
        .thenReturn(MappingCommandUtils.createAsyncEventQueueName("region3"));

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .hasDataSection()
        .hasContent()
        .containsEntry(REGION_NAME, "region1")
        .containsEntry(DATA_SOURCE_NAME, "name1").containsEntry(TABLE_NAME, "table1")
        .containsEntry(PDX_NAME, "class1").containsEntry(ID_NAME, "myId")
        .containsEntry(SPECIFIED_ID_NAME, "true")
        .containsEntry(SCHEMA_NAME, "mySchema").containsEntry(CATALOG_NAME, "myCatalog")
        .containsEntry("synchronous", "false");
  }

  @Test
  public void commandSuccessWithFieldMappings() {
    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(false);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    FieldMapping fieldMapping =
        new FieldMapping("pdxName1", "pdxType1", "jdbcName1", "jdbcType1", true);
    regionMapping.addFieldMapping(fieldMapping);
    FieldMapping fieldMapping2 =
        new FieldMapping("veryLongpdxName2", "pdxType2", "veryLongjdbcName2", "jdbcType2", false);
    regionMapping.addFieldMapping(fieldMapping2);
    ArrayList<CacheElement> elements = new ArrayList<>();
    elements.add(regionMapping);
    when(regionConfig.getCustomRegionElements()).thenReturn(elements);
    when(regionConfig.getRegionAttributes().getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.REPLICATE);

    CommandResultAssert resultAssert =
        gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess();
    resultAssert
        .hasDataSection()
        .hasContent()
        .containsEntry(REGION_NAME, "region1")
        .containsEntry(DATA_SOURCE_NAME, "name1")
        .containsEntry(TABLE_NAME, "table1")
        .containsEntry(PDX_NAME, "class1")
        .containsEntry(ID_NAME, "myId")
        .containsEntry(SPECIFIED_ID_NAME, "false")
        .containsEntry(SCHEMA_NAME, "mySchema")
        .containsEntry(CATALOG_NAME, "myCatalog")
        .containsEntry("synchronous", "true");
    resultAssert.hasTableSection()
        .hasAnyRow()
        .containsExactly("pdxName1", "pdxType1", "jdbcName1", "jdbcType1", "true")
        .hasAnyRow()
        .containsExactly("veryLongpdxName2", "pdxType2", "veryLongjdbcName2", "jdbcType2", "false");
  }

  @Test
  public void whenMemberExistsForGroup() {
    RegionMapping regionMapping = new RegionMapping();
    regionMapping.setRegionName("region1");
    regionMapping.setPdxName("class1");
    regionMapping.setTableName("table1");
    regionMapping.setDataSourceName("name1");
    regionMapping.setIds("myId");
    regionMapping.setSpecifiedIds(false);
    regionMapping.setCatalog("myCatalog");
    regionMapping.setSchema("mySchema");
    ArrayList<CacheElement> elements = new ArrayList<>();
    elements.add(regionMapping);
    when(regionConfig.getCustomRegionElements()).thenReturn(elements);
    when(regionConfig.getRegionAttributes().getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.REPLICATE);
    when(configurationPersistenceService.getCacheConfig("group1")).thenReturn(clusterConfig);



    gfsh.executeAndAssertThat(command, COMMAND + " --groups=group1").statusIsSuccess()
        .hasDataSection()
        .hasContent()
        .containsEntry(REGION_NAME, "region1")
        .containsEntry(DATA_SOURCE_NAME, "name1").containsEntry(TABLE_NAME, "table1")
        .containsEntry(PDX_NAME, "class1").containsEntry(ID_NAME, "myId")
        .containsEntry(SPECIFIED_ID_NAME, "false")
        .containsEntry(SCHEMA_NAME, "mySchema").containsEntry(CATALOG_NAME, "myCatalog")
        .containsEntry(SYNCHRONOUS_NAME, "true");
  }

  @Test
  public void whenNoMappingFoundOnMember() {
    when(regionConfig.getCustomRegionElements()).thenReturn(new ArrayList<>());
    when(regionConfig.getRegionAttributes().getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.REPLICATE);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("mapping for region 'region1' not found");
  }
}
