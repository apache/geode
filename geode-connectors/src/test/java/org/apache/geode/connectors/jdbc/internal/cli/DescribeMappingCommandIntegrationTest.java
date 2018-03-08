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

import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__CONNECTION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__PDX_CLASS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__TABLE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__VALUE_CONTAINS_PRIMARY_KEY;
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeMappingCommand.FIELD_TO_COLUMN_TABLE;
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeMappingCommand.RESULT_SECTION_NAME;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.management.internal.cli.result.AbstractResultData.SECTION_DATA_ACCESSOR;
import static org.apache.geode.management.internal.cli.result.AbstractResultData.TABLE_DATA_ACCESSOR;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.connectors.jdbc.internal.RegionMappingBuilder;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class DescribeMappingCommandIntegrationTest {
  private static final String REGION_NAME = "testRegion";

  private InternalCache cache;
  private JdbcConnectorService service;
  private RegionMapping regionMapping;
  private DescribeMappingCommand command;

  @Before
  public void setup() {
    String[] fieldMappings = new String[] {"field1:column1", "field2:column2"};

    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true").create();
    service = cache.getService(JdbcConnectorService.class);
    regionMapping = new RegionMappingBuilder().withRegionName(REGION_NAME)
        .withConnectionConfigName("connection").withTableName("testTable")
        .withPdxClassName("myPdxClass").withPrimaryKeyInValue(true)
        .withFieldToColumnMappings(fieldMappings).build();
    command = new DescribeMappingCommand();
    command.setCache(cache);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void displaysNoMappingFoundMessageWhenMappingDoesNotExist() {
    String nonExistingMappingRegionName = "non existing";
    Result result = command.describeMapping(nonExistingMappingRegionName);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    CommandResult commandResult = (CommandResult) result;
    String tableContent = commandResult.getTableContent().toString();
    assertThat(tableContent)
        .contains("Mapping for region '" + nonExistingMappingRegionName + "' not found");
  }

  @Test
  public void displaysMappingInformationWhenMappingExists() throws Exception {
    service.createRegionMapping(regionMapping);
    Result result = command.describeMapping(REGION_NAME);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    CommandResult commandResult = (CommandResult) result;
    GfJsonObject sectionContent = commandResult.getTableContent()
        .getJSONObject(SECTION_DATA_ACCESSOR + "-" + RESULT_SECTION_NAME);

    assertThat(sectionContent.get(CREATE_MAPPING__REGION_NAME))
        .isEqualTo(regionMapping.getRegionName());
    assertThat(sectionContent.get(CREATE_MAPPING__CONNECTION_NAME))
        .isEqualTo(regionMapping.getConnectionConfigName());
    assertThat(sectionContent.get(CREATE_MAPPING__TABLE_NAME))
        .isEqualTo(regionMapping.getTableName());
    assertThat(sectionContent.get(CREATE_MAPPING__PDX_CLASS_NAME))
        .isEqualTo(regionMapping.getPdxClassName());
    assertThat(sectionContent.get(CREATE_MAPPING__VALUE_CONTAINS_PRIMARY_KEY))
        .isEqualTo(regionMapping.isPrimaryKeyInValue());

    GfJsonObject tableContent = sectionContent
        .getJSONObject(TABLE_DATA_ACCESSOR + "-" + FIELD_TO_COLUMN_TABLE).getJSONObject("content");

    regionMapping.getFieldToColumnMap().entrySet().forEach((entry) -> {
      assertThat(tableContent.get("Field").toString()).contains(entry.getKey());
      assertThat(tableContent.get("Column").toString()).contains(entry.getValue());
    });
  }

  @Test
  public void displaysMappingInformationWhenMappingWithNoFieldToColumnsExists() throws Exception {
    regionMapping = new RegionMappingBuilder().withRegionName(REGION_NAME)
        .withConnectionConfigName("connection").withTableName("testTable")
        .withPdxClassName("myPdxClass").withPrimaryKeyInValue(true).withFieldToColumnMappings(null)
        .build();
    service.createRegionMapping(regionMapping);
    Result result = command.describeMapping(REGION_NAME);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    CommandResult commandResult = (CommandResult) result;
    GfJsonObject sectionContent = commandResult.getTableContent()
        .getJSONObject(SECTION_DATA_ACCESSOR + "-" + RESULT_SECTION_NAME);

    assertThat(sectionContent.get(CREATE_MAPPING__REGION_NAME))
        .isEqualTo(regionMapping.getRegionName());
    assertThat(sectionContent.get(CREATE_MAPPING__CONNECTION_NAME))
        .isEqualTo(regionMapping.getConnectionConfigName());
    assertThat(sectionContent.get(CREATE_MAPPING__TABLE_NAME))
        .isEqualTo(regionMapping.getTableName());
    assertThat(sectionContent.get(CREATE_MAPPING__PDX_CLASS_NAME))
        .isEqualTo(regionMapping.getPdxClassName());
    assertThat(sectionContent.get(CREATE_MAPPING__VALUE_CONTAINS_PRIMARY_KEY))
        .isEqualTo(regionMapping.isPrimaryKeyInValue());

    GfJsonObject tableContent = sectionContent
        .getJSONObject(TABLE_DATA_ACCESSOR + "-" + FIELD_TO_COLUMN_TABLE).getJSONObject("content");
    assertThat(tableContent.get("Field")).isNull();
    assertThat(tableContent.get("Column")).isNull();
  }
}
