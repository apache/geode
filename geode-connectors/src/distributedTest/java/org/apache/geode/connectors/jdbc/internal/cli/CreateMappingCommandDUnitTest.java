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

import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__DATA_SOURCE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({JDBCConnectorTest.class})
public class CreateMappingCommandDUnitTest {

  private static final String REGION_NAME = "testRegion";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server;

  @Before
  public void before() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat("create region --name=" + REGION_NAME + " --type=REPLICATE")
        .statusIsSuccess();
  }

  @Test
  public void createMappingUpdatesServiceAndClusterConfig() {
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__TABLE_NAME, "myTable");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService();
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig();
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
    });
  }

  @Test
  public void createMappingWithNoTable() {
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService();
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isNull();
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig();
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isNull();
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
    });
  }

  @Test
  public void createExistingRegionMappingFails() {
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");
    csb.addOption(CREATE_MAPPING__TABLE_NAME, "myTable");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "bogusConnection");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "bogusPdxClass");
    csb.addOption(CREATE_MAPPING__TABLE_NAME, "bogusTable");
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("A jdbc-mapping for " + REGION_NAME + " already exists");

    server.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService();
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig();
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
    });
  }

  private static RegionMapping getRegionMappingFromClusterConfig() {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(null);
    RegionConfig regionConfig = cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(REGION_NAME)).findFirst().orElse(null);
    return (RegionMapping) regionConfig.getCustomRegionElements().stream()
        .filter(element -> element instanceof RegionMapping).findFirst().orElse(null);
  }

  private static RegionMapping getRegionMappingFromService() {
    return ClusterStartupRule.getCache().getService(JdbcConnectorService.class)
        .getMappingForRegion(REGION_NAME);
  }

  @Test
  public void createMappingWithoutPdxNameFails() {
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    // NOTE: --table is optional so it should not be in the ouput but it is. See GEODE-3468.
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("You should specify option (--table, --pdx-name) for this command");
  }
}
