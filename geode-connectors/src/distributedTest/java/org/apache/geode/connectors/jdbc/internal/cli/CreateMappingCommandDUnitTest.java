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
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.JdbcAsyncWriter;
import org.apache.geode.connectors.jdbc.JdbcLoader;
import org.apache.geode.connectors.jdbc.JdbcWriter;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({JDBCConnectorTest.class})
@RunWith(JUnitParamsRunner.class)
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

  }

  private void setupReplicate(String regionName) {
    setupReplicate(regionName, false);
  }

  private void setupReplicate(String regionName, boolean addLoader) {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE"
        + (addLoader ? " --cache-loader=" + JdbcLoader.class.getName() : ""))
        .statusIsSuccess();
  }

  private void setupPartition(String regionName) {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=PARTITION")
        .statusIsSuccess();
  }

  private void setupAsyncEventQueue(String regionName) {
    gfsh.executeAndAssertThat(
        "create async-event-queue --id="
            + CreateMappingCommand.createAsyncEventQueueName(regionName)
            + " --listener=" + JdbcAsyncWriter.class.getName())
        .statusIsSuccess();
  }

  private static RegionMapping getRegionMappingFromClusterConfig(String regionName) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(null);
    RegionConfig regionConfig = cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(convertRegionPathToName(regionName))).findFirst()
        .orElse(null);
    return (RegionMapping) regionConfig.getCustomRegionElements().stream()
        .filter(element -> element instanceof RegionMapping).findFirst().orElse(null);
  }

  private static RegionMapping getRegionMappingFromService(String regionName) {
    return ClusterStartupRule.getCache().getService(JdbcConnectorService.class)
        .getMappingForRegion(convertRegionPathToName(regionName));
  }

  private static void validateAsyncEventQueueCreatedInClusterConfig(String regionName,
      boolean isParallel) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(null);
    List<CacheConfig.AsyncEventQueue> queueList = cacheConfig.getAsyncEventQueues();
    CacheConfig.AsyncEventQueue queue = queueList.get(0);
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    assertThat(queue.getId()).isEqualTo(queueName);
    assertThat(queue.getAsyncEventListener().getClassName())
        .isEqualTo(JdbcAsyncWriter.class.getName());
    assertThat(queue.isParallel()).isEqualTo(isParallel);
  }

  private static String convertRegionPathToName(String regionPath) {
    if (regionPath.startsWith("/")) {
      return regionPath.substring(1);
    }
    return regionPath;
  }

  private static void validateRegionAlteredInClusterConfig(String regionName, boolean synchronous) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(null);
    RegionConfig regionConfig = cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(convertRegionPathToName(regionName))).findFirst()
        .orElse(null);
    RegionAttributesType attributes = regionConfig.getRegionAttributes();
    assertThat(attributes.getCacheLoader().getClassName()).isEqualTo(JdbcLoader.class.getName());
    if (synchronous) {
      assertThat(attributes.getCacheWriter().getClassName()).isEqualTo(JdbcWriter.class.getName());
    } else {
      String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
      assertThat(attributes.getAsyncEventQueueIds()).isEqualTo(queueName);
    }
  }

  private static void validateAsyncEventQueueCreatedOnServer(String regionName,
      boolean isParallel) {
    InternalCache cache = ClusterStartupRule.getCache();
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    AsyncEventQueue queue = cache.getAsyncEventQueue(queueName);
    assertThat(queue).isNotNull();
    assertThat(queue.getAsyncEventListener()).isInstanceOf(JdbcAsyncWriter.class);
    assertThat(queue.isParallel()).isEqualTo(isParallel);
  }

  private static void validateRegionAlteredOnServer(String regionName, boolean synchronous) {
    InternalCache cache = ClusterStartupRule.getCache();
    Region<?, ?> region = cache.getRegion(regionName);
    assertThat(region.getAttributes().getCacheLoader()).isInstanceOf(JdbcLoader.class);
    if (synchronous) {
      assertThat(region.getAttributes().getCacheWriter()).isInstanceOf(JdbcWriter.class);
    } else {
      String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
      assertThat(region.getAttributes().getAsyncEventQueueIds()).contains(queueName);
    }
  }

  @Test
  @Parameters({REGION_NAME, "/" + REGION_NAME})
  public void createMappingUpdatesServiceAndClusterConfig(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, regionName);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__TABLE_NAME, "myTable");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredInClusterConfig(regionName, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, false);
    });
  }

  @Test
  @Parameters({REGION_NAME, "/" + REGION_NAME})
  public void createSynchronousMappingUpdatesServiceAndClusterConfig(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, regionName);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__TABLE_NAME, "myTable");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");
    csb.addOption(CREATE_MAPPING__SYNCHRONOUS_NAME, "true");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, true);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredInClusterConfig(regionName, true);
    });
  }

  @Test
  @Parameters({REGION_NAME, "/" + REGION_NAME})
  public void createMappingWithPartitionUpdatesServiceAndClusterConfig(String regionName) {
    setupPartition(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, regionName);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__TABLE_NAME, "myTable");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, true);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredInClusterConfig(regionName, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, true);
    });
  }

  @Test
  @Parameters({REGION_NAME, "/" + REGION_NAME})
  public void createMappingWithNoTable(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, regionName);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isNull();
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isNull();
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredInClusterConfig(regionName, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, false);
    });
  }

  @Test
  @Parameters({REGION_NAME, "/" + REGION_NAME})
  public void createExistingRegionMappingFails(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, regionName);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");
    csb.addOption(CREATE_MAPPING__TABLE_NAME, "myTable");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, regionName);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "bogusConnection");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "bogusPdxClass");
    csb.addOption(CREATE_MAPPING__TABLE_NAME, "bogusTable");
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput(
            "A jdbc-mapping for " + convertRegionPathToName(regionName) + " already exists");

    server.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
    });
  }

  @Test
  @Parameters({REGION_NAME, "/" + REGION_NAME})
  public void createMappingWithoutPdxNameFails(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, regionName);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");

    // NOTE: --table is optional so it should not be in the output but it is. See GEODE-3468.
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput(
            "You should specify option (--table, --pdx-name, --synchronous) for this command");
  }

  @Test
  @Parameters({REGION_NAME, "/" + REGION_NAME})
  public void createMappingWithNonExistentRegionFails(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, "bogusRegion");
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("A region named bogusRegion must already exist");
  }

  @Test
  @Parameters({REGION_NAME, "/" + REGION_NAME})
  public void createMappingWithRegionThatHasALoaderFails(String regionName) {
    setupReplicate(regionName, true);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, regionName);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("The existing region " + convertRegionPathToName(regionName)
            + " must not already have a cache-loader, but it has " + JdbcLoader.class.getName());
  }

  @Test
  @Parameters({REGION_NAME, "/" + REGION_NAME})
  public void createMappingWithExistingQueueFails(String regionName) {
    setupReplicate(regionName);
    setupAsyncEventQueue(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, regionName);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "connection");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("An async-event-queue named "
            + CreateMappingCommand.createAsyncEventQueueName(regionName)
            + " must not already exist.");
  }

}
