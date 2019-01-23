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
import static org.apache.geode.connectors.util.internal.MappingConstants.CATALOG_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.GROUP_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.TABLE_NAME;
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

  private static final String TEST_REGION = "testRegion";
  private static final String TEST_GROUP1 = "testGroup1";
  // private static final String TEST_GROUP2 = "testGroup2";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Before
  public void before() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, locator.getPort());
    server2 = startupRule.startServerVM(2, TEST_GROUP1, locator.getPort());
    // TODO:
    // create a TEST_GROUP2 to contain server3
    // create a TEST_GROUP3 to contain both server2 and server3

    gfsh.connectAndVerify(locator);

  }

  public void cleanUp() throws Exception {
    startupRule.stop(0);
    startupRule.stop(1);
    startupRule.stop(2);
    startupRule.stop(3);
    gfsh.disconnect();
  }

  private void setupReplicate(String regionName) {
    setupReplicate(regionName, false);
  }

  // TODO: we can refactor to create subclass to test different combination of server groups
  // each with different 'create region' and 'create jdbc-mapping' commands
  private void setupReplicate(String regionName, boolean addLoader) {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE"
        + (addLoader ? " --cache-loader=" + JdbcLoader.class.getName() : ""))
        .statusIsSuccess();
  }

  // TODO: might need a parameter for server group names
  private void setupGroupReplicate(String regionName) {
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --groups=" + TEST_GROUP1)
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

  private static RegionMapping getRegionMappingFromClusterConfig(String regionName,
      String groups) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(groups);
    RegionConfig regionConfig = cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(convertRegionPathToName(regionName))).findFirst()
        .orElse(null);
    RegionMapping regionMapping =
        (RegionMapping) regionConfig.getCustomRegionElements().stream()
            .filter(element -> element instanceof RegionMapping).findFirst().orElse(null);
    return regionMapping;
  }

  private static RegionMapping getRegionMappingFromService(String regionName) {
    return ClusterStartupRule.getCache().getService(JdbcConnectorService.class)
        .getMappingForRegion(convertRegionPathToName(regionName));
  }

  private static void validateAsyncEventQueueCreatedInClusterConfig(String regionName,
      String groups,
      boolean isParallel) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(groups);
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

  private static void validateRegionAlteredInClusterConfig(String regionName,
      String groups,
      boolean synchronous) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(groups);
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
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createMappingUpdatesServiceAndClusterConfigForServerGroup(String regionName) {
    setupGroupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(CATALOG_NAME, "myCatalog");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP1);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // TEST_GROUP1 only contains server2
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      assertThat(mapping.getIds()).isEqualTo("myId");
      assertThat(mapping.getCatalog()).isEqualTo("myCatalog");
      assertThat(mapping.getSchema()).isEqualTo("mySchema");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
      // assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      // assertThat(mapping.getTableName()).isEqualTo("myTable");
      // assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      // assertThat(mapping.getIds()).isEqualTo("myId");
      // assertThat(mapping.getCatalog()).isEqualTo("myCatalog");
      // assertThat(mapping.getSchema()).isEqualTo("mySchema");
      // validateRegionAlteredOnServer(regionName, false);
      // validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP1);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      assertThat(regionMapping.getIds()).isEqualTo("myId");
      assertThat(regionMapping.getCatalog()).isEqualTo("myCatalog");
      assertThat(regionMapping.getSchema()).isEqualTo("mySchema");
      validateRegionAlteredInClusterConfig(regionName, TEST_GROUP1, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, TEST_GROUP1, false);
    });
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createMappingUpdatesServiceAndClusterConfig(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(CATALOG_NAME, "myCatalog");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      assertThat(mapping.getIds()).isEqualTo("myId");
      assertThat(mapping.getCatalog()).isEqualTo("myCatalog");
      assertThat(mapping.getSchema()).isEqualTo("mySchema");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    // without specifying 'group/groups', the region and regionmapping will be created on all
    // servers
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      assertThat(mapping.getIds()).isEqualTo("myId");
      assertThat(mapping.getCatalog()).isEqualTo("myCatalog");
      assertThat(mapping.getSchema()).isEqualTo("mySchema");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      assertThat(regionMapping.getIds()).isEqualTo("myId");
      assertThat(regionMapping.getCatalog()).isEqualTo("myCatalog");
      assertThat(regionMapping.getSchema()).isEqualTo("mySchema");
      validateRegionAlteredInClusterConfig(regionName, null, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, null, false);
    });
  }

  // TODO: need to have createSynchronousMappingUpdatesServiceAndClusterConfigForServerGroup
  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createSynchronousMappingUpdatesServiceAndClusterConfig(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(SYNCHRONOUS_NAME, "true");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(CATALOG_NAME, "myCatalog");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      assertThat(mapping.getIds()).isEqualTo("myId");
      assertThat(mapping.getCatalog()).isEqualTo("myCatalog");
      assertThat(mapping.getSchema()).isEqualTo("mySchema");
      validateRegionAlteredOnServer(regionName, true);
    });

    // without specifying 'group/groups', the region and regionmapping will be created on all
    // servers
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      assertThat(mapping.getIds()).isEqualTo("myId");
      assertThat(mapping.getCatalog()).isEqualTo("myCatalog");
      assertThat(mapping.getSchema()).isEqualTo("mySchema");
      validateRegionAlteredOnServer(regionName, true);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      assertThat(regionMapping.getIds()).isEqualTo("myId");
      assertThat(regionMapping.getCatalog()).isEqualTo("myCatalog");
      assertThat(regionMapping.getSchema()).isEqualTo("mySchema");
      validateRegionAlteredInClusterConfig(regionName, null, true);
    });
  }

  // TODO: need to have a createMappingWithPartitionUpdatesServiceAndClusterConfigForServerGroup
  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createMappingWithPartitionUpdatesServiceAndClusterConfig(String regionName) {
    setupPartition(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, true);
    });

    // without specifying 'group/groups', the region and regionmapping will be created on all
    // servers
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, true);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredInClusterConfig(regionName, null, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, null, true);
    });
  }

  // TODO: need to have createMappingWithNoTableForServerGroup
  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createMappingWithNoTable(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isNull();
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    // without specifying 'group/groups', the region and regionmapping will be created on all
    // servers
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isNull();
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isNull();
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredInClusterConfig(regionName, null, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, null, false);
    });
  }

  // TODO: need to have createExistingRegionMappingFailsForServerGroup
  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createExistingRegionMappingFails(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(TABLE_NAME, "myTable");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "bogusConnection");
    csb.addOption(PDX_NAME, "bogusPdxClass");
    csb.addOption(TABLE_NAME, "bogusTable");
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput(
            "A JDBC mapping for " + convertRegionPathToName(regionName) + " already exists");

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
    });

    // without specifying 'group/groups', the region and regionmapping will be created on all
    // servers
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
    });
  }

  // TODO: need to add createMappingWithoutPdxNameFailsForServerGroup
  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createMappingWithoutPdxNameFails(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");

    // NOTE: --table is optional so it should not be in the output but it is. See GEODE-3468.
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput(
            "You should specify option (--table, --pdx-name, --synchronous, --id, --catalog, --schema, --group) for this command");
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createMappingWithNonExistentRegionFails(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, "bogusRegion");
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("A region named bogusRegion must already exist");
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createMappingWithRegionThatHasALoaderFails(String regionName) {
    setupReplicate(regionName, true);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("The existing region " + convertRegionPathToName(regionName)
            + " must not already have a cache-loader, but it has " + JdbcLoader.class.getName());
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createMappingWithExistingQueueFails(String regionName) {
    setupReplicate(regionName);
    setupAsyncEventQueue(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("An async-event-queue named "
            + CreateMappingCommand.createAsyncEventQueueName(regionName)
            + " must not already exist.");
  }

}
