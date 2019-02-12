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
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeMappingCommand.DESCRIBE_MAPPING;
import static org.apache.geode.connectors.util.internal.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.GROUP_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
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
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.util.BlobHelper;
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
  private static final String EMPLOYEE_REGION = "employeeRegion";
  private static final String EMPLOYEE_LOWER = "employee";
  private static final String EMPLOYEE_UPPER = "EMPLOYEE";
  private static final String GROUP1_REGION = "group1Region";
  private static final String GROUP2_REGION = "group2Region";
  private static final String GROUP1_GROUP2_REGION = "group1Group2Region";
  private static final String TEST_GROUP1 = "testGroup1";
  private static final String TEST_GROUP2 = "testGroup2";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;
  private MemberVM server4;

  @Before
  public void before() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, locator.getPort());
    server2 = startupRule.startServerVM(2, TEST_GROUP1, locator.getPort());
    server3 = startupRule.startServerVM(3, TEST_GROUP2, locator.getPort());
    server4 = startupRule.startServerVM(4, TEST_GROUP1 + "," + TEST_GROUP2, locator.getPort());

    gfsh.connectAndVerify(locator);
    setupDatabase();
  }

  @After
  public void after() throws Exception {
    teardownDatabase();
  }

  private void setupDatabase() {
    gfsh.executeAndAssertThat(
        "create data-source --name=connection"
            + " --pooled=false"
            + " --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    executeSql(
        "create table mySchema.myTable (myId varchar(10) primary key, name varchar(10))");
    executeSql(
        "create table mySchema." + EMPLOYEE_REGION
            + "(ID varchar(10) primary key, NAME varchar(10), AGE int)");
    executeSql(
        "create table mySchema." + EMPLOYEE_UPPER
            + "(ID varchar(10) primary key, name varchar(10), AGE int)");
  }

  private void teardownDatabase() {
    executeSql("drop table mySchema.myTable");
    executeSql("drop table mySchema." + EMPLOYEE_REGION);
    executeSql("drop table mySchema." + EMPLOYEE_UPPER);
  }

  private void executeSql(String sql) {
    for (MemberVM server : Arrays.asList(server1, server2, server3, server4)) {
      server.invoke(() -> {
        try {
          DataSource ds = JNDIInvoker.getDataSource("connection");
          Connection conn = ds.getConnection();
          Statement sm = conn.createStatement();
          sm.execute(sql);
          sm.close();
          conn.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    }
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

  private void setupGroupReplicate(String regionName, String groupNames) {
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --groups=" + groupNames)
        .statusIsSuccess();
  }

  private void setupPartition(String regionName) {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=PARTITION")
        .statusIsSuccess();
  }

  private void setupGroupPartition(String regionName, String groupNames) {
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=PARTITION --groups=" + groupNames)
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
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);

    CacheConfig.AsyncEventQueue queue = queueList.stream()
        .filter(q -> q.getId().equals(queueName)).findFirst()
        .orElse(null);

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
  @Parameters({GROUP1_REGION, "/" + GROUP1_REGION})
  public void createMappingReplicatedUpdatesServiceAndClusterConfigForServerGroup(
      String regionName) {
    setupGroupReplicate(regionName, TEST_GROUP1);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP1);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // TEST_GROUP1 only contains server2 and server 4
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, false);
    });

    server4.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, false);
    });

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    server3.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP1);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP1, false, false);
    });
  }

  @Test
  @Parameters({GROUP2_REGION, "/" + GROUP2_REGION})
  public void createMappingPartitionedUpdatesServiceAndClusterConfigForServerGroup(
      String regionName) {
    setupGroupPartition(regionName, TEST_GROUP2);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP2);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // TEST_GROUP2 only contains server3 and server4
    server3.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, true);
    });

    server4.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, true);
    });

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP2);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP2, false, true);
    });
  }

  @Test
  @Parameters({GROUP1_GROUP2_REGION, "/" + GROUP1_GROUP2_REGION})
  public void createMappingReplicatedUpdatesServiceAndClusterConfigForMultiServerGroup(
      String regionName) {
    setupGroupReplicate(regionName, TEST_GROUP1 + "," + TEST_GROUP2);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP1 + "," + TEST_GROUP2);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // TEST_GROUP1 and TEST_GROUP2 only contains server 2, server 3, and server 4
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, false);
    });

    server3.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, false);
    });

    server4.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, false);
    });

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP1);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP1, false, false);
    });
  }

  @Test
  @Parameters({GROUP1_GROUP2_REGION, "/" + GROUP1_GROUP2_REGION})
  public void createMappingPartitionedUpdatesServiceAndClusterConfigForMultiServerGroup(
      String regionName) {
    setupGroupPartition(regionName, TEST_GROUP1 + "," + TEST_GROUP2);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP1 + "," + TEST_GROUP2);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // TEST_GROUP1 and TEST_GROUP2 only contains server 2, server 3, and server 4
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, true);
    });

    server3.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, true);
    });

    server4.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, true);
    });

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP1);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP1, false, true);
    });
  }

  private static void assertValidMappingOnServer(RegionMapping mapping, String regionName,
      boolean synchronous, boolean isParallel) {
    assertValidMapping(mapping);
    validateRegionAlteredOnServer(regionName, synchronous);
    if (!synchronous) {
      validateAsyncEventQueueCreatedOnServer(regionName, isParallel);
    }
  }

  private static void assertValidMappingOnLocator(RegionMapping mapping, String regionName,
      String groups,
      boolean synchronous, boolean isParallel) {
    assertValidMapping(mapping);
    validateRegionAlteredInClusterConfig(regionName, groups, synchronous);
    if (!synchronous) {
      validateAsyncEventQueueCreatedInClusterConfig(regionName, groups, isParallel);
    }
  }

  private static void assertValidMapping(RegionMapping mapping) {
    assertThat(mapping.getDataSourceName()).isEqualTo("connection");
    assertThat(mapping.getTableName()).isEqualTo("myTable");
    assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
    assertThat(mapping.getIds()).isEqualTo("myId");
    assertThat(mapping.getCatalog()).isNull();
    assertThat(mapping.getSchema()).isEqualTo("mySchema");
    List<FieldMapping> fieldMappings = mapping.getFieldMappings();
    assertThat(fieldMappings.size()).isEqualTo(2);
    List<String> pdxFieldNames = new ArrayList<>();
    for (FieldMapping fieldMapping : fieldMappings) {
      pdxFieldNames.add(fieldMapping.getPdxName());
    }
    assertThat(pdxFieldNames.size()).isEqualTo(2);
    assertThat(pdxFieldNames).contains("MYID"); // pdx types built from metadata
    assertThat(pdxFieldNames).contains("NAME"); // use the metadata field names
  }

  private static void assertValidEmployeeMappingOnServer(RegionMapping mapping, String regionName,
      boolean synchronous, boolean isParallel, String tableName) {
    assertValidEmployeeMapping(mapping, tableName);
    validateRegionAlteredOnServer(regionName, synchronous);
    if (!synchronous) {
      validateAsyncEventQueueCreatedOnServer(regionName, isParallel);
    }
  }

  private static void assertValidEmployeeMappingOnLocator(RegionMapping mapping, String regionName,
      String groups,
      boolean synchronous, boolean isParallel, String tableName) {
    assertValidEmployeeMapping(mapping, tableName);
    validateRegionAlteredInClusterConfig(regionName, groups, synchronous);
    if (!synchronous) {
      validateAsyncEventQueueCreatedInClusterConfig(regionName, groups, isParallel);
    }
  }

  private static void assertValidEmployeeMapping(RegionMapping mapping, String tableName) {
    assertThat(mapping.getDataSourceName()).isEqualTo("connection");
    assertThat(mapping.getTableName()).isEqualTo(tableName);
    assertThat(mapping.getPdxName()).isEqualTo("org.apache.geode.connectors.jdbc.Employee");
    assertThat(mapping.getIds()).isEqualTo("id");
    assertThat(mapping.getCatalog()).isNull();
    assertThat(mapping.getSchema()).isEqualTo("mySchema");
    List<FieldMapping> fieldMappings = mapping.getFieldMappings();
    assertThat(fieldMappings.size()).isEqualTo(3);
    List<String> pdxFieldNames = new ArrayList<>();
    for (FieldMapping fieldMapping : fieldMappings) {
      pdxFieldNames.add(fieldMapping.getPdxName());
    }
    assertThat(pdxFieldNames.contains("id"));
    assertThat(pdxFieldNames.contains("name"));
    assertThat(pdxFieldNames.contains("age"));
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
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, false);
    });

    // without specifying 'group/groups', the region and regionmapping will be created on all
    // servers
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, false);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertValidMappingOnLocator(regionMapping, regionName, null, false, false);
    });
  }

  @Test
  @Parameters({EMPLOYEE_REGION, "/" + EMPLOYEE_REGION})
  public void createMappingWithDomainClassUpdatesServiceAndClusterConfig(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    // csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.Employee");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidEmployeeMappingOnServer(mapping, regionName, false, false, "employeeRegion");
    });

    // without specifying 'group/groups', the region and regionmapping will be created on all
    // servers
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidEmployeeMappingOnServer(mapping, regionName, false, false, "employeeRegion");
    });

    server3.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidEmployeeMappingOnServer(mapping, regionName, false, false, "employeeRegion");
    });

    server4.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidEmployeeMappingOnServer(mapping, regionName, false, false, "employeeRegion");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertValidEmployeeMappingOnLocator(regionMapping, regionName, null, false, false,
          "employeeRegion");
    });
  }

  @Test
  public void createTwoMappingsWithSamePdxName() {
    String region1Name = "region1";
    String region2Name = "region2";
    setupReplicate(region1Name);
    setupReplicate(region2Name);

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.Employee");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region2Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.Employee");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(region1Name);
      assertValidEmployeeMappingOnServer(mapping, region1Name, false, false, "employeeRegion");
    });

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(region2Name);
      assertValidEmployeeMappingOnServer(mapping, region2Name, false, false, "employeeRegion");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(region1Name, null);
      assertValidEmployeeMappingOnLocator(regionMapping, region1Name, null, false, false,
          "employeeRegion");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(region2Name, null);
      assertValidEmployeeMappingOnLocator(regionMapping, region2Name, null, false, false,
          "employeeRegion");
    });
  }

  @Test
  public void createMappingsWithExistingPdxName() {
    String region1Name = "region1";
    setupReplicate(region1Name);

    server1.invoke(() -> {
      try { // build a PDX registry entry
        Class<?> clazz = Class.forName("org.apache.geode.connectors.jdbc.Employee");
        assertThat(clazz).isNotNull();
        Constructor<?> ctor = clazz.getConstructor();
        Object object = ctor.newInstance(new Object[] {});
        BlobHelper.serializeToBlob(object);
      } catch (Exception ex) {
        assertThat(false).isEqualTo(ex);
      }
    });

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.Employee");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(region1Name);
      assertValidEmployeeMappingOnServer(mapping, region1Name, false, false, "employeeRegion");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(region1Name, null);
      assertValidEmployeeMappingOnLocator(regionMapping, region1Name, null, false, false,
          "employeeRegion");
    });

  }

  @Test
  public void createMappingUsingRegionNameUsesDomainClass() {
    setupReplicate(EMPLOYEE_LOWER);

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, EMPLOYEE_LOWER);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.Employee");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // TODO We are saving the lower case table name in region mapping,
    // even though the metadata lookup found an upper case table name.
    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(EMPLOYEE_LOWER);
      assertValidEmployeeMappingOnServer(mapping, EMPLOYEE_LOWER, false, false, EMPLOYEE_LOWER);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(EMPLOYEE_LOWER, null);
      assertValidEmployeeMappingOnLocator(regionMapping, EMPLOYEE_LOWER, null, false, false,
          EMPLOYEE_LOWER);
    });

  }

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
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, true, false);
    });

    // without specifying 'group/groups', the region and regionmapping will be created on all
    // servers
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, true, false);
    });

    server3.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, true, false);
    });

    server4.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, true, false);
    });

    locator.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromClusterConfig(regionName, null);
      assertValidMappingOnLocator(mapping, regionName, null, true, false);
    });
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createMappingWithPartitionUpdatesServiceAndClusterConfig(String regionName) {
    setupPartition(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getSchema()).isEqualTo("mySchema");
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
      assertThat(mapping.getSchema()).isEqualTo("mySchema");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, true);
    });

    server3.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, true);
    });

    server4.invoke(() -> {
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

  @Test
  @Parameters({"myTable", "/" + "myTable"})
  public void createMappingWithNoTable(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    String justRegion = regionName.startsWith("/") ? regionName.substring(1) : regionName;

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo(justRegion);
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    // without specifying 'group/groups', the region and regionmapping will be created on all
    // servers
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo(justRegion);
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    server3.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo(justRegion);
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    server4.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo(justRegion);
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredOnServer(regionName, false);
      validateAsyncEventQueueCreatedOnServer(regionName, false);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo(justRegion);
      assertThat(regionMapping.getPdxName()).isEqualTo("myPdxClass");
      validateRegionAlteredInClusterConfig(regionName, null, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, null, false);
    });
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void createExistingRegionMappingFails(String regionName) {
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(SCHEMA_NAME, "mySchema");
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

    server3.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping.getDataSourceName()).isEqualTo("connection");
      assertThat(mapping.getTableName()).isEqualTo("myTable");
      assertThat(mapping.getPdxName()).isEqualTo("myPdxClass");
    });

    server4.invoke(() -> {
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
