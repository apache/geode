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
import static org.apache.geode.connectors.jdbc.internal.cli.DestroyMappingCommand.DESTROY_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.ListMappingCommand.LIST_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.ListMappingCommand.LIST_OF_MAPPINGS;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.GROUP_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({JDBCConnectorTest.class})
@RunWith(JUnitParamsRunner.class)
public class CreateMappingCommandForProxyRegionDUnitTest {

  private static final String TEST_REGION = "testRegion";
  private static final String TEST_GROUP1 = "testGroup1";
  private static final String TEST_GROUP2 = "testGroup2";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Before
  public void before() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, TEST_GROUP1, locator.getPort());
    server2 = startupRule.startServerVM(2, TEST_GROUP2, locator.getPort());

    gfsh.connectAndVerify(locator);
    setupDatabase();
  }

  @After
  public void after() {
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
  }

  private void teardownDatabase() {
    executeSql("drop table mySchema.myTable");
  }

  private void executeSql(String sql) {
    for (MemberVM server : Arrays.asList(server1, server2)) {
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

  private void setupGroupPartition(String regionName, String groupNames, boolean isAccessor) {
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + (isAccessor ? " --type=PARTITION_PROXY"
            : " --type=PARTITION") + " --groups=" + groupNames)
        .statusIsSuccess();
  }


  private void setupGroupReplicate(String regionName, String groupNames, boolean isAccessor) {
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + (isAccessor ? " --type=REPLICATE_PROXY"
            : " --type=REPLICATE") + " --groups=" + groupNames)
        .statusIsSuccess();
  }

  private static RegionMapping getRegionMappingFromClusterConfig(String regionName,
      String groups) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(groups);
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
      String groups,
      boolean isParallel) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(groups);
    List<CacheConfig.AsyncEventQueue> queueList = cacheConfig.getAsyncEventQueues();
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    CacheConfig.AsyncEventQueue queue = findQueue(queueList, queueName);
    assertThat(queue).isNotNull();
    assertThat(queue.getId()).isEqualTo(queueName);
    assertThat(queue.getAsyncEventListener().getClassName())
        .isEqualTo(JdbcAsyncWriter.class.getName());
    assertThat(queue.isParallel()).isEqualTo(isParallel);
  }

  private static CacheConfig.AsyncEventQueue findQueue(
      List<CacheConfig.AsyncEventQueue> queueList,
      String queueName) {
    for (CacheConfig.AsyncEventQueue queue : queueList) {
      if (queue.getId().equals(queueName)) {
        return queue;
      }
    }
    return null;
  }

  private static String convertRegionPathToName(String regionPath) {
    if (regionPath.startsWith("/")) {
      return regionPath.substring(1);
    }
    return regionPath;
  }

  private static void validateRegionAlteredInClusterConfig(String regionName,
      String groups, boolean synchronous) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(groups);
    RegionConfig regionConfig = cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(convertRegionPathToName(regionName))).findFirst()
        .orElse(null);
    RegionAttributesType attributes = regionConfig.getRegionAttributes();
    assertThat(attributes.getCacheLoader().getClassName()).isEqualTo(JdbcLoader.class.getName());
    if (synchronous) {
      assertThat(attributes.getCacheWriter().getClassName())
          .isEqualTo(JdbcWriter.class.getName());
    } else {
      String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
      assertThat(attributes.getAsyncEventQueueIds()).isEqualTo(queueName);
    }
  }

  private static void validateAsyncEventQueueCreatedOnServer(String regionName,
      boolean isParallel) {
    InternalCache cache = ClusterStartupRule.getCache();
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
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
      String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
      assertThat(region.getAttributes().getAsyncEventQueueIds()).contains(queueName);
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  @Parameters(method = "parametersToTestPRDR")
  public void createMappingTogetherForMultiServerGroupWithEmptyRegion(boolean isPR) {
    String regionName = "/" + TEST_REGION;
    if (isPR) {
      setupGroupPartition(regionName, TEST_GROUP1, false);
      setupGroupPartition(regionName, TEST_GROUP2, true);
    } else {
      setupGroupReplicate(regionName, TEST_GROUP1, false);
      setupGroupReplicate(regionName, TEST_GROUP2, true);
    }
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP1 + "," + TEST_GROUP2);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNotNull();
      assertValidMappingOnServer(mapping, regionName, false, isPR);
    });
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP1);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP1, false, isPR);
      regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP2);
      assertThat(regionMapping).isNull();
    });

    // do describe mapping
    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, TEST_GROUP1 + "," + TEST_GROUP2);
    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsOutput("Mapping for group");
    commandResultAssert.containsOutput(TEST_GROUP1);
    commandResultAssert.doesNotContainOutput(TEST_GROUP2);
    commandResultAssert.containsKeyValuePair(REGION_NAME, convertRegionPathToName(regionName));
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "myTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName.class.getName());
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");

    // do list mapping
    csb = new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP1 + "," + TEST_GROUP2);
    commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess();
    commandResultAssert.tableHasRowCount(1);
    commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, TEST_REGION);

    // do destroy mapping
    csb =
        new CommandStringBuilder(DESTROY_MAPPING + " --groups=" + TEST_GROUP1 + "," + TEST_GROUP2);
    csb.addOption(REGION_NAME, TEST_REGION);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @SuppressWarnings("deprecation")
  @Test
  @Parameters(method = "parametersToTestPRDR")
  public void createMappingSeparatelyForMultiServerGroupWithEmptyRegion(boolean isPR) {
    String regionName = "/" + TEST_REGION;
    if (isPR) {
      setupGroupPartition(regionName, TEST_GROUP1, false);
      setupGroupPartition(regionName, TEST_GROUP2, true);
    } else {
      setupGroupReplicate(regionName, TEST_GROUP1, false);
      setupGroupReplicate(regionName, TEST_GROUP2, true);
    }
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP1);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP2);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput(MappingConstants.THERE_IS_NO_JDBC_MAPPING_ON_PROXY_REGION);

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, isPR);
    });
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP1);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP1, false, isPR);
      regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP2);
      assertThat(regionMapping).isNull();
    });

    // do describe mapping for TEST_GROUP2
    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, TEST_GROUP2);
    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess()
        .containsOutput(MappingConstants.THERE_IS_NO_JDBC_MAPPING_ON_PROXY_REGION);

    // do list mapping for TEST_GROUP2
    csb = new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP2);
    commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess().hasNoTableSection();

    // do destroy mapping for for TEST_GROUP2
    csb = new CommandStringBuilder(DESTROY_MAPPING + " --groups=" + TEST_GROUP2);
    csb.addOption(REGION_NAME, TEST_REGION);
    gfsh.executeAndAssertThat(csb.toString()).statusIsError();

    // do describe mapping for TEST_GROUP1
    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, TEST_GROUP1);
    commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsOutput("Mapping for group");
    commandResultAssert.containsOutput(TEST_GROUP1);
    commandResultAssert.containsKeyValuePair(REGION_NAME,
        convertRegionPathToName(regionName));
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "myTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName.class.getName());
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");

    // do list mapping
    csb = new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP1);
    commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess();
    commandResultAssert.tableHasRowCount(1);
    commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, TEST_REGION);

    // do destroy mapping
    csb =
        new CommandStringBuilder(DESTROY_MAPPING + " --groups=" + TEST_GROUP1);
    csb.addOption(REGION_NAME, TEST_REGION);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @SuppressWarnings("deprecation")
  @Test
  @Parameters(method = "parametersToTestPRDR")
  public void createEmptyRegionAfterCreateMapping(boolean isPR) {
    String regionName = "/" + TEST_REGION;
    if (isPR) {
      setupGroupPartition(regionName, TEST_GROUP1, false);
    } else {
      setupGroupReplicate(regionName, TEST_GROUP1, false);
    }
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP1);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    if (isPR) {
      setupGroupPartition(regionName, TEST_GROUP2, true);
    } else {
      setupGroupReplicate(regionName, TEST_GROUP2, true);
    }
    csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP2);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput(MappingConstants.THERE_IS_NO_JDBC_MAPPING_ON_PROXY_REGION);

    // do create jdbc-mapping again
    csb.addOption(CliStrings.IFNOTEXISTS, "true");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput("Skipping: ");

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertValidMappingOnServer(mapping, regionName, false, isPR);
    });
    server2.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP1);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP1, false, isPR);
      regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP2);
      assertThat(regionMapping).isNull();
    });

    // do describe mapping for TEST_GROUP2
    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, TEST_GROUP2);
    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess()
        .containsOutput(MappingConstants.THERE_IS_NO_JDBC_MAPPING_ON_PROXY_REGION);

    // do list mapping for TEST_GROUP2
    csb = new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP2);
    commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess().hasNoTableSection();

    // do destroy mapping for for TEST_GROUP2
    csb = new CommandStringBuilder(DESTROY_MAPPING + " --groups=" + TEST_GROUP2);
    csb.addOption(REGION_NAME, TEST_REGION);
    gfsh.executeAndAssertThat(csb.toString()).statusIsError();

    // do describe mapping for TEST_GROUP1
    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, TEST_GROUP1);
    commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsOutput("Mapping for group");
    commandResultAssert.containsOutput(TEST_GROUP1);
    commandResultAssert.containsKeyValuePair(REGION_NAME,
        convertRegionPathToName(regionName));
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "myTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName.class.getName());
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");

    // do list mapping
    csb = new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP1);
    commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess();
    commandResultAssert.tableHasRowCount(1);
    commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, TEST_REGION);

    // do destroy mapping
    csb =
        new CommandStringBuilder(DESTROY_MAPPING + " --groups=" + TEST_GROUP1);
    csb.addOption(REGION_NAME, TEST_REGION);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @SuppressWarnings("unused")
  private Object[] parametersToTestPRDR() {
    return new Object[] {true, false};
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
    assertThat(mapping.getPdxName()).isEqualTo(IdAndName.class.getName());
    assertThat(mapping.getIds()).isEqualTo("myId");
    assertThat(mapping.getCatalog()).isNull();
    assertThat(mapping.getSchema()).isEqualTo("mySchema");
    List<FieldMapping> fieldMappings = mapping.getFieldMappings();
    assertThat(fieldMappings.size()).isEqualTo(2);
    assertThat(fieldMappings.get(0)).isEqualTo(
        new FieldMapping("myid", FieldType.STRING.name(), "MYID", JDBCType.VARCHAR.name(), false));
    assertThat(fieldMappings.get(1)).isEqualTo(
        new FieldMapping("name", FieldType.STRING.name(), "NAME", JDBCType.VARCHAR.name(), true));
  }

  public static class IdAndName implements PdxSerializable {
    private String id;
    private String name;

    public IdAndName() {
      // nothing
    }

    IdAndName(String id, String name) {
      this.id = id;
      this.name = name;
    }

    String getId() {
      return id;
    }

    String getName() {
      return name;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("myid", id);
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("myid");
      name = reader.readString("name");
    }
  }

}
