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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeMappingCommand.DESCRIBE_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.GROUP_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.PDX_CLASS_FILE;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.TABLE_NAME;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
import org.apache.geode.internal.PdxSerializerObject;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.compiler.InMemoryClassFile;
import org.apache.geode.test.compiler.InMemoryJavaCompiler;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(JDBCConnectorTest.class)
public class CreateMappingCommandDUnitTest {

  private static final String TEST_REGION = "testRegion";
  private static final String EMPLOYEE_REGION = "employeeRegion";
  private static final String EMPLOYEE_LOWER = "employee";
  private static final String EMPLOYEE_UPPER = "EMPLOYEE";
  private static final String EMPLOYEE_NUMERIC = "employeeNumeric";
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

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

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
    executeSql(
        "create table mySchema." + EMPLOYEE_REGION
            + "(ID varchar(10) primary key, NAME varchar(10), AGE int)");
    executeSql(
        "create table mySchema." + EMPLOYEE_UPPER
            + "(ID varchar(10) primary key, name varchar(10), AGE int)");
    executeSql(
        "create table mySchema." + EMPLOYEE_NUMERIC
            + "(ID varchar(10) primary key, name varchar(10), AGE int, income real, refid bigint)");
  }

  private void teardownDatabase() {
    executeSql("drop table mySchema.myTable");
    executeSql("drop table mySchema." + EMPLOYEE_REGION);
    executeSql("drop table mySchema." + EMPLOYEE_UPPER);
    executeSql("drop table mySchema." + EMPLOYEE_NUMERIC);
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
            + MappingCommandUtils.createAsyncEventQueueName(regionName)
            + " --listener=" + JdbcAsyncWriter.class.getName())
        .statusIsSuccess();
  }

  private static RegionMapping getRegionMappingFromClusterConfig(String regionName, String groups) {
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
      String groups, boolean isParallel) {
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

  private static CacheConfig.AsyncEventQueue findQueue(List<CacheConfig.AsyncEventQueue> queueList,
      String queueName) {
    for (CacheConfig.AsyncEventQueue queue : queueList) {
      if (queue.getId().equals(queueName)) {
        return queue;
      }
    }
    return null;
  }

  private static String convertRegionPathToName(String regionPath) {
    if (regionPath.startsWith(SEPARATOR)) {
      return regionPath.substring(1);
    }
    return regionPath;
  }

  private static void validateRegionAlteredInClusterConfig(String regionName, String groups,
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

  @Test
  public void createMappingSucceedsWithWarningWhenMappingAlreadyExistsAndIfNotExistsIsTrue() {
    String regionName = REGION_NAME;
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // do it again and expect to fail
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("A JDBC mapping for " + regionName + " already exists.");

    csb.addOption(CliStrings.IFNOTEXISTS, "true");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput("Skipping: ");
  }

  @Test
  public void createMappingReplicatedUpdatesServiceAndClusterConfigForServerGroup() {
    String regionName = GROUP1_REGION;
    setupGroupReplicate(regionName, TEST_GROUP1);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
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
  public void createMappingPartitionedUpdatesServiceAndClusterConfigForServerGroup() {
    String regionName = GROUP2_REGION;
    setupGroupPartition(regionName, TEST_GROUP2);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
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
  public void createMappingReplicatedUpdatesServiceAndClusterConfigForMultiServerGroup() {
    String regionName = SEPARATOR + GROUP1_GROUP2_REGION;
    setupGroupReplicate(regionName, TEST_GROUP1 + "," + TEST_GROUP2);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP1 + "," + TEST_GROUP2);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // TEST_GROUP1 and TEST_GROUP2 only contains server 2, server 3, and server 4
    for (MemberVM server : Arrays.asList(server2, server3, server4)) {
      server.invoke(() -> {
        RegionMapping mapping = getRegionMappingFromService(regionName);
        assertValidMappingOnServer(mapping, regionName, false, false);
      });
    }

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP1);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP1, false, false);
      regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP2);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP2, false, false);
    });
  }

  @Test
  public void createMappingPartitionedUpdatesServiceAndClusterConfigForMultiServerGroup() {
    String regionName = SEPARATOR + GROUP1_GROUP2_REGION;
    setupGroupPartition(regionName, TEST_GROUP1 + "," + TEST_GROUP2);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(GROUP_NAME, TEST_GROUP1 + "," + TEST_GROUP2);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // TEST_GROUP1 and TEST_GROUP2 only contains server 2, server 3, and server 4
    for (MemberVM server : Arrays.asList(server2, server3, server4)) {
      server.invoke(() -> {
        RegionMapping mapping = getRegionMappingFromService(regionName);
        assertValidMappingOnServer(mapping, regionName, false, true);
      });
    }

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(regionName);
      assertThat(mapping).isNull();
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP1);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP1, false, true);
      regionMapping = getRegionMappingFromClusterConfig(regionName, TEST_GROUP2);
      assertValidMappingOnLocator(regionMapping, regionName, TEST_GROUP2, false, true);
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
      String groups, boolean synchronous, boolean isParallel) {
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

  private static void assertValidEmployeeMappingOnServer(RegionMapping mapping, String regionName,
      boolean synchronous, boolean isParallel, String tableName) {
    assertValidEmployeeMapping(mapping, tableName);
    validateRegionAlteredOnServer(regionName, synchronous);
    if (!synchronous) {
      validateAsyncEventQueueCreatedOnServer(regionName, isParallel);
    }
  }

  private static void assertValidEmployeeMappingOnLocator(RegionMapping mapping, String regionName,
      String groups, boolean synchronous, boolean isParallel, String tableName) {
    assertValidEmployeeMapping(mapping, tableName);
    validateRegionAlteredInClusterConfig(regionName, groups, synchronous);
    if (!synchronous) {
      validateAsyncEventQueueCreatedInClusterConfig(regionName, groups, isParallel);
    }
  }

  private static void assertValidResourcePDXMapping(RegionMapping mapping, String tableName) {
    assertThat(mapping.getDataSourceName()).isEqualTo("connection");
    assertThat(mapping.getTableName()).isEqualTo(tableName);
    assertThat(mapping.getPdxName())
        .isEqualTo("org.apache.geode.connectors.jdbc.internal.cli.ResourcePDX");
    assertThat(mapping.getIds()).isEqualTo("id");
    assertThat(mapping.getCatalog()).isNull();
    assertThat(mapping.getSchema()).isEqualTo("mySchema");
    List<FieldMapping> fieldMappings = mapping.getFieldMappings();
    assertThat(fieldMappings).hasSize(3);
    assertThat(fieldMappings.get(0))
        .isEqualTo(new FieldMapping("id", "STRING", "ID", "VARCHAR", false));
    assertThat(fieldMappings.get(1))
        .isEqualTo(new FieldMapping("name", "STRING", "NAME", "VARCHAR", true));
    assertThat(fieldMappings.get(2))
        .isEqualTo(new FieldMapping("age", "INT", "AGE", "INTEGER", true));
  }

  private static void assertValidResourcePDXMappingOnServer(RegionMapping mapping,
      String regionName, boolean synchronous, boolean isParallel, String tableName) {
    assertValidResourcePDXMapping(mapping, tableName);
    validateRegionAlteredOnServer(regionName, synchronous);
    if (!synchronous) {
      validateAsyncEventQueueCreatedOnServer(regionName, isParallel);
    }
  }

  private static void assertValidResourcePDXMappingOnLocator(RegionMapping mapping,
      String regionName, String groups, boolean synchronous, boolean isParallel, String tableName) {
    assertValidResourcePDXMapping(mapping, tableName);
    validateRegionAlteredInClusterConfig(regionName, groups, synchronous);
    if (!synchronous) {
      validateAsyncEventQueueCreatedInClusterConfig(regionName, groups, isParallel);
    }
  }

  private File loadTestResource(String fileName) {
    String filePath = createTempFileFromResource(getClass(), fileName).getAbsolutePath();
    assertThat(filePath).isNotNull();

    return new File(filePath);
  }

  private void deployJar() throws IOException {
    File outputJar = createJar();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DEPLOY);
    csb.addOption(CliStrings.JAR, outputJar.getAbsolutePath());
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private File createJar() throws IOException {
    JarBuilder jarBuilder = new JarBuilder();
    File source =
        loadTestResource("/org/apache/geode/connectors/jdbc/internal/cli/ResourcePDX.java");

    File outputJar = new File(temporaryFolder.getRoot(), "output.jar");
    jarBuilder.buildJar(outputJar, source);
    return outputJar;
  }

  private File createClassFile() throws IOException {
    final InMemoryJavaCompiler javaCompiler = new InMemoryJavaCompiler();
    File source =
        loadTestResource("/org/apache/geode/connectors/jdbc/internal/cli/ResourcePDX.java");
    List<InMemoryClassFile> compiledSourceCodes = javaCompiler.compile(source);
    String className = compiledSourceCodes.get(0).getName();
    String fileName = className.substring(className.lastIndexOf(".") + 1) + ".class";
    File file = new File(temporaryFolder.getRoot(), fileName);
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    fileOutputStream.write(compiledSourceCodes.get(0).getByteContent());
    fileOutputStream.close();
    return file;
  }

  @Test
  public void createMappingWithoutExistingPdxNameFails() {
    String region1Name = "region1";
    setupReplicate(region1Name);

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.internal.cli.ResourcePDX");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");
    IgnoredException.addIgnoredException(ClassNotFoundException.class);

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("ClassNotFoundException");
  }

  @Test
  public void createMappingWithDeployedPdxClassSucceeds() throws IOException {
    String region1Name = "region1";
    setupReplicate(region1Name);

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.internal.cli.ResourcePDX");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    deployJar();
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(region1Name);
      assertValidResourcePDXMappingOnServer(mapping, region1Name, false, false, "employeeRegion");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(region1Name, null);
      assertValidResourcePDXMappingOnLocator(regionMapping, region1Name, null, false, false,
          "employeeRegion");
    });
  }

  @Test
  public void createMappingWithPdxClassFileSetToAJarFile() throws IOException {
    String region1Name = "region1";
    setupReplicate(region1Name);
    File jarFile = createJar();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.internal.cli.ResourcePDX");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(PDX_CLASS_FILE, jarFile);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(region1Name);
      assertValidResourcePDXMappingOnServer(mapping, region1Name, false, false, "employeeRegion");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(region1Name, null);
      assertValidResourcePDXMappingOnLocator(regionMapping, region1Name, null, false, false,
          "employeeRegion");
    });
  }

  @Test
  public void createMappingWithNonExistingPdxClassFileFails() {
    String region1Name = "region1";
    setupReplicate(region1Name);

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.internal.cli.ResourcePDX");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(PDX_CLASS_FILE, "NonExistingJarFile.jar");

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("NonExistingJarFile.jar not found.");
  }

  @Test
  public void createMappingWithInvalidJarPdxClassFileFails() {
    String region1Name = "region1";
    setupReplicate(region1Name);
    File invalidFile =
        loadTestResource("/org/apache/geode/connectors/jdbc/internal/cli/ResourcePDX.java");

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.internal.cli.ResourcePDX");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(PDX_CLASS_FILE, invalidFile);

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput(invalidFile + " must end with \".jar\" or \".class\".");
  }

  @Test
  public void createMappingWithPdxClassFileSetToAClassFile()
      throws IOException {
    String region1Name = "region1";
    setupReplicate(region1Name);
    File classFile = createClassFile();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, "org.apache.geode.connectors.jdbc.internal.cli.ResourcePDX");
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(PDX_CLASS_FILE, classFile);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(region1Name);
      assertValidResourcePDXMappingOnServer(mapping, region1Name, false, false, "employeeRegion");
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(region1Name, null);
      assertValidResourcePDXMappingOnLocator(regionMapping, region1Name, null, false, false,
          "employeeRegion");
    });
  }

  private static void assertValidEmployeeMapping(RegionMapping mapping, String tableName) {
    assertThat(mapping.getDataSourceName()).isEqualTo("connection");
    assertThat(mapping.getTableName()).isEqualTo(tableName);
    assertThat(mapping.getPdxName()).isEqualTo(Employee.class.getName());
    assertThat(mapping.getIds()).isEqualTo("id");
    assertThat(mapping.getCatalog()).isNull();
    assertThat(mapping.getSchema()).isEqualTo("mySchema");
    List<FieldMapping> fieldMappings = mapping.getFieldMappings();
    assertThat(fieldMappings).hasSize(3);
    assertThat(fieldMappings.get(0))
        .isEqualTo(new FieldMapping("id", "STRING", "ID", "VARCHAR", false));
    assertThat(fieldMappings.get(1))
        .isEqualTo(new FieldMapping("name", "STRING", "NAME", "VARCHAR", true));
    assertThat(fieldMappings.get(2))
        .isEqualTo(new FieldMapping("age", "INT", "AGE", "INTEGER", true));
  }

  @Test
  public void createMappingUpdatesServiceAndClusterConfig() {
    String regionName = SEPARATOR + TEST_REGION;
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : Arrays.asList(server1, server2)) {
      server.invoke(() -> {
        RegionMapping mapping = getRegionMappingFromService(regionName);
        assertValidMappingOnServer(mapping, regionName, false, false);
      });
    }

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertValidMappingOnLocator(regionMapping, regionName, null, false, false);
    });
  }

  @Test
  public void createMappingWithDomainClassUpdatesServiceAndClusterConfig() {
    String regionName = SEPARATOR + EMPLOYEE_REGION;
    setupReplicate(regionName);
    server1.invoke(() -> {
      ClusterStartupRule.getCache().registerPdxMetaData(new Employee());
    });
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, Employee.class.getName());
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : Arrays.asList(server1, server2, server3, server4)) {
      server.invoke(() -> {
        RegionMapping mapping = getRegionMappingFromService(regionName);
        assertValidEmployeeMappingOnServer(mapping, regionName, false, false, null);
      });
    }

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertValidEmployeeMappingOnLocator(regionMapping, regionName, null, false, false,
          null);
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
    csb.addOption(PDX_NAME, Employee.class.getName());
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region2Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, Employee.class.getName());
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

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeRegion");
    csb.addOption(PDX_NAME, Employee.class.getName());
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

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        region1Name);
    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.containsOutput("age       | INT      | AGE         | INTEGER   | true");
  }

  @Test
  public void createMappingsDoesNotRequirePdxSerializable() {
    String region1Name = "region1";
    setupReplicate(region1Name);

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, region1Name);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "employeeNumeric");
    csb.addOption(PDX_NAME, EmployeeNumeric.class.getName());
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        region1Name);
    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.containsOutput("income    | FLOAT    | INCOME      | REAL      | true");
    commandResultAssert.containsOutput("refid     | LONG     | REFID       | BIGINT    | true");
    commandResultAssert.containsOutput("age       | INT      | AGE         | INTEGER   | true");
  }

  @Test
  public void createMappingUsingRegionNameUsesDomainClass() {
    setupReplicate(EMPLOYEE_LOWER);

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, EMPLOYEE_LOWER);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, Employee.class.getName());
    csb.addOption(ID_NAME, "id");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // TODO We are saving the lower case table name in region mapping,
    // even though the metadata lookup found an upper case table name.
    server1.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromService(EMPLOYEE_LOWER);
      assertValidEmployeeMappingOnServer(mapping, EMPLOYEE_LOWER, false, false, null);
    });

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(EMPLOYEE_LOWER, null);
      assertValidEmployeeMappingOnLocator(regionMapping, EMPLOYEE_LOWER, null, false, false, null);
    });
  }

  @Test
  public void createSynchronousMappingUpdatesServiceAndClusterConfig() {
    String regionName = SEPARATOR + TEST_REGION;
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(SYNCHRONOUS_NAME, "true");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : Arrays.asList(server1, server2, server3, server4)) {
      server.invoke(() -> {
        RegionMapping mapping = getRegionMappingFromService(regionName);
        assertValidMappingOnServer(mapping, regionName, true, false);
      });
    }

    locator.invoke(() -> {
      RegionMapping mapping = getRegionMappingFromClusterConfig(regionName, null);
      assertValidMappingOnLocator(mapping, regionName, null, true, false);
    });
  }

  @Test
  public void createMappingWithPartitionUpdatesServiceAndClusterConfig() {
    String regionName = SEPARATOR + TEST_REGION;
    setupPartition(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "myTable");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(PDX_NAME, IdAndName.class.getName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : Arrays.asList(server1, server2, server3, server4)) {
      server.invoke(() -> {
        RegionMapping mapping = getRegionMappingFromService(regionName);
        assertThat(mapping.getDataSourceName()).isEqualTo("connection");
        assertThat(mapping.getTableName()).isEqualTo("myTable");
        assertThat(mapping.getSchema()).isEqualTo("mySchema");
        assertThat(mapping.getPdxName()).isEqualTo(IdAndName.class.getName());
        validateRegionAlteredOnServer(regionName, false);
        validateAsyncEventQueueCreatedOnServer(regionName, true);
      });
    }

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo(IdAndName.class.getName());
      validateRegionAlteredInClusterConfig(regionName, null, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, null, true);
    });
  }

  @Test
  public void createMappingWithNoTable() {
    String regionName = SEPARATOR + "myTable";
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : Arrays.asList(server1, server2, server3, server4)) {
      server.invoke(() -> {
        RegionMapping mapping = getRegionMappingFromService(regionName);
        assertThat(mapping.getDataSourceName()).isEqualTo("connection");
        assertThat(mapping.getTableName()).isNull();
        assertThat(mapping.getPdxName()).isEqualTo(IdAndName.class.getName());
        validateRegionAlteredOnServer(regionName, false);
        validateAsyncEventQueueCreatedOnServer(regionName, false);
      });
    }

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isNull();
      assertThat(regionMapping.getPdxName()).isEqualTo(IdAndName.class.getName());
      validateRegionAlteredInClusterConfig(regionName, null, false);
      validateAsyncEventQueueCreatedInClusterConfig(regionName, null, false);
    });
  }

  @Test
  public void createExistingRegionMappingFails() {
    String regionName = SEPARATOR + TEST_REGION;
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
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

    for (MemberVM server : Arrays.asList(server1, server2, server3, server4)) {
      server.invoke(() -> {
        RegionMapping mapping = getRegionMappingFromService(regionName);
        assertThat(mapping.getDataSourceName()).isEqualTo("connection");
        assertThat(mapping.getTableName()).isEqualTo("myTable");
        assertThat(mapping.getPdxName()).isEqualTo(IdAndName.class.getName());
      });
    }

    locator.invoke(() -> {
      RegionMapping regionMapping = getRegionMappingFromClusterConfig(regionName, null);
      assertThat(regionMapping.getDataSourceName()).isEqualTo("connection");
      assertThat(regionMapping.getTableName()).isEqualTo("myTable");
      assertThat(regionMapping.getPdxName()).isEqualTo(IdAndName.class.getName());
    });
  }

  @Test
  public void createMappingWithoutPdxNameFails() {
    String regionName = SEPARATOR + TEST_REGION;
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");

    // NOTE: --table is optional so it should not be in the output but it is. See GEODE-3468.
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput(
            "You should specify option (--table, --pdx-name, --pdx-class-file, --synchronous, --id, --catalog, --schema, --if-not-exists, --group) for this command");
  }

  @Test
  public void createMappingWithNonExistentRegionFails() {
    String regionName = SEPARATOR + TEST_REGION;
    setupReplicate(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, "bogusRegion");
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, Employee.class.getName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("A region named bogusRegion must already exist");
  }

  @Test
  public void createMappingWithRegionThatHasALoaderFails() {
    String regionName = SEPARATOR + TEST_REGION;
    setupReplicate(regionName, true);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, Employee.class.getName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("The existing region " + convertRegionPathToName(regionName)
            + " must not already have a cache-loader, but it has " + JdbcLoader.class.getName());
  }

  @Test
  public void createMappingWithExistingQueueFails() {
    String regionName = SEPARATOR + TEST_REGION;
    setupReplicate(regionName);
    setupAsyncEventQueue(regionName);
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(PDX_NAME, Employee.class.getName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("An async-event-queue named "
            + MappingCommandUtils.createAsyncEventQueueName(regionName)
            + " must not already exist.");
  }

  private static class Employee implements PdxSerializable {

    private String id;
    private String name;
    private int age;

    public Employee() {
      // nothing
    }

    Employee(String id, String name, int age) {
      this.id = id;
      this.name = name;
      this.age = age;
    }

    String getId() {
      return id;
    }

    String getName() {
      return name;
    }

    int getAge() {
      return age;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("id", id);
      writer.writeString("name", name);
      writer.writeInt("age", age);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("id");
      name = reader.readString("name");
      age = reader.readInt("age");
    }
  }

  private static class EmployeeNumeric implements PdxSerializerObject {

    private String id;
    private String name;
    private int age;
    private float income;
    private long refid;

    public EmployeeNumeric() {
      // nothing
    }

    EmployeeNumeric(String id, String name, int age, float income, long refid) {
      this.id = id;
      this.name = name;
      this.age = age;
      this.income = income;
      this.refid = refid;
    }

    String getId() {
      return id;
    }

    String getName() {
      return name;
    }

    int getAge() {
      return age;
    }

    float getIncome() {
      return income;
    }

    void setIncome(float income) {
      this.income = income;
    }

    long getRefid() {
      return refid;
    }

    void setRefid(long refid) {
      this.refid = refid;
    }
  }

  private static class IdAndName implements PdxSerializable {

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
