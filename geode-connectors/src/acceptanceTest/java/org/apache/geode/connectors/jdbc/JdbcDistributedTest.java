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
package org.apache.geode.connectors.jdbc;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.cli.MappingCommandUtils;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.AutoSerializableManager;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableConsumerIF;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * End-to-end dunits for JDBC connector
 */
public abstract class JdbcDistributedTest implements Serializable {

  private static final String TABLE_NAME = "employees";
  private static final String REGION_NAME = "employees";
  private static final String DATA_SOURCE_NAME = "TestDataSource";

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  private MemberVM dataserver;
  private MemberVM locator;

  @Before
  public void setup() throws Exception {
    locator = startupRule.startLocatorVM(0);
    gfsh.connectAndVerify(locator);
  }

  public abstract Connection getConnection() throws SQLException;

  public abstract String getConnectionUrl() throws IOException, InterruptedException;

  private void createTable() throws SQLException {
    int locatorPort = locator.getPort();
    dataserver = startupRule.startServerVM(1, x -> x.withConnectionToLocator(locatorPort));
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("Create Table " + TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int not null)");
  }

  private MemberVM createTableForGroup(int idx, String groupName) throws SQLException {
    int locatorPort = locator.getPort();
    MemberVM server = startupRule.startServerVM(idx, groupName, locatorPort);
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("Create Table " + TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int not null)");
    return server;
  }

  private MemberVM addServerForGroup(int idx, String groupName) {
    int locatorPort = locator.getPort();
    MemberVM server = startupRule.startServerVM(idx, groupName, locatorPort);
    return server;
  }

  private void alterTable() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("Alter Table " + TABLE_NAME
        + " add column new_column varchar(10)");
  }

  private void createTableForAllSupportedFields() throws SQLException {
    int locatorPort = locator.getPort();
    dataserver = startupRule.startServerVM(1,
        x -> x.withConnectionToLocator(locatorPort).withPDXReadSerialized());
    Connection connection = getConnection();
    DatabaseMetaData metaData = connection.getMetaData();
    String quote = metaData.getIdentifierQuoteString();
    Statement statement = connection.createStatement();
    createSupportedFieldsTable(statement, TABLE_NAME, quote);
  }

  protected abstract void createSupportedFieldsTable(Statement statement, String tableName,
      String quote) throws SQLException;

  protected abstract void insertNullDataForAllSupportedFieldsTable(MemberVM dataserver, String key,
      String tableName) throws SQLException;

  private static void insertDataForAllSupportedFieldsTable(String connectionUrl, String key,
      ClassWithSupportedPdxFields data)
      throws SQLException {
    Connection connection = DriverManager.getConnection(connectionUrl);
    DatabaseMetaData metaData = connection.getMetaData();
    String quote = metaData.getIdentifierQuoteString();

    String insertQuery =
        "Insert into " + quote + TABLE_NAME + quote + " values (" + "?,?,?,?,?,?,?,?,?,?,?,?,?)";
    System.out.println("### Query is :" + insertQuery);
    PreparedStatement statement = connection.prepareStatement(insertQuery);
    statement.setObject(1, key);
    statement.setObject(2, data.isAboolean());
    statement.setObject(3, data.getAbyte());
    statement.setObject(4, data.getAshort());
    statement.setObject(5, data.getAnint());
    statement.setObject(6, data.getAlong());
    statement.setObject(7, data.getAfloat());
    statement.setObject(8, data.getAdouble());
    statement.setObject(9, data.getAstring());
    statement.setObject(10, new java.sql.Timestamp(data.getAdate().getTime()));
    statement.setObject(11, data.getAnobject());
    statement.setObject(12, data.getAbytearray());
    statement.setObject(13, new Character(data.getAchar()).toString());

    statement.execute();
  }

  @After
  public void tearDown() throws Exception {
    closeDB();
  }

  private void closeDB() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      String quote = metaData.getIdentifierQuoteString();
      try (Statement statement = connection.createStatement()) {
        try {
          statement.execute("Drop table " + TABLE_NAME);
        } catch (SQLException ignore) {
        }

        try {
          statement.execute("Drop table " + quote + TABLE_NAME + quote);
        } catch (SQLException ignore) {
        }
      }
    }
  }

  @Test
  public void throwsExceptionWhenNoMappingExistsUsingWriter() throws Exception {
    createTable();
    StringBuilder createRegionCmd = new StringBuilder();
    createRegionCmd.append("create region --name=" + REGION_NAME + " --type=REPLICATE"
        + " --cache-writer=" + JdbcWriter.class.getName());
    gfsh.executeAndAssertThat(createRegionCmd.toString()).statusIsSuccess();
    createJdbcDataSource();

    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("name", "Emp1").writeInt("age", 55).create();
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(() -> region.put("key1", pdxEmployee1))
          .isExactlyInstanceOf(JdbcConnectorException.class).hasMessage(
              "JDBC mapping for region employees not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    });
  }

  @Test
  public void throwsExceptionWhenNoMappingExistsUsingAsyncWriter() throws Exception {
    createTable();
    IgnoredException.addIgnoredException("JdbcConnectorException");
    StringBuilder createRegionCmd = new StringBuilder();
    createAsyncListener("JAW");
    createRegionCmd.append("create region --name=" + REGION_NAME + " --type=REPLICATE"
        + " --async-event-queue-id=JAW");
    gfsh.executeAndAssertThat(createRegionCmd.toString()).statusIsSuccess();
    createJdbcDataSource();

    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("name", "Emp1").writeInt("age", 55).create();
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put("key1", pdxEmployee1);

      JdbcAsyncWriter asyncWriter = (JdbcAsyncWriter) ClusterStartupRule.getCache()
          .getAsyncEventQueue("JAW").getAsyncEventListener();
      await().untilAsserted(() -> {
        assertThat(asyncWriter.getFailedEvents()).isEqualTo(1);
      });
    });
  }

  @Test
  public void throwsExceptionWhenMappingDoesNotMatchTableDefinitionOnInitialOperation()
      throws Exception {
    IgnoredException.addIgnoredException(
        "Error detected when comparing mapping for region \"employees\" with table definition:");
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, true);
    alterTable();
    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "id1").writeString("name", "Emp1").writeInt("age", 55).create();

      String key = "id1";
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(() -> region.put(key, pdxEmployee1))
          .isExactlyInstanceOf(JdbcConnectorException.class).hasMessage(
              "Jdbc mapping for \"" + REGION_NAME
                  + "\" does not match table definition, check logs for more details.");
    });
  }

  @Test
  public void throwsExceptionWhenMappingDoesNotMatchTableDefinitionOnLoaderAlreadyInitialized()
      throws Exception {
    IgnoredException.addIgnoredException(
        "Error detected when comparing mapping for region \"employees\" with table definition:");
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, true);
    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "id1").writeString("name", "Emp1").writeInt("age", 55).create();

      String key = "id1";
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put(key, pdxEmployee1); // this initializes the writer
      region.invalidate(key);
      region.get(key); // this initializes the loader
      region.invalidate(key);
    });
    alterTable();
    dataserver.invoke(() -> {
      String key = "id1";
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(() -> region.get(key))
          .isExactlyInstanceOf(JdbcConnectorException.class).hasMessage(
              "The jdbc-mapping does not contain the column name \"new_column\"."
                  + " This is probably caused by a column being added to the table after the jdbc-mapping was created.");
    });
  }

  @Test
  public void throwsExceptionWhenMappingDoesNotMatchTableDefinitionOnServerStartup()
      throws Exception {
    IgnoredException.addIgnoredException(
        "Error detected when comparing mapping for region \"employees\" with table definition:");
    IgnoredException.addIgnoredException(
        "Jdbc mapping for \"employees\" does not match table definition, check logs for more details.");
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, true);
    alterTable();
    int locatorPort = locator.getPort();
    assertThatThrownBy(
        () -> startupRule.startServerVM(3, x -> x.withConnectionToLocator(locatorPort)))
            .hasCauseExactlyInstanceOf(JdbcConnectorException.class).hasStackTraceContaining(
                "Jdbc mapping for \"employees\" does not match table definition, check logs for more details.");
  }

  private void validateBothServersAndAccessors(MemberVM server1, MemberVM server2,
      MemberVM accessor1, MemberVM accessor2) {
    for (MemberVM server : Arrays.asList(server1, server2, accessor1, accessor2)) {
      server.invoke(() -> {
        PdxInstance pdxEmployee1 =
            ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
                .writeString("id", "id1").writeString("name", "Emp1").writeInt("age", 55).create();

        String pdxkey1 = "pdxkey1";
        Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put(pdxkey1, pdxEmployee1);
        Employee employee1 = new Employee("key1", "name1", 30);
        region.put("key1", employee1);
        region.invalidate(pdxkey1);
        region.invalidate("key1");
        await().untilAsserted(() -> {
          assertThat(region.get(pdxkey1)).isNotNull();
          assertThat(region.get("key1")).isNotNull();
        });
        Employee pdxEmployee2 = (Employee) region.get(pdxkey1);
        Employee employee2 = (Employee) region.get("key1");
        assertThat(pdxEmployee2.getName()).isEqualTo("Emp1");
        assertThat(employee2.getName()).isEqualTo("name1");
      });
    }

    for (MemberVM server : Arrays.asList(server1, server2)) {
      server.invoke(() -> {
        String queueName = MappingCommandUtils.createAsyncEventQueueName(REGION_NAME);
        AsyncEventQueue queue = ClusterStartupRule.getCache().getAsyncEventQueue(queueName);
        assertThat(queue).isNotNull();
        await().untilAsserted(() -> {
          assertThat(queue.size()).isEqualTo(0);
        });
      });
    }
  }

  @Test
  public void startAccessorForPRThenPutAndGet() throws Exception {
    MemberVM server1 = createTableForGroup(4, "datagroup");
    MemberVM server2 = addServerForGroup(5, "datagroup");
    MemberVM accessor1 = addServerForGroup(6, "accessorgroup");
    MemberVM accessor2 = addServerForGroup(7, "accessorgroup");

    createJdbcDataSource();
    createPartitionedRegionUsingGfshForGroup(false, "datagroup");
    createAsyncMappingForGroup(REGION_NAME, DATA_SOURCE_NAME, Employee.class.getName(),
        "datagroup");
    createPartitionedRegionUsingGfshForGroup(true, "accessorgroup");
    createAsyncMappingForGroup(REGION_NAME, DATA_SOURCE_NAME, Employee.class.getName(),
        "accessorgroup");

    validateBothServersAndAccessors(server1, server2, accessor1, accessor2);

    for (int i = 4; i <= 7; i++) {
      startupRule.stop(i);
    }
  }

  @Test
  public void startAccessorForRRThenPutAndGet() throws Exception {
    MemberVM server1 = createTableForGroup(4, "datagroup");
    MemberVM server2 = addServerForGroup(5, "datagroup");
    MemberVM accessor1 = addServerForGroup(6, "accessorgroup");
    MemberVM accessor2 = addServerForGroup(7, "accessorgroup");

    createJdbcDataSource();
    createReplicatedRegionUsingGfshForGroup(false, "datagroup");
    createAsyncMappingForGroup(REGION_NAME, DATA_SOURCE_NAME, Employee.class.getName(),
        "datagroup");
    createReplicatedRegionUsingGfshForGroup(true, "accessorgroup");
    createAsyncMappingForGroup(REGION_NAME, DATA_SOURCE_NAME, Employee.class.getName(),
        "accessorgroup");

    validateBothServersAndAccessors(server1, server2, accessor1, accessor2);

    for (int i = 4; i <= 7; i++) {
      startupRule.stop(i);
    }
  }

  @Test
  public void throwsExceptionWhenNoDataSourceExists() throws Exception {
    createTable();
    createReplicatedRegionUsingGfsh();
    IgnoredException.addIgnoredException(JdbcConnectorException.class);
    final String commandStr = "create jdbc-mapping --region=" + REGION_NAME
        + " --data-source=" + DATA_SOURCE_NAME
        + " --pdx-name=" + Employee.class.getName();
    gfsh.executeAndAssertThat(commandStr).statusIsError()
        .containsOutput("JDBC data-source named \"" + DATA_SOURCE_NAME
            + "\" not found. Create it with gfsh 'create data-source --pooled --name="
            + DATA_SOURCE_NAME + "'.");
  }

  @Test
  public void serverStartupSucceedsForPartitionedRegionAfterMappingIsCreated()
      throws Exception {
    createTable();
    createPartitionedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, false);
    int locatorPort = locator.getPort();
    MemberVM server3 =
        startupRule.startServerVM(3, x -> x.withConnectionToLocator(locatorPort));
    server3.invoke(() -> {
      RegionMapping mapping =
          ClusterStartupRule.getCache().getService(JdbcConnectorService.class)
              .getMappingForRegion(REGION_NAME);
      assertThat(mapping.getDataSourceName()).isEqualTo(DATA_SOURCE_NAME);
      assertThat(mapping.getPdxName()).isEqualTo(Employee.class.getName());
      assertThat(mapping.getTableName()).isEqualTo(TABLE_NAME);
      List<FieldMapping> fieldMappings = mapping.getFieldMappings();
      assertThat(fieldMappings.size()).isEqualTo(3);
      assertThat(fieldMappings.get(0)).isEqualTo(
          new FieldMapping("name", FieldType.STRING.name(), "name", JDBCType.VARCHAR.name(),
              true));
      assertThat(fieldMappings.get(1)).isEqualTo(
          new FieldMapping("id", FieldType.STRING.name(), "id", JDBCType.VARCHAR.name(), false));
      assertThat(fieldMappings.get(2)).isEqualTo(
          new FieldMapping("age", FieldType.INT.name(), "age", JDBCType.INTEGER.name(), false));

      String queueName = MappingCommandUtils.createAsyncEventQueueName(REGION_NAME);
      AsyncEventQueue queue = ClusterStartupRule.getCache().getAsyncEventQueue(queueName);
      assertThat(queue).isNotNull();
      assertThat(queue.getAsyncEventListener()).isInstanceOf(JdbcAsyncWriter.class);
    });
  }

  @Test
  public void verifyDateToDate() throws Exception {
    int locatorPort = locator.getPort();
    dataserver = startupRule.startServerVM(1, x -> x.withConnectionToLocator(locatorPort));
    String connectionUrl = getConnectionUrl();
    dataserver.invoke(() -> {
      Connection connection = DriverManager.getConnection(connectionUrl);
      Statement statement = connection.createStatement();
      statement.execute(
          "Create Table " + TABLE_NAME + " (id varchar(10) primary key not null, "
              + TestDate.DATE_FIELD_NAME + " date not null)");
    });
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, TestDate.class.getName(), true);
    final String key = "emp1";
    final java.sql.Date sqlDate = java.sql.Date.valueOf("1982-09-11");
    final Date jdkDate = new Date(sqlDate.getTime());
    dataserver.invoke(() -> {
      PdxInstance testDateInput =
          ClusterStartupRule.getCache().createPdxInstanceFactory(TestDate.class.getName())
              .writeString("id", "key1").writeDate(TestDate.DATE_FIELD_NAME, jdkDate).create();

      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put(key, testDateInput);
      region.invalidate(key);
    });
    ClientVM client = getClientVM();
    createClientRegion(client);
    client.invoke(() -> {
      Region<String, TestDate> region =
          ClusterStartupRule.getClientCache().getRegion(REGION_NAME);
      TestDate getResult = region.get(key);
      assertThat(getResult.getMyDate()).isEqualTo(jdkDate);
    });
  }

  @Test
  public void verifyDateToTime() throws Exception {
    int locatorPort = locator.getPort();
    dataserver = startupRule.startServerVM(1, x -> x.withConnectionToLocator(locatorPort));
    String connectionUrl = getConnectionUrl();
    dataserver.invoke(() -> {
      Connection connection = DriverManager.getConnection(connectionUrl);
      Statement statement = connection.createStatement();
      statement.execute(
          "Create Table " + TABLE_NAME + " (id varchar(10) primary key not null, "
              + TestDate.DATE_FIELD_NAME + " time not null)");
    });
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, TestDate.class.getName(), true);
    final String key = "emp1";
    final java.sql.Time sqlTime = java.sql.Time.valueOf("23:59:59");
    final Date jdkDate = new Date(sqlTime.getTime());
    dataserver.invoke(() -> {
      PdxInstance testDateInput =
          ClusterStartupRule.getCache().createPdxInstanceFactory(TestDate.class.getName())
              .writeString("id", "key1").writeDate(TestDate.DATE_FIELD_NAME, jdkDate).create();

      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put(key, testDateInput);
      region.invalidate(key);
    });
    ClientVM client = getClientVM();
    createClientRegion(client);
    client.invoke(() -> {
      Region<String, TestDate> region =
          ClusterStartupRule.getClientCache().getRegion(REGION_NAME);
      TestDate getResult = region.get(key);
      assertThat(getResult.getMyDate()).isEqualTo(jdkDate);
    });
  }

  @Test
  public void verifyDateToTimestamp() throws Exception {
    int locatorPort = locator.getPort();
    dataserver = startupRule.startServerVM(1, x -> x.withConnectionToLocator(locatorPort));
    createTableWithTimeStamp(dataserver, getConnectionUrl(), TABLE_NAME, TestDate.DATE_FIELD_NAME);

    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, TestDate.class.getName(), true);
    final String key = "emp1";
    final java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf("1982-09-11 23:59:59.123");
    final Date jdkDate = new Date(sqlTimestamp.getTime());
    dataserver.invoke(() -> {
      PdxInstance testDateInput =
          ClusterStartupRule.getCache().createPdxInstanceFactory(TestDate.class.getName())
              .writeString("id", "key1").writeDate(TestDate.DATE_FIELD_NAME, jdkDate).create();

      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put(key, testDateInput);
      region.invalidate(key);
    });
    ClientVM client = getClientVM();
    createClientRegion(client);
    client.invoke(() -> {
      Region<String, TestDate> region =
          ClusterStartupRule.getClientCache().getRegion(REGION_NAME);
      TestDate getResult = region.get(key);
      assertThat(getResult.getMyDate()).isEqualTo(jdkDate);
    });
  }

  protected void createTableWithTimeStamp(MemberVM vm, String connectionUrl, String tableName,
      String columnName) {
    vm.invoke(() -> {
      Connection connection = DriverManager.getConnection(connectionUrl);
      Statement statement = connection.createStatement();
      statement.execute("Create Table " + tableName
          + " (id varchar(10) primary key not null, " + columnName + " timestamp not null)");
    });
  }

  @Test
  public void putWritesToDB() throws Exception {
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, true);
    String finalUrl = getConnectionUrl();
    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "key1").writeString("name", "Emp1").writeInt("age", 55).create();

      String key = "emp1";
      ClusterStartupRule.getCache().getRegion(REGION_NAME).put(key, pdxEmployee1);
      assertTableHasEmployeeData(finalUrl, 1, pdxEmployee1, key);
    });
  }

  @Test
  public void putAsyncWritesToDB() throws Exception {
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, false);
    String finalUrl = getConnectionUrl();
    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "key1").writeString("name", "Emp1").writeInt("age", 55).create();

      String key = "emp1";
      ClusterStartupRule.getCache().getRegion(REGION_NAME).put(key, pdxEmployee1);
      await().untilAsserted(() -> {
        assertTableHasEmployeeData(finalUrl, 1, pdxEmployee1, key);
      });
    });
  }

  @Test
  public void putAsyncWithPartitionWritesToDB() throws Exception {
    createTable();
    createPartitionedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, false);
    String finalUrl = getConnectionUrl();
    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "key1").writeString("name", "Emp1").writeInt("age", 55).create();

      String key = "emp1";
      ClusterStartupRule.getCache().getRegion(REGION_NAME).put(key, pdxEmployee1);
      await().untilAsserted(() -> {
        assertTableHasEmployeeData(finalUrl, 1, pdxEmployee1, key);
      });
    });
  }

  @Test
  public void getReadsFromEmptyDB() throws Exception {
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, true);
    dataserver.invoke(() -> {
      String key = "emp1";
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      Object value = region.get(key);
      assertThat(value).isNull();
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void getReadsFromDB() throws Exception {
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, true);
    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "id1").writeString("name", "Emp1").writeInt("age", 55).create();

      String key = "id1";
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put(key, pdxEmployee1);
      region.invalidate(key);

      JdbcWriter<Object, Object> writer =
          (JdbcWriter<Object, Object>) region.getAttributes().getCacheWriter();
      long writeCallsCompletedBeforeGet = writer.getTotalEvents();

      Employee result = (Employee) region.get(key);
      assertThat(result.getId()).isEqualTo(key);
      assertThat(result.getName()).isEqualTo("Emp1");
      assertThat(result.getAge()).isEqualTo(55);
      assertThat(writer.getTotalEvents()).isEqualTo(writeCallsCompletedBeforeGet);
    });
  }

  @Test
  public void getReadsFromDBWithCompositeKey() throws Exception {
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, Employee.class.getName(), true, "id,age");
    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "id1").writeString("name", "Emp1").writeInt("age", 55).create();
      PdxInstance key =
          ClusterStartupRule.getCache().createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
              .writeField("id", (String) pdxEmployee1.getField("id"), String.class)
              .writeField("age", (Integer) pdxEmployee1.getField("age"), int.class).create();
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put(key, pdxEmployee1);
      region.invalidate(key);
      JdbcWriter<Object, Object> writer =
          (JdbcWriter<Object, Object>) region.getAttributes().getCacheWriter();
      long writeCallsCompletedBeforeGet = writer.getTotalEvents();

      Employee result = (Employee) region.get(key);

      assertThat(result.getId()).isEqualTo("id1");
      assertThat(result.getName()).isEqualTo("Emp1");
      assertThat(result.getAge()).isEqualTo(55);
      assertThat(writer.getTotalEvents()).isEqualTo(writeCallsCompletedBeforeGet);
    });
  }

  @Test
  public void getReadsFromDBWithAsyncWriter() throws Exception {
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, false);
    dataserver.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "id1").writeString("name", "Emp1").writeInt("age", 55).create();
      String key = "id1";
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      JdbcAsyncWriter asyncWriter = (JdbcAsyncWriter) ClusterStartupRule.getCache()
          .getAsyncEventQueue(MappingCommandUtils.createAsyncEventQueueName(REGION_NAME))
          .getAsyncEventListener();

      region.put(key, pdxEmployee1);
      await().untilAsserted(() -> {
        assertThat(asyncWriter.getSuccessfulEvents()).isEqualTo(1);
      });
      region.invalidate(key);
      Employee result = (Employee) region.get(key);

      assertThat(result.getId()).isEqualTo(pdxEmployee1.getField("id"));
      assertThat(result.getName()).isEqualTo(pdxEmployee1.getField("name"));
      assertThat(result.getAge()).isEqualTo(pdxEmployee1.getField("age"));
      await().untilAsserted(() -> {
        assertThat(asyncWriter.getIgnoredEvents()).isEqualTo(1);
      });
    });
  }

  @Test
  public void getReadsFromDBWithPdxClassName() throws Exception {
    createTable();
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, Employee.class.getName(), true);
    dataserver.invoke(() -> {
      String key = "id1";
      Employee value = new Employee(key, "Emp1", 55);
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put(key, value);
      region.invalidate(key);

      Employee result = (Employee) region.get(key);
      assertThat(result.getName()).isEqualTo("Emp1");
      assertThat(result.getAge()).isEqualTo(55);
    });
  }

  @Test
  public void clientGetReadsFromDBWithPdxClassName() throws Exception {
    createTableForAllSupportedFields();
    ClientVM client = getClientVM();
    createClientRegion(client);

    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, ClassWithSupportedPdxFields.class.getName(), true);
    client.invoke(() -> {
      String key = "id1";
      ClassWithSupportedPdxFields value =
          new ClassWithSupportedPdxFields(key, true, (byte) 1, (short) 2,
              3, 4, 5.5f, 6.0, "BigEmp", new Date(0), "BigEmpObject", new byte[] {1, 2}, 'c');
      Region<String, ClassWithSupportedPdxFields> region =
          ClusterStartupRule.getClientCache().getRegion(REGION_NAME);
      region.put(key, value);
      region.invalidate(key);

      ClassWithSupportedPdxFields result = region.get(key);
      assertThat(result).isEqualTo(value);
    });
  }

  @Test
  public void clientPutsAndGetsWithNullFieldsWithPdxClassName() throws Exception {
    createTableForAllSupportedFields();
    ClientVM client = getClientVM();
    createClientRegion(client);

    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, ClassWithSupportedPdxFields.class.getName(), true);
    client.invoke(() -> {
      String key = "id1";
      ClassWithSupportedPdxFields value = new ClassWithSupportedPdxFields(key);
      Region<String, ClassWithSupportedPdxFields> region =
          ClusterStartupRule.getClientCache().getRegion(REGION_NAME);
      region.put(key, value);
      region.invalidate(key);

      ClassWithSupportedPdxFields result = region.get(key);
      assertThat(result).isEqualTo(value);
    });
  }

  @Test
  public void clientRegistersPdxAndReadsFromDBWithPdxClassName() throws Exception {
    createTableForAllSupportedFields();
    ClientVM client = getClientVM();
    createClientRegion(client);
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, ClassWithSupportedPdxFields.class.getName(), true);
    String key = "id1";
    ClassWithSupportedPdxFields value =
        new ClassWithSupportedPdxFields(key, true, (byte) 1, (short) 2,
            3, 4, 5.5f, 6.0, "BigEmp", new Date(0), "BigEmpObject", new byte[] {1, 2}, 'c');

    String finalUrl = getConnectionUrl();
    dataserver.invoke(() -> {
      insertDataForAllSupportedFieldsTable(finalUrl, key, value);
    });

    client.invoke(() -> {
      ClusterStartupRule.getClientCache().registerPdxMetaData(new ClassWithSupportedPdxFields());

      Region<String, ClassWithSupportedPdxFields> region =
          ClusterStartupRule.getClientCache().getRegion(REGION_NAME);

      ClassWithSupportedPdxFields result = region.get(key);
      assertThat(result).isEqualTo(value);
    });
  }

  @Test
  public void clientRegistersPdxAndReadsFromDBContainingNullColumnsWithPdxClassName()
      throws Exception {
    createTableForAllSupportedFields();
    ClientVM client = getClientVM();
    createClientRegion(client);
    createReplicatedRegionUsingGfsh();
    createJdbcDataSource();
    createMapping(REGION_NAME, DATA_SOURCE_NAME, ClassWithSupportedPdxFields.class.getName(), true);
    String key = "id1";
    ClassWithSupportedPdxFields value = new ClassWithSupportedPdxFields(key);

    insertNullDataForAllSupportedFieldsTable(dataserver, key, TABLE_NAME);

    client.invoke(() -> {
      ClusterStartupRule.getClientCache().registerPdxMetaData(new ClassWithSupportedPdxFields());

      Region<String, ClassWithSupportedPdxFields> region =
          ClusterStartupRule.getClientCache().getRegion(REGION_NAME);

      ClassWithSupportedPdxFields result = region.get(key);
      assertThat(result).isEqualTo(value);
    });
  }

  private ClientVM getClientVM() throws Exception {
    int locatorPort = locator.getPort();
    SerializableConsumerIF<ClientCacheFactory> cacheSetup = cf -> {
      System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true");
      cf.addPoolLocator("localhost", locatorPort);
      cf.setPdxSerializer(
          new ReflectionBasedAutoSerializer(ClassWithSupportedPdxFields.class.getName()));
    };
    return startupRule.startClientVM(2, c -> c.withCacheSetup(cacheSetup));
  }

  private void createClientRegion(ClientVM client) {
    client.invoke(() -> {
      ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);
    });
  }

  private void createJdbcDataSource() throws Exception {
    final String commandStr =
        "create data-source --pooled --name=" + DATA_SOURCE_NAME + " --url=" + getConnectionUrl();
    gfsh.executeAndAssertThat(commandStr).statusIsSuccess();
  }

  private void createAsyncListener(String id) {
    final String commandStr =
        "create async-event-queue --id=" + id + " --listener=" + JdbcAsyncWriter.class.getName()
            + " --batch-size=1 --batch-time-interval=0 --parallel=false";
    gfsh.executeAndAssertThat(commandStr).statusIsSuccess();
  }

  private void createReplicatedRegionUsingGfsh() {
    StringBuilder createRegionCmd = new StringBuilder();
    createRegionCmd.append("create region --name=" + REGION_NAME + " --type=REPLICATE");
    gfsh.executeAndAssertThat(createRegionCmd.toString()).statusIsSuccess();
  }

  private void createReplicatedRegionUsingGfshForGroup(boolean isAccessor, String groupName) {
    StringBuilder createRegionCmd = new StringBuilder();
    createRegionCmd.append("create region --name=" + REGION_NAME + " --groups=" + groupName
        + " --if-not-exists=true"
        + (isAccessor
            ? " --type=" + RegionShortcut.REPLICATE_PROXY.name()
            : " --type=" + RegionShortcut.REPLICATE.name()));
    gfsh.executeAndAssertThat(createRegionCmd.toString()).statusIsSuccess();
  }

  private void createPartitionedRegionUsingGfshForGroup(boolean isAccessor, String groupName) {
    StringBuilder createRegionCmd = new StringBuilder();
    createRegionCmd
        .append("create region --name=" + REGION_NAME + " --groups=" + groupName
            + " --if-not-exists=true"
            + (isAccessor
                ? " --type=" + RegionShortcut.PARTITION_PROXY.name()
                : " --type=" + RegionShortcut.PARTITION.name())
            + " --redundant-copies=1");
    gfsh.executeAndAssertThat(createRegionCmd.toString()).statusIsSuccess();
  }

  private void createAsyncMappingForGroup(String regionName, String connectionName,
      String pdxClassName,
      String groupName) {
    final String commandStr = "create jdbc-mapping --region=" + regionName
        + " --data-source=" + connectionName
        + " --table=" + TABLE_NAME
        + " --synchronous=false"
        + " --if-not-exists=true"
        + " --pdx-name=" + pdxClassName
        + " --groups=" + groupName;
    gfsh.executeAndAssertThat(commandStr).statusIsSuccess();
  }

  private void createPartitionedRegionUsingGfsh() {
    StringBuilder createRegionCmd = new StringBuilder();
    createRegionCmd
        .append("create region --name=" + REGION_NAME + " --type=PARTITION --redundant-copies=1");
    gfsh.executeAndAssertThat(createRegionCmd.toString()).statusIsSuccess();
  }

  private void createMapping(String regionName, String connectionName, boolean synchronous) {
    createMapping(regionName, connectionName, Employee.class.getName(), synchronous, null);
  }

  private void createMapping(String regionName, String connectionName, String pdxClassName,
      boolean synchronous) {
    createMapping(regionName, connectionName, pdxClassName, synchronous, null);
  }

  private void createMapping(String regionName, String connectionName, String pdxClassName,
      boolean synchronous, String ids) {
    final String commandStr = "create jdbc-mapping --region=" + regionName
        + " --data-source=" + connectionName
        + " --table=" + TABLE_NAME
        + " --synchronous=" + synchronous
        + " --pdx-name=" + pdxClassName
        + ((ids != null) ? (" --id=" + ids) : "");
    gfsh.executeAndAssertThat(commandStr).statusIsSuccess();
    if (!synchronous) {
      final String alterAsyncQueue =
          "alter async-event-queue --id="
              + MappingCommandUtils.createAsyncEventQueueName(regionName)
              + " --batch-size=1 --batch-time-interval=0";
      gfsh.executeAndAssertThat(alterAsyncQueue).statusIsSuccess();
    }
  }

  private static void assertTableHasEmployeeData(String connectionUrl, int size,
      PdxInstance employee, String key)
      throws SQLException {
    Connection connection = DriverManager.getConnection(connectionUrl);
    Statement statement = connection.createStatement();
    await().untilAsserted(() -> {
      assertThat(getRowCount(statement, TABLE_NAME)).isEqualTo(size);
    });

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_NAME + " order by id asc");
    assertThat(resultSet.next()).isTrue();
    assertThat(resultSet.getString("id")).isEqualTo(key);
    assertThat(resultSet.getString("name")).isEqualTo(employee.getField("name"));
    assertThat(resultSet.getObject("age")).isEqualTo(employee.getField("age"));
  }

  private static int getRowCount(Statement stmt, String tableName) {
    try {
      ResultSet resultSet = stmt.executeQuery("select count(*) from " + tableName);
      resultSet.next();
      return resultSet.getInt(1);
    } catch (SQLException e) {
      return -1;
    }
  }

}
