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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.AutoSerializableManager;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * End-to-end dunits for jdbc connector
 */
@Category(DistributedTest.class)
public class JdbcDUnitTest implements Serializable {

  private static final String DB_NAME = "DerbyDB";
  private static final String TABLE_NAME = "employees";
  private static final String REGION_NAME = "employees";
  private static final String CONNECTION_URL = "jdbc:derby:memory:" + DB_NAME + ";create=true";
  private static final String CONNECTION_NAME = "TestConnection";


  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public transient ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  private MemberVM server;
  private MemberVM locator;

  @Before
  public void setup() throws Exception {
    locator = startupRule.startLocatorVM(0);
    gfsh.connectAndVerify(locator);
  }

  private void createTable() throws SQLException {
    server = startupRule.startServerVM(1, x -> x.withConnectionToLocator(locator.getPort()));
    server.invoke(() -> {
      Connection connection = DriverManager.getConnection(CONNECTION_URL);
      Statement statement = connection.createStatement();
      statement.execute("Create Table " + TABLE_NAME
          + " (id varchar(10) primary key not null, name varchar(10), age int)");
    });
  }

  private void createTableForAllSupportedFields() throws SQLException {
    server = startupRule.startServerVM(1,
        x -> x.withConnectionToLocator(locator.getPort()).withPDXReadSerialized());
    server.invoke(() -> {
      Connection connection = DriverManager.getConnection(CONNECTION_URL);
      Statement statement = connection.createStatement();
      statement.execute("Create Table " + TABLE_NAME + " (id varchar(10) primary key not null, "
          + "aboolean smallint, " + "abyte smallint, " + "ashort smallint, " + "anint int, "
          + "along bigint, " + "afloat float, " + "adouble double, " + "astring varchar(10), "
          + "adate timestamp, " + "anobject varchar(20), " + "abytearray blob(100), "
          + "achar char(1))");
    });
  }

  private void insertNullDataForAllSupportedFieldsTable(String key) {
    server.invoke(() -> {
      Connection connection = DriverManager.getConnection(CONNECTION_URL);

      String insertQuery = "Insert into " + TABLE_NAME + " values (" + "?,?,?,?,?,?,?,?,?,?,?,?,?)";
      System.out.println("### Query is :" + insertQuery);
      PreparedStatement statement = connection.prepareStatement(insertQuery);
      statement.setObject(1, key);
      statement.setNull(2, Types.SMALLINT);
      statement.setNull(3, Types.SMALLINT);
      statement.setNull(4, Types.SMALLINT);
      statement.setNull(5, Types.INTEGER);
      statement.setNull(6, Types.BIGINT);
      statement.setNull(7, Types.FLOAT);
      statement.setNull(8, Types.DOUBLE);
      statement.setNull(9, Types.VARCHAR);
      statement.setNull(10, Types.TIMESTAMP);
      statement.setNull(11, Types.VARCHAR);
      statement.setNull(12, Types.BLOB);
      statement.setNull(13, Types.CHAR);

      statement.execute();
    });
  }

  private void insertDataForAllSupportedFieldsTable(String key, ClassWithSupportedPdxFields data) {
    server.invoke(() -> {
      Connection connection = DriverManager.getConnection(CONNECTION_URL);

      String insertQuery = "Insert into " + TABLE_NAME + " values (" + "?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
      statement.setObject(10, data.getAdate());
      statement.setObject(11, data.getAnobject());
      statement.setObject(12, data.getAbytearray());
      statement.setObject(13, new Character(data.getAchar()).toString());

      statement.execute();
    });
  }

  @After
  public void tearDown() throws Exception {
    server.invoke(() -> {
      closeDB();
    });
  }

  private void closeDB() throws Exception {
    try {
      Connection connection = DriverManager.getConnection(CONNECTION_URL);
      Statement statement = connection.createStatement();
      if (statement == null) {
        statement = connection.createStatement();
      }
      statement.execute("Drop table " + TABLE_NAME);
      statement.close();

      connection.close();
    } catch (SQLException ex) {
      System.out.println("SQL Exception is thrown while closing the database.");
    }
  }

  @Test
  public void throwsExceptionWhenNoMappingExistsUsingWriter() throws Exception {
    createTable();
    createRegionUsingGfsh(true, false, false);
    createJdbcConnection();

    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("name", "Emp1").writeInt("age", 55).create();
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(() -> region.put("key1", pdxEmployee1))
          .isExactlyInstanceOf(JdbcConnectorException.class).hasMessage(
              "JDBC mapping for region employees not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    });
  }

  @Test
  public void throwsExceptionWhenNoMappingExistsUsingAsyncWriter() throws Exception {
    createTable();
    IgnoredException.addIgnoredException("JdbcConnectorException");
    createRegionUsingGfsh(false, true, false);
    createJdbcConnection();

    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("name", "Emp1").writeInt("age", 55).create();
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put("key1", pdxEmployee1);

      JdbcAsyncWriter asyncWriter = (JdbcAsyncWriter) ClusterStartupRule.getCache()
          .getAsyncEventQueue("JAW").getAsyncEventListener();
      Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
        assertThat(asyncWriter.getFailedEvents()).isEqualTo(1);
      });

    });
  }

  @Test
  public void throwsExceptionWhenNoMappingMatches() throws Exception {
    createTable();
    createRegionUsingGfsh(true, false, false);
    createJdbcConnection();
    createMapping("NoSuchRegion", CONNECTION_NAME);

    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("name", "Emp1").writeInt("age", 55).create();
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(() -> region.put("key1", pdxEmployee1))
          .isExactlyInstanceOf(JdbcConnectorException.class).hasMessage(
              "JDBC mapping for region employees not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    });
  }

  @Test
  public void throwsExceptionWhenNoConnectionExists() throws Exception {
    createTable();
    createRegionUsingGfsh(true, false, false);
    createMapping(REGION_NAME, CONNECTION_NAME);

    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("name", "Emp1").writeInt("age", 55).create();
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(() -> region.put("key1", pdxEmployee1))
          .isExactlyInstanceOf(JdbcConnectorException.class).hasMessage(
              "JDBC connection with name TestConnection not found. Create the connection with the gfsh command 'create jdbc-connection'");
    });
  }

  @Test
  public void putWritesToDB() throws Exception {
    createTable();
    createRegionUsingGfsh(true, false, false);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME);
    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "key1").writeString("name", "Emp1").writeInt("age", 55).create();

      String key = "emp1";
      ClusterStartupRule.getCache().getRegion(REGION_NAME).put(key, pdxEmployee1);
      assertTableHasEmployeeData(1, pdxEmployee1, key);
    });
  }

  @Test
  public void putAsyncWritesToDB() throws Exception {
    createTable();
    createRegionUsingGfsh(true, false, false);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME);
    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "key1").writeString("name", "Emp1").writeInt("age", 55).create();

      String key = "emp1";
      ClusterStartupRule.getCache().getRegion(REGION_NAME).put(key, pdxEmployee1);
      assertTableHasEmployeeData(1, pdxEmployee1, key);
    });
  }

  @Test
  public void getReadsFromEmptyDB() throws Exception {
    createTable();
    createRegionUsingGfsh(false, false, true);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME);
    server.invoke(() -> {
      String key = "emp1";
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      Object value = region.get(key);
      assertThat(value).isNull();
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void getReadsFromDB() throws Exception {
    createTable();
    createRegionUsingGfsh(true, false, true);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME);
    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("id", "id1").writeString("name", "Emp1").writeInt("age", 55).create();

      String key = "id1";
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.put(key, pdxEmployee1);
      region.invalidate(key);

      PdxInstance result = (PdxInstance) region.get(key);
      assertThat(result.getFieldNames()).hasSize(3);
      assertThat(result.getField("id")).isEqualTo(key);
      assertThat(result.getField("name")).isEqualTo("Emp1");
      assertThat(result.getField("age")).isEqualTo(55);
    });
  }

  @Test
  public void getReadsFromDBWithPdxClassName() throws Exception {
    createTable();
    createRegionUsingGfsh(true, false, true);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME, Employee.class.getName(), false);
    server.invoke(() -> {
      String key = "id1";
      Employee value = new Employee("Emp1", 55);
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
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

    createRegionUsingGfsh(true, false, true);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME, ClassWithSupportedPdxFields.class.getName(), false);
    client.invoke(() -> {
      String key = "id1";
      ClassWithSupportedPdxFields value = new ClassWithSupportedPdxFields(true, (byte) 1, (short) 2,
          3, 4, 5.5f, 6.0, "BigEmp", new Date(100000), "BigEmpObject", new byte[] {1, 2}, 'c');
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

    createRegionUsingGfsh(true, false, true);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME, ClassWithSupportedPdxFields.class.getName(), false);
    client.invoke(() -> {
      String key = "id1";
      ClassWithSupportedPdxFields value = new ClassWithSupportedPdxFields();
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
    createRegionUsingGfsh(true, false, true);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME, ClassWithSupportedPdxFields.class.getName(), false);
    String key = "id1";
    ClassWithSupportedPdxFields value = new ClassWithSupportedPdxFields(true, (byte) 1, (short) 2,
        3, 4, 5.5f, 6.0, "BigEmp", new Date(100000), "BigEmpObject", new byte[] {1, 2}, 'c');

    server.invoke(() -> {
      insertDataForAllSupportedFieldsTable(key, value);
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
    createRegionUsingGfsh(true, false, true);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME, ClassWithSupportedPdxFields.class.getName(), false);
    String key = "id1";
    ClassWithSupportedPdxFields value = new ClassWithSupportedPdxFields();

    server.invoke(() -> {
      insertNullDataForAllSupportedFieldsTable(key);
    });

    client.invoke(() -> {
      ClusterStartupRule.getClientCache().registerPdxMetaData(new ClassWithSupportedPdxFields());

      Region<String, ClassWithSupportedPdxFields> region =
          ClusterStartupRule.getClientCache().getRegion(REGION_NAME);

      ClassWithSupportedPdxFields result = region.get(key);
      assertThat(result).isEqualTo(value);
    });
  }

  private ClientVM getClientVM() throws Exception {
    Consumer<ClientCacheFactory> cacheSetup = (Serializable & Consumer<ClientCacheFactory>) cf -> {
      System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true");
      cf.addPoolLocator("localhost", locator.getPort());
      cf.setPdxSerializer(
          new ReflectionBasedAutoSerializer(ClassWithSupportedPdxFields.class.getName()));
    };
    return startupRule.startClientVM(2, new Properties(), cacheSetup);
  }

  private void createClientRegion(ClientVM client) {
    client.invoke(() -> {
      ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);
    });
  }

  private void createJdbcConnection() {
    final String commandStr =
        "create jdbc-connection --name=" + CONNECTION_NAME + " --url=" + CONNECTION_URL;
    gfsh.executeAndAssertThat(commandStr).statusIsSuccess();
  }

  private void createAsyncListener(String id) {
    final String commandStr =
        "create async-event-queue --id=" + id + " --listener=" + JdbcAsyncWriter.class.getName()
            + " --batch-size=1 --batch-time-interval=0 --parallel=false";
    gfsh.executeAndAssertThat(commandStr).statusIsSuccess();
  }

  private void createRegionUsingGfsh(boolean withCacheWriter, boolean withAsyncWriter,
      boolean withLoader) {
    StringBuffer createRegionCmd = new StringBuffer();
    createRegionCmd.append("create region --name=" + REGION_NAME + " --type=REPLICATE ");
    if (withCacheWriter) {
      createRegionCmd.append(" --cache-writer=" + JdbcWriter.class.getName());
    }
    if (withLoader) {
      createRegionCmd.append(" --cache-loader=" + JdbcLoader.class.getName());
    }
    if (withAsyncWriter) {
      createAsyncListener("JAW");
      createRegionCmd.append(" --async-event-queue-id=JAW");
    }

    gfsh.executeAndAssertThat(createRegionCmd.toString()).statusIsSuccess();
  }

  private void createMapping(String regionName, String connectionName) {
    createMapping(regionName, connectionName, null);
  }

  private void createMapping(String regionName, String connectionName, String pdxClassName) {
    createMapping(regionName, connectionName, pdxClassName, true);
  }

  private void createMapping(String regionName, String connectionName, String pdxClassName,
      boolean valueContainsPrimaryKey) {
    final String commandStr = "create jdbc-mapping --region=" + regionName + " --connection="
        + connectionName + (valueContainsPrimaryKey ? " --value-contains-primary-key" : "")
        + (pdxClassName != null ? " --pdx-class-name=" + pdxClassName : "");
    gfsh.executeAndAssertThat(commandStr).statusIsSuccess();
  }

  private void assertTableHasEmployeeData(int size, PdxInstance employee, String key)
      throws SQLException {
    Connection connection = DriverManager.getConnection(CONNECTION_URL);
    Statement statement = connection.createStatement();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
      assertThat(getRowCount(statement, TABLE_NAME)).isEqualTo(size);
    });

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_NAME + " order by id asc");
    assertThat(resultSet.next()).isTrue();
    assertThat(resultSet.getString("id")).isEqualTo(key);
    assertThat(resultSet.getString("name")).isEqualTo(employee.getField("name"));
    assertThat(resultSet.getObject("age")).isEqualTo(employee.getField("age"));
  }

  private int getRowCount(Statement stmt, String tableName) {
    try {
      ResultSet resultSet = stmt.executeQuery("select count(*) from " + tableName);
      resultSet.next();
      return resultSet.getInt(1);
    } catch (SQLException e) {
      return -1;
    }
  }

}
