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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * End-to-end dunits for jdbc connector
 *
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
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM server;

  @Before
  public void setup() throws Exception {
    MemberVM locator = startupRule.startLocatorVM(0);
    gfsh.connectAndVerify(locator);
    server = startupRule.startServerVM(1, locator.getPort());
    server.invoke(() -> createTable());
  }

  private void createTable() throws SQLException {
    Connection connection = DriverManager.getConnection(CONNECTION_URL);
    Statement statement = connection.createStatement();
    statement.execute("Create Table " + TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
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
    createRegion(true, false, false);
    createJdbcConnection();

    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("name", "Emp1").writeInt("age", 55).create();
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(() -> region.put("key1", pdxEmployee1))
          .isExactlyInstanceOf(IllegalStateException.class).hasMessage(
              "JDBC mapping for region employees not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    });
  }

  @Test
  public void throwsExceptionWhenNoMappingExistsUsingAsyncWriter() throws Exception {
    IgnoredException.addIgnoredException("IllegalStateException");
    createRegion(false, true, false);
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
    createRegion(true, false, false);
    createJdbcConnection();
    createMapping("NoSuchRegion", CONNECTION_NAME);

    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("name", "Emp1").writeInt("age", 55).create();
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(() -> region.put("key1", pdxEmployee1))
          .isExactlyInstanceOf(IllegalStateException.class).hasMessage(
              "JDBC mapping for region employees not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    });
  }

  @Test
  public void throwsExceptionWhenNoConnectionExists() throws Exception {
    createRegion(true, false, false);
    createMapping(REGION_NAME, CONNECTION_NAME);

    server.invoke(() -> {
      PdxInstance pdxEmployee1 =
          ClusterStartupRule.getCache().createPdxInstanceFactory(Employee.class.getName())
              .writeString("name", "Emp1").writeInt("age", 55).create();
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(() -> region.put("key1", pdxEmployee1))
          .isExactlyInstanceOf(IllegalStateException.class).hasMessage(
              "JDBC connection with name TestConnection not found. Create the connection with the gfsh command 'create jdbc-connection'");
    });
  }

  @Test
  public void putWritesToDB() throws Exception {
    createRegion(true, false, false);
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
    createRegion(true, false, false);
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
    createRegion(false, false, true);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME);
    server.invoke(() -> {
      String key = "emp1";
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      region.get(key);
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void getReadsFromDB() throws Exception {
    createRegion(true, false, true);
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
      assertThat(result.getFieldNames().size()).isEqualTo(3);
      assertThat(result.getField("id")).isEqualTo(key);
      assertThat(result.getField("name")).isEqualTo("Emp1");
      assertThat(result.getField("age")).isEqualTo(55);
    });
  }

  @Test
  public void getReadsFromDBWithPdxClassName() throws Exception {
    createRegion(true, false, true);
    createJdbcConnection();
    createMapping(REGION_NAME, CONNECTION_NAME, Employee.class.getName());
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

  private void createRegion(boolean withCacheWriter, boolean withAsyncWriter, boolean withLoader) {
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
    final String commandStr = "create jdbc-mapping --region=" + regionName + " --connection="
        + connectionName + " --value-contains-primary-key"
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
