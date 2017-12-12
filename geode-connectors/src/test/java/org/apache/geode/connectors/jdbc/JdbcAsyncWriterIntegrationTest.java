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

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.connectors.jdbc.internal.TestConfigService;
import org.apache.geode.connectors.jdbc.internal.TestableConnectionManager;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class JdbcAsyncWriterIntegrationTest {

  private static final String DB_NAME = "DerbyDB";
  private static final String REGION_TABLE_NAME = "employees";
  private static final String CONNECTION_URL = "jdbc:derby:memory:" + DB_NAME + ";create=true";

  private Cache cache;
  private Region<String, PdxInstance> employees;
  private Connection connection;
  private Statement statement;
  private JdbcAsyncWriter jdbcWriter;
  private PdxInstance pdxEmployee1;
  private PdxInstance pdxEmployee2;
  private Employee employee1;
  private Employee employee2;

  @Before
  public void setup() throws Exception {
    cache = new CacheFactory().setPdxReadSerialized(false).create();
    employees = createRegionWithJDBCAsyncWriter(REGION_TABLE_NAME);
    connection = DriverManager.getConnection(CONNECTION_URL);
    statement = connection.createStatement();
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
    pdxEmployee1 = cache.createPdxInstanceFactory(Employee.class.getName())
        .writeString("name", "Emp1").writeInt("age", 55).create();
    pdxEmployee2 = cache.createPdxInstanceFactory(Employee.class.getName())
        .writeString("name", "Emp2").writeInt("age", 21).create();
    employee1 = (Employee) pdxEmployee1.getObject();
    employee2 = (Employee) pdxEmployee2.getObject();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    closeDB();
  }

  private void closeDB() throws Exception {
    if (statement == null) {
      statement = connection.createStatement();
    }
    statement.execute("Drop table " + REGION_TABLE_NAME);
    statement.close();

    if (connection != null) {
      connection.close();
    }
  }

  @Test
  public void validateJDBCAsyncWriterTotalEvents() {
    employees.put("1", pdxEmployee1);
    employees.put("2", pdxEmployee2);

    awaitUntil(() -> assertThat(jdbcWriter.getTotalEvents()).isEqualTo(2));
  }

  @Test
  public void canInsertIntoTable() throws Exception {
    employees.put("1", pdxEmployee1);
    employees.put("2", pdxEmployee2);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void verifyThatPdxFieldNamedSameAsPrimaryKeyIsIgnored() throws Exception {
    PdxInstance pdx1 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp1")
        .writeInt("age", 55).writeInt("id", 3).create();
    employees.put("1", pdx1);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(1));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void putNonPdxInstanceFails() {
    Region nonPdxEmployees = this.employees;
    nonPdxEmployees.put("1", "non pdx instance");

    awaitUntil(() -> assertThat(jdbcWriter.getTotalEvents()).isEqualTo(1));

    assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(0);
  }

  @Test
  public void putNonPdxInstanceThatIsPdxSerializable() throws SQLException {
    Region nonPdxEmployees = this.employees;
    Employee value = new Employee("Emp2", 22);
    nonPdxEmployees.put("2", value);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(1));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", value);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canDestroyFromTable() throws Exception {
    employees.put("1", pdxEmployee1);
    employees.put("2", pdxEmployee2);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    employees.destroy("1");

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(3));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateTable() throws Exception {
    employees.put("1", pdxEmployee1);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(1));

    employees.put("1", pdxEmployee2);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateBecomeInsert() throws Exception {
    employees.put("1", pdxEmployee1);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(1));

    statement.execute("delete from " + REGION_TABLE_NAME + " where id = '1'");
    validateTableRowCount(0);

    employees.put("1", pdxEmployee2);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canInsertBecomeUpdate() throws Exception {
    statement.execute("Insert into " + REGION_TABLE_NAME + " values('1', 'bogus', 11)");
    validateTableRowCount(1);

    employees.put("1", pdxEmployee1);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(1));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertThat(resultSet.next()).isFalse();
  }

  private void awaitUntil(final Runnable supplier) {
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(supplier);
  }

  private void assertRecordMatchesEmployee(ResultSet resultSet, String key, Employee employee)
      throws SQLException {
    assertThat(resultSet.next()).isTrue();
    assertThat(resultSet.getString("id")).isEqualTo(key);
    assertThat(resultSet.getString("name")).isEqualTo(employee.getName());
    assertThat(resultSet.getObject("age")).isEqualTo(employee.getAge());
  }

  private Region<String, PdxInstance> createRegionWithJDBCAsyncWriter(String regionName)
      throws ConnectionConfigExistsException, RegionMappingExistsException {
    jdbcWriter = new JdbcAsyncWriter(createSqlHandler());
    cache.createAsyncEventQueueFactory().setBatchSize(1).setBatchTimeInterval(1)
        .create("jdbcAsyncQueue", jdbcWriter);

    RegionFactory<String, PdxInstance> regionFactory = cache.createRegionFactory(REPLICATE);
    regionFactory.addAsyncEventQueueId("jdbcAsyncQueue");
    return regionFactory.create(regionName);
  }

  private void validateTableRowCount(int expected) throws Exception {
    ResultSet resultSet = statement.executeQuery("select count(*) from " + REGION_TABLE_NAME);
    resultSet.next();
    int size = resultSet.getInt(1);
    assertThat(size).isEqualTo(expected);
  }

  private SqlHandler createSqlHandler()
      throws ConnectionConfigExistsException, RegionMappingExistsException {
    return new SqlHandler(new TestableConnectionManager(TestConfigService.getTestConfigService()));
  }

}
