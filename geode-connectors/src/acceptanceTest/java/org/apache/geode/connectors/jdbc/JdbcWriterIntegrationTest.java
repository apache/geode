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
import static org.assertj.core.api.Assertions.catchThrowable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TestConfigService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;

public abstract class JdbcWriterIntegrationTest {

  static final String DB_NAME = "test";
  private static final String REGION_TABLE_NAME = "employees";

  private InternalCache cache;
  private Region<String, PdxInstance> employees;
  private Connection connection;
  private Statement statement;
  private JdbcWriter jdbcWriter;
  private PdxInstance pdx1;
  private PdxInstance pdx2;
  private Employee employee1;
  private Employee employee2;
  private final TestDataSourceFactory testDataSourceFactory =
      new TestDataSourceFactory(getConnectionUrl());

  @Before
  public void setUp() throws Exception {
    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .setPdxReadSerialized(false).create();
    employees = createRegionWithJDBCSynchronousWriter(REGION_TABLE_NAME);

    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
    pdx1 = cache.createPdxInstanceFactory(Employee.class.getName()).writeString("name", "Emp1")
        .writeInt("age", 55).create();
    pdx2 = cache.createPdxInstanceFactory(Employee.class.getName()).writeString("name", "Emp2")
        .writeInt("age", 21).create();
    employee1 = (Employee) pdx1.getObject();
    employee2 = (Employee) pdx2.getObject();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    closeDB();
  }

  public abstract Connection getConnection() throws SQLException;

  public abstract String getConnectionUrl();

  private void closeDB() throws Exception {
    if (statement == null) {
      if (connection != null) {
        statement = connection.createStatement();
      }
    }
    if (statement != null) {
      statement.execute("Drop table " + REGION_TABLE_NAME);
      statement.close();
    }
    if (connection != null) {
      connection.close();
    }
    testDataSourceFactory.close();
  }

  @Test
  public void canInsertIntoTable() throws Exception {
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canPutAllInsertIntoTable() throws Exception {
    Map<String, PdxInstance> putAllMap = new HashMap<>();
    putAllMap.put("1", pdx1);
    putAllMap.put("2", pdx2);
    employees.putAll(putAllMap);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void verifyThatPdxFieldNamedSameAsPrimaryKeyIsIgnored() throws Exception {
    PdxInstance pdxInstanceWithId = cache.createPdxInstanceFactory(Employee.class.getName())
        .writeString("name", "Emp1").writeInt("age", 55).writeString("id", "3").create();
    employees.put("1", pdxInstanceWithId);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", (Employee) pdxInstanceWithId.getObject());
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void putNonPdxInstanceFails() {
    Region nonPdxEmployees = this.employees;
    Throwable thrown = catchThrowable(() -> nonPdxEmployees.put("1", "non pdx instance"));
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void putNonPdxInstanceThatIsPdxSerializable() throws SQLException {
    Region nonPdxEmployees = this.employees;
    Employee value = new Employee("2", "Emp2", 22);
    nonPdxEmployees.put("2", value);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", value);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canDestroyFromTable() throws Exception {
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    employees.destroy("1");

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateTable() throws Exception {
    employees.put("1", pdx1);
    employees.put("1", pdx2);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateBecomeInsert() throws Exception {
    employees.put("1", pdx1);

    statement.execute("delete from " + REGION_TABLE_NAME + " where id = '1'");
    validateTableRowCount(0);

    employees.put("1", pdx2);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canInsertBecomeUpdate() throws Exception {
    statement.execute("Insert into " + REGION_TABLE_NAME + " values('1', 'bogus', 11)");
    validateTableRowCount(1);

    employees.put("1", pdx1);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertThat(resultSet.next()).isFalse();
  }

  private Region<String, PdxInstance> createRegionWithJDBCSynchronousWriter(String regionName)
      throws RegionMappingExistsException {
    jdbcWriter = new JdbcWriter(createSqlHandler(), cache);

    RegionFactory<String, PdxInstance> regionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.setCacheWriter(jdbcWriter);
    return regionFactory.create(regionName);
  }

  private void validateTableRowCount(int expected) throws Exception {
    ResultSet resultSet = statement.executeQuery("select count(*) from " + REGION_TABLE_NAME);
    resultSet.next();
    int size = resultSet.getInt(1);
    assertThat(size).isEqualTo(expected);
  }

  private SqlHandler createSqlHandler()
      throws RegionMappingExistsException {
    return new SqlHandler(new TableMetaDataManager(),
        TestConfigService.getTestConfigService(getConnectionUrl()),
        testDataSourceFactory);
  }

  private void assertRecordMatchesEmployee(ResultSet resultSet, String key, Employee employee)
      throws SQLException {
    assertThat(resultSet.next()).isTrue();
    assertThat(resultSet.getString("id")).isEqualTo(key);
    assertThat(resultSet.getString("name")).isEqualTo(employee.getName());
    assertThat(resultSet.getInt("age")).isEqualTo(employee.getAge());
  }
}
