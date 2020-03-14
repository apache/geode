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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.awaitility.core.ThrowingRunnable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TestConfigService;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.WritablePdxInstance;

public abstract class JdbcAsyncWriterIntegrationTest {

  static final String DB_NAME = "test";
  private static final String REGION_TABLE_NAME = "employees";

  private InternalCache cache;
  private Region<Object, Object> employees;
  private Connection connection;
  private Statement statement;
  private JdbcAsyncWriter jdbcWriter;
  private PdxInstance pdxEmployee1;
  private PdxInstance pdxEmployee2;
  private Employee employee1;
  private Employee employee2;
  private final TestDataSourceFactory testDataSourceFactory =
      new TestDataSourceFactory(getConnectionUrl());

  @Before
  public void setup() throws Exception {
    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .setPdxReadSerialized(false).create();
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
    pdxEmployee1 = cache.createPdxInstanceFactory(Employee.class.getName())
        .writeString("id", "1").writeString("name", "Emp1").writeInt("age", 55).create();
    pdxEmployee2 = cache.createPdxInstanceFactory(Employee.class.getName())
        .writeString("id", "2").writeString("name", "Emp2").writeInt("age", 21).create();
    employee1 = (Employee) pdxEmployee1.getObject();
    employee2 = (Employee) pdxEmployee2.getObject();
  }

  private void setupRegion(String ids) throws RegionMappingExistsException {
    List<FieldMapping> fieldMappings = Arrays.asList(
        new FieldMapping("id", FieldType.STRING.name(), "id", JDBCType.VARCHAR.name(), false),
        new FieldMapping("name", FieldType.STRING.name(), "name", JDBCType.VARCHAR.name(), true),
        new FieldMapping("age", FieldType.OBJECT.name(), "age", JDBCType.INTEGER.name(), true));
    employees = createRegionWithJDBCAsyncWriter(REGION_TABLE_NAME, ids, fieldMappings);
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    closeDB();
  }

  private void closeDB() throws Exception {
    if (statement == null && connection != null) {
      statement = connection.createStatement();
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

  public abstract Connection getConnection() throws SQLException;

  public abstract String getConnectionUrl();

  @Test
  public void validateJDBCAsyncWriterTotalEvents() throws RegionMappingExistsException {
    setupRegion("id");
    employees.put("1", pdxEmployee1);
    employees.put("2", pdxEmployee2);

    awaitUntil(() -> assertThat(jdbcWriter.getTotalEvents()).isEqualTo(2));
  }

  @Test
  public void verifyThatPdxFieldNamedSameAsPrimaryKeyIsIgnored() throws Exception {
    setupRegion("id");
    PdxInstance pdx1 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp1")
        .writeObject("age", 55).writeInt("id", 3).create();
    employees.put("1", pdx1);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(1));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void putNonPdxInstanceFails() throws RegionMappingExistsException {
    setupRegion("id");
    employees.put("1", "non pdx instance");

    awaitUntil(() -> assertThat(jdbcWriter.getTotalEvents()).isEqualTo(1));

    awaitUntil(() -> assertThat(jdbcWriter.getFailedEvents()).isEqualTo(1));
  }

  @Test
  public void putNonPdxInstanceThatIsPdxSerializable()
      throws SQLException, RegionMappingExistsException {
    setupRegion("id");
    Employee value = new Employee("2", "Emp2", 22);
    employees.put("2", value);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(1));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", value);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canDestroyFromTable() throws Exception {
    setupRegion("id");
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
  public void canDestroyFromTableWithCompositeKey() throws Exception {
    setupRegion("id,age");
    PdxInstance compositeKey1 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) pdxEmployee1.getField("id"), String.class)
        .writeField("age", (Integer) pdxEmployee1.getField("age"), int.class).create();
    PdxInstance compositeKey2 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) pdxEmployee2.getField("id"), String.class)
        .writeField("age", (Integer) pdxEmployee2.getField("age"), int.class).create();
    employees.put(compositeKey1, pdxEmployee1);
    employees.put(compositeKey2, pdxEmployee2);
    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    employees.destroy(compositeKey1);
    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(3));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canInsertIntoTable() throws Exception {
    setupRegion("id");
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
  public void canInsertIntoTableWithCompositeKey() throws Exception {
    setupRegion("id,age");
    PdxInstance compositeKey1 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) pdxEmployee1.getField("id"), String.class)
        .writeField("age", (Integer) pdxEmployee1.getField("age"), int.class).create();
    PdxInstance compositeKey2 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) pdxEmployee2.getField("id"), String.class)
        .writeField("age", (Integer) pdxEmployee2.getField("age"), int.class).create();

    employees.put(compositeKey1, pdxEmployee1);
    employees.put(compositeKey2, pdxEmployee2);
    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateTable() throws Exception {
    setupRegion("id");
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
  public void canUpdateTableWithCompositeKey() throws Exception {
    setupRegion("id,age");
    PdxInstance myPdx = cache.createPdxInstanceFactory(Employee.class.getName())
        .writeString("id", "1").writeString("name", "Emp1")
        .writeInt("age", 55).create();
    PdxInstance compositeKey1 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) myPdx.getField("id"), String.class)
        .writeField("age", (Integer) myPdx.getField("age"), int.class).create();
    employees.put(compositeKey1, myPdx);
    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(1));
    WritablePdxInstance updatedPdx = myPdx.createWriter();
    updatedPdx.setField("name", "updated");
    Employee updatedEmployee = (Employee) updatedPdx.getObject();

    employees.put(compositeKey1, updatedPdx);
    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", updatedEmployee);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateBecomeInsert() throws Exception {
    setupRegion("id");
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
    setupRegion("id");
    statement.execute("Insert into " + REGION_TABLE_NAME + " values('1', 'bogus', 11)");
    validateTableRowCount(1);

    employees.put("1", pdxEmployee1);

    awaitUntil(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(1));

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertThat(resultSet.next()).isFalse();
  }

  private void awaitUntil(final ThrowingRunnable supplier) {
    await().untilAsserted(supplier);
  }

  private void assertRecordMatchesEmployee(ResultSet resultSet, String key, Employee employee)
      throws SQLException {
    assertThat(resultSet.next()).isTrue();
    assertThat(resultSet.getString("id")).isEqualTo(key);
    assertThat(resultSet.getString("name")).isEqualTo(employee.getName());
    assertThat(resultSet.getObject("age")).isEqualTo(employee.getAge());
  }

  private Region<Object, Object> createRegionWithJDBCAsyncWriter(String regionName, String ids,
      List<FieldMapping> fieldMappings)
      throws RegionMappingExistsException {
    jdbcWriter = new JdbcAsyncWriter(createSqlHandler(regionName, ids, fieldMappings), cache);
    cache.createAsyncEventQueueFactory().setBatchSize(1).setBatchTimeInterval(1)
        .create("jdbcAsyncQueue", jdbcWriter);
    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(REPLICATE);
    regionFactory.addAsyncEventQueueId("jdbcAsyncQueue");
    return regionFactory.create(regionName);
  }

  private void validateTableRowCount(int expected) throws Exception {
    ResultSet resultSet = statement.executeQuery("select count(*) from " + REGION_TABLE_NAME);
    resultSet.next();
    int size = resultSet.getInt(1);
    assertThat(size).isEqualTo(expected);
  }

  private SqlHandler createSqlHandler(String regionName, String ids,
      List<FieldMapping> fieldMappings)
      throws RegionMappingExistsException {
    return new SqlHandler(cache, regionName, new TableMetaDataManager(),
        TestConfigService.getTestConfigService(ids, fieldMappings),
        testDataSourceFactory);
  }

}
