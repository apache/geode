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
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

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
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.WritablePdxInstance;

public abstract class JdbcWriterIntegrationTest {

  static final String DB_NAME = "test";
  protected static final String SCHEMA_NAME = "mySchema";
  protected static final String REGION_TABLE_NAME = "employees";

  protected InternalCache cache;
  protected Region<Object, Object> employees;
  protected Connection connection;
  protected Statement statement;
  protected JdbcWriter<Object, Object> jdbcWriter;
  protected PdxInstance pdx1;
  protected PdxInstance pdx2;
  protected Employee employee1;
  protected Employee employee2;
  protected final TestDataSourceFactory testDataSourceFactory =
      new TestDataSourceFactory(getConnectionUrl());
  protected String catalog;
  protected String schema;

  @Before
  public void setUp() throws Exception {
    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .setPdxReadSerialized(false).create();

    connection = getConnection();
    statement = connection.createStatement();
    pdx1 = cache.createPdxInstanceFactory(Employee.class.getName()).writeString("id", "1")
        .writeString("name", "Emp1")
        .writeInt("age", 55).create();
    pdx2 = cache.createPdxInstanceFactory(Employee.class.getName()).writeString("id", "2")
        .writeString("name", "Emp2")
        .writeInt("age", 21).create();
    employee1 = (Employee) pdx1.getObject();
    employee2 = (Employee) pdx2.getObject();
    createTableInUnusedSchema();
  }

  protected void createTable() throws SQLException {
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  protected void createTableWithSchema() throws SQLException {
    statement.execute("Create Schema " + SCHEMA_NAME);
    statement.execute("Create Table " + SCHEMA_NAME + '.' + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  protected void createTableInUnusedSchema() throws SQLException {
    Connection connection2 = getConnection();
    statement.execute("Create Schema unusedSchema");
    statement = connection2.createStatement();
    statement.execute("Create Table " + "unusedSchema." + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  protected void setupRegion(String ids) throws RegionMappingExistsException {
    sharedRegionSetup(ids, null, null);
  }

  protected void sharedRegionSetup(String ids, String catalog, String schema)
      throws RegionMappingExistsException {
    List<FieldMapping> fieldMappings = Arrays.asList(
        new FieldMapping("id", FieldType.STRING.name(), "id", JDBCType.VARCHAR.name(), false),
        new FieldMapping("name", FieldType.STRING.name(), "name", JDBCType.VARCHAR.name(), true),
        new FieldMapping("age", FieldType.OBJECT.name(), "age", JDBCType.INTEGER.name(), true));
    employees = createRegionWithJDBCSynchronousWriter(REGION_TABLE_NAME, ids, catalog, schema,
        fieldMappings);
  }

  protected void setupRegionWithSchema(String ids) throws RegionMappingExistsException {
    if (vendorSupportsSchemas()) {
      catalog = null;
      schema = SCHEMA_NAME;
    } else {
      catalog = SCHEMA_NAME;
      schema = null;

    }
    sharedRegionSetup(ids, catalog, schema);
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
      statement.execute("Drop table IF EXISTS " + REGION_TABLE_NAME);
      statement.execute("Drop table IF EXISTS unusedSchema." + REGION_TABLE_NAME);
      statement.execute("Drop schema IF EXISTS unusedSchema");
      statement.execute("Drop table IF EXISTS " + SCHEMA_NAME + '.' + REGION_TABLE_NAME);
      statement.execute("Drop schema IF EXISTS " + SCHEMA_NAME);
      statement.close();
    }
    if (connection != null) {
      connection.close();
    }
    testDataSourceFactory.close();
  }

  @Test
  public void canInsertIntoTable() throws Exception {
    createTable();
    DataSource dataSource = testDataSourceFactory.getDataSource("testConnectionConfig");
    setupRegion("id");

    employees.put("1", pdx1);
    employees.put("2", pdx2);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();

    dataSource.getConnection();
  }

  protected abstract boolean vendorSupportsSchemas();

  @Test
  public void canInsertIntoTableWithSchema() throws Exception {
    createTableWithSchema();
    setupRegionWithSchema("id");
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    ResultSet resultSet =
        statement.executeQuery(
            "select * from " + SCHEMA_NAME + '.' + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canInsertIntoTableWithCompositeKey() throws Exception {
    createTable();
    setupRegion("id,age");
    PdxInstance compositeKey1 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) pdx1.getField("id"), String.class)
        .writeField("age", (Integer) pdx1.getField("age"), int.class).create();
    PdxInstance compositeKey2 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) pdx2.getField("id"), String.class)
        .writeField("age", (Integer) pdx2.getField("age"), int.class).create();

    employees.put(compositeKey1, pdx1);
    employees.put(compositeKey2, pdx2);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canPutAllInsertIntoTable() throws Exception {
    createTable();
    setupRegion("id");
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
    createTable();
    setupRegion("id");
    PdxInstance pdxInstanceWithId = cache.createPdxInstanceFactory(Employee.class.getName())
        .writeString("name", "Emp1").writeInt("age", 55).writeString("id", "3").create();
    employees.put("1", pdxInstanceWithId);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", (Employee) pdxInstanceWithId.getObject());
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void putNonPdxInstanceFails() throws Exception {
    createTable();
    setupRegion("id");
    Throwable thrown = catchThrowable(() -> employees.put("1", "non pdx instance"));
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void putNonPdxInstanceThatIsPdxSerializable()
      throws SQLException, RegionMappingExistsException {
    createTable();
    setupRegion("id");
    Employee value = new Employee("2", "Emp2", 22);
    employees.put("2", value);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", value);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canDestroyFromTable() throws Exception {
    createTable();
    setupRegion("id");
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    employees.destroy("1");

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canDestroyFromTableWithSchema() throws Exception {
    createTableWithSchema();
    setupRegionWithSchema("id");
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    employees.destroy("1");

    ResultSet resultSet =
        statement.executeQuery(
            "select * from " + SCHEMA_NAME + '.' + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canDestroyFromTableWithCompositeKey() throws Exception {
    createTable();
    setupRegion("id,age");
    PdxInstance compositeKey1 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) pdx1.getField("id"), String.class)
        .writeField("age", (Integer) pdx1.getField("age"), int.class).create();
    PdxInstance compositeKey2 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) pdx2.getField("id"), String.class)
        .writeField("age", (Integer) pdx2.getField("age"), int.class).create();
    employees.put(compositeKey1, pdx1);
    employees.put(compositeKey2, pdx2);

    employees.destroy(compositeKey1);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateTable() throws Exception {
    createTable();
    setupRegion("id");
    employees.put("1", pdx1);
    employees.put("1", pdx2);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateTableWithSchema() throws Exception {
    createTableWithSchema();
    setupRegionWithSchema("id");
    employees.put("1", pdx1);
    employees.put("1", pdx2);

    ResultSet resultSet =
        statement.executeQuery(
            "select * from " + SCHEMA_NAME + '.' + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateTableWithCompositeKey() throws Exception {
    createTable();
    setupRegion("id,age");
    PdxInstance myPdx = cache.createPdxInstanceFactory(Employee.class.getName())
        .writeString("id", "1").writeString("name", "Emp1")
        .writeInt("age", 55).create();
    PdxInstance compositeKey1 = cache.createPdxInstanceFactory("IdAgeKeyType").neverDeserialize()
        .writeField("id", (String) myPdx.getField("id"), String.class)
        .writeField("age", (Integer) myPdx.getField("age"), int.class).create();
    employees.put(compositeKey1, myPdx);
    WritablePdxInstance updatedPdx = myPdx.createWriter();
    updatedPdx.setField("name", "updated");
    Employee updatedEmployee = (Employee) updatedPdx.getObject();

    employees.put(compositeKey1, updatedPdx);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", updatedEmployee);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateBecomeInsert() throws Exception {
    createTable();
    setupRegion("id");
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
    createTable();
    setupRegion("id");
    statement.execute("Insert into " + REGION_TABLE_NAME + " values('1', 'bogus', 11)");
    validateTableRowCount(1);

    employees.put("1", pdx1);

    ResultSet resultSet =
        statement.executeQuery("select * from " + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertThat(resultSet.next()).isFalse();
  }

  protected Region<Object, Object> createRegionWithJDBCSynchronousWriter(String regionName,
      String ids, String catalog, String schema, List<FieldMapping> fieldMappings)
      throws RegionMappingExistsException {
    jdbcWriter =
        new JdbcWriter<>(createSqlHandler(regionName, ids, catalog, schema, fieldMappings), cache);

    RegionFactory<Object, Object> regionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.setCacheWriter(jdbcWriter);
    return regionFactory.create(regionName);
  }

  protected void validateTableRowCount(int expected) throws Exception {
    ResultSet resultSet = statement.executeQuery("select count(*) from " + REGION_TABLE_NAME);
    resultSet.next();
    int size = resultSet.getInt(1);
    assertThat(size).isEqualTo(expected);
  }

  protected SqlHandler createSqlHandler(String regionName, String ids, String catalog,
      String schema,
      List<FieldMapping> fieldMappings)
      throws RegionMappingExistsException {
    return new SqlHandler(cache, regionName, new TableMetaDataManager(),
        TestConfigService.getTestConfigService(cache, null, ids, catalog, schema, fieldMappings),
        testDataSourceFactory);
  }

  protected void assertRecordMatchesEmployee(ResultSet resultSet, String id, Employee employee)
      throws SQLException {
    assertThat(resultSet.next()).isTrue();
    assertThat(resultSet.getString("id")).isEqualTo(id);
    assertThat(resultSet.getString("name")).isEqualTo(employee.getName());
    assertThat(resultSet.getInt("age")).isEqualTo(employee.getAge());
  }
}
