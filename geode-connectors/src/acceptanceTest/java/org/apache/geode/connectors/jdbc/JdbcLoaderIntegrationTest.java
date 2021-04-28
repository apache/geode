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

import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TestConfigService;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.AutoSerializableManager;

public abstract class JdbcLoaderIntegrationTest {

  protected static final String SCHEMA_NAME = "mySchema";
  protected static final String REGION_TABLE_NAME = "employees";

  protected InternalCache cache;
  protected Connection connection;
  protected Statement statement;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private final TestDataSourceFactory testDataSourceFactory =
      new TestDataSourceFactory(getConnectionUrlSupplier());

  @Before
  public void setUp() throws Exception {
    System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true");
    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .setPdxReadSerialized(false)
        .setPdxSerializer(
            new ReflectionBasedAutoSerializer(ClassWithSupportedPdxFields.class.getName()))
        .create();
    connection = getConnection();
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    closeDB();
  }

  public abstract Connection getConnection() throws SQLException;

  public abstract Supplier<String> getConnectionUrlSupplier();

  protected abstract void createClassWithSupportedPdxFieldsTable(Statement statement,
      String tableName) throws SQLException;

  protected abstract List<FieldMapping> getSupportedPdxFieldsTableFieldMappings();

  private void createEmployeeTable() throws Exception {
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  protected List<FieldMapping> getEmployeeTableFieldMappings() {
    List<FieldMapping> fieldMappings = Arrays.asList(
        new FieldMapping("id", FieldType.STRING.name(), "id", JDBCType.VARCHAR.name(), false),
        new FieldMapping("name", FieldType.STRING.name(), "name", JDBCType.VARCHAR.name(), true),
        new FieldMapping("age", FieldType.INT.name(), "age", JDBCType.INTEGER.name(), true));
    return fieldMappings;
  }

  private void createEmployeeTableWithSchema() throws Exception {
    statement.execute("CREATE SCHEMA " + SCHEMA_NAME);
    statement.execute("Create Table " + SCHEMA_NAME + '.' + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  private void closeDB() throws Exception {
    if (statement == null) {
      statement = connection.createStatement();
    }
    statement.execute("Drop table IF EXISTS " + REGION_TABLE_NAME);
    statement.execute("Drop table IF EXISTS " + SCHEMA_NAME + "." + REGION_TABLE_NAME);
    statement.execute("Drop schema IF EXISTS " + SCHEMA_NAME);
    statement.close();

    if (connection != null) {
      connection.close();
    }
    testDataSourceFactory.close();
  }

  @Test
  public void verifyGetWithPdxClassName() throws Exception {
    createEmployeeTable();
    statement
        .execute("Insert into " + REGION_TABLE_NAME + "(id, name, age) values('1', 'Emp1', 21)");
    Region<String, Employee> region =
        createRegionWithJDBCLoader(REGION_TABLE_NAME, Employee.class.getName(),
            getEmployeeTableFieldMappings());
    createPdxType();

    Employee value = region.get("1");

    assertThat(value.getName()).isEqualTo("Emp1");
    assertThat(value.getAge()).isEqualTo(21);
  }

  @Test
  public void verifyGetWithPdxClassNameAndCompositeKey() throws Exception {
    createEmployeeTable();
    statement
        .execute("Insert into " + REGION_TABLE_NAME + "(id, name, age) values('1', 'Emp1', 21)");
    String ids = "id,name";
    Region<String, Employee> region =
        createRegionWithJDBCLoader(REGION_TABLE_NAME, Employee.class.getName(), ids, null, null,
            getEmployeeTableFieldMappings());
    createPdxType();

    PdxInstance key =
        cache.createPdxInstanceFactory("MyPdxKeyType").neverDeserialize().writeString("id", "1")
            .writeString("name", "Emp1").create();
    Employee value = region.get(key);

    assertThat(value.getId()).isEqualTo("1");
    assertThat(value.getName()).isEqualTo("Emp1");
    assertThat(value.getAge()).isEqualTo(21);
  }

  @Test
  public void verifyGetWithSchemaAndPdxClassNameAndCompositeKey() throws Exception {
    createEmployeeTableWithSchema();
    statement
        .execute("Insert into " + SCHEMA_NAME + '.' + REGION_TABLE_NAME
            + "(id, name, age) values('1', 'Emp1', 21)");
    String ids = "id,name";
    String catalog;
    String schema;
    if (vendorSupportsSchemas()) {
      catalog = null;
      schema = SCHEMA_NAME;
    } else {
      catalog = SCHEMA_NAME;
      schema = null;
    }
    Region<String, Employee> region =
        createRegionWithJDBCLoader(REGION_TABLE_NAME, Employee.class.getName(), ids, catalog,
            schema, getEmployeeTableFieldMappings());
    createPdxType();

    PdxInstance key =
        cache.createPdxInstanceFactory("MyPdxKeyType").neverDeserialize().writeString("id", "1")
            .writeString("name", "Emp1").create();
    Employee value = region.get(key);

    assertThat(value.getId()).isEqualTo("1");
    assertThat(value.getName()).isEqualTo("Emp1");
    assertThat(value.getAge()).isEqualTo(21);
  }

  protected abstract boolean vendorSupportsSchemas();

  @Test
  public void verifyGetWithSupportedFieldsWithPdxClassName() throws Exception {
    createClassWithSupportedPdxFieldsTable(statement, REGION_TABLE_NAME);
    ClassWithSupportedPdxFields classWithSupportedPdxFields =
        createClassWithSupportedPdxFieldsForInsert("1");
    insertIntoClassWithSupportedPdxFieldsTable("1", classWithSupportedPdxFields);
    Region<String, ClassWithSupportedPdxFields> region = createRegionWithJDBCLoader(
        REGION_TABLE_NAME, ClassWithSupportedPdxFields.class.getName(),
        getSupportedPdxFieldsTableFieldMappings());

    createPdxType(classWithSupportedPdxFields);

    ClassWithSupportedPdxFields value = region.get("1");
    assertThat(value).isEqualTo(classWithSupportedPdxFields);
  }

  protected void createPdxType() throws IOException {
    createPdxType(new Employee("id", "name", 45));
  }

  protected void createPdxType(Object value) throws IOException {
    // the following serialization will add a pdxType
    BlobHelper.serializeToBlob(value);
  }

  @Test
  public void verifySimpleMiss() throws Exception {
    createEmployeeTable();
    Region<String, PdxInstance> region =
        createRegionWithJDBCLoader(REGION_TABLE_NAME, Employee.class.getName(),
            getEmployeeTableFieldMappings());
    PdxInstance pdx = region.get("1");
    assertThat(pdx).isNull();
  }

  protected SqlHandler createSqlHandler(String regionName, String pdxClassName, String ids,
      String catalog,
      String schema, List<FieldMapping> fieldMappings)
      throws RegionMappingExistsException {
    return new SqlHandler(cache, regionName, new TableMetaDataManager(),
        TestConfigService.getTestConfigService((InternalCache) cache, pdxClassName, ids, catalog,
            schema, fieldMappings),
        testDataSourceFactory);
  }

  protected <K, V> Region<K, V> createRegionWithJDBCLoader(String regionName, String pdxClassName,
      String ids, String catalog, String schema, List<FieldMapping> fieldMappings)
      throws RegionMappingExistsException {
    JdbcLoader<K, V> jdbcLoader =
        new JdbcLoader<>(
            createSqlHandler(regionName, pdxClassName, ids, catalog, schema, fieldMappings),
            cache);
    RegionFactory<K, V> regionFactory = cache.createRegionFactory(REPLICATE);
    regionFactory.setCacheLoader(jdbcLoader);
    Region<K, V> region = regionFactory.create(regionName);
    return region;
  }

  protected <K, V> Region<K, V> createRegionWithJDBCLoader(String regionName, String pdxClassName,
      List<FieldMapping> fieldMappings)
      throws RegionMappingExistsException {
    return createRegionWithJDBCLoader(regionName, pdxClassName, "id", null, null, fieldMappings);
  }

  protected ClassWithSupportedPdxFields createClassWithSupportedPdxFieldsForInsert(String key) {
    ClassWithSupportedPdxFields classWithSupportedPdxFields =
        new ClassWithSupportedPdxFields(key, true, (byte) 1, (short) 2, 3, 4, 5.5f, 6.0, "BigEmp",
            new Date(0), "BigEmpObject", new byte[] {1, 2}, 'c');

    return classWithSupportedPdxFields;
  }

  protected void insertIntoClassWithSupportedPdxFieldsTable(String id,
      ClassWithSupportedPdxFields classWithSupportedPdxFields) throws Exception {
    String insertString =
        "Insert into " + REGION_TABLE_NAME + " values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
    PreparedStatement ps = connection.prepareStatement(insertString);
    int i = 1;
    ps.setObject(i++, id);
    ps.setObject(i++, classWithSupportedPdxFields.isAboolean());
    ps.setObject(i++, classWithSupportedPdxFields.getAbyte());
    ps.setObject(i++, classWithSupportedPdxFields.getAshort());
    ps.setObject(i++, classWithSupportedPdxFields.getAnint());
    ps.setObject(i++, classWithSupportedPdxFields.getAlong());
    ps.setObject(i++, classWithSupportedPdxFields.getAfloat());
    ps.setObject(i++, classWithSupportedPdxFields.getAdouble());
    ps.setObject(i++, classWithSupportedPdxFields.getAstring());
    ps.setObject(i++, new java.sql.Timestamp(classWithSupportedPdxFields.getAdate().getTime()));
    ps.setObject(i++, classWithSupportedPdxFields.getAnobject());
    ps.setObject(i++, classWithSupportedPdxFields.getAbytearray());
    ps.setObject(i++, new Character(classWithSupportedPdxFields.getAchar()).toString());
    ps.executeUpdate();
  }
}
