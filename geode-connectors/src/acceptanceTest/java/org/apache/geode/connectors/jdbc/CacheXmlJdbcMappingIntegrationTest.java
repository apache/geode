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

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.net.URL;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.connectors.jdbc.test.junit.rules.DatabaseConnectionRule;
import org.apache.geode.connectors.jdbc.test.junit.rules.MySqlConnectionRule;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.internal.AutoSerializableManager;

public class CacheXmlJdbcMappingIntegrationTest {

  private static final URL COMPOSE_RESOURCE_PATH =
      CacheXmlJdbcMappingIntegrationTest.class.getResource("/mysql.yml");

  private static final String DATA_SOURCE_NAME = "TestDataSource";
  private static final String DB_NAME = "test";
  private static final String REGION_TABLE_NAME = "employees";
  private static final String REGION_NAME = "Region1";

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private Connection connection;
  private Statement statement;
  private InternalCache cache;

  @ClassRule
  public static DatabaseConnectionRule dbRule = new MySqlConnectionRule.Builder()
      .file(COMPOSE_RESOURCE_PATH.getPath()).build();

  @Before
  public void setUp() throws Exception {
    System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true");
    connection = dbRule.getConnection();
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    JNDIInvoker.unMapDatasource(DATA_SOURCE_NAME);

    if (cache != null) {
      cache.close();
    }

    if (statement == null) {
      statement = connection.createStatement();
    }
    statement.execute("Drop table IF EXISTS " + REGION_TABLE_NAME);
    statement.close();

    if (connection != null) {
      connection.close();
    }
  }

  private InternalCache createCacheAndCreateJdbcMapping(String cacheXmlTestName) {
    String url = dbRule.getConnectionUrl().replaceAll("&", "&amp;");
    System.setProperty("TestDataSourceUrl", url);
    InternalCache cache =
        (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
            .set("cache-xml-file", getXmlFileForTest(cacheXmlTestName))
            .create();
    return cache;
  }

  private InternalCache createCacheAndCreateJdbcMappingWithWrongDataSource(
      String cacheXmlTestName) {
    System.setProperty("TestDataSourceUrl", "jdbc:mysql://localhost/test");
    InternalCache cache =
        (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
            .set("cache-xml-file", getXmlFileForTest(cacheXmlTestName))
            .create();
    return cache;
  }

  private String getXmlFileForTest(String testName) {
    return createTempFileFromResource(getClass(),
        getClassSimpleName() + "." + testName + ".cache.xml").getAbsolutePath();
  }

  private String getClassSimpleName() {
    return getClass().getSimpleName();
  }

  private void createEmployeeTable() throws Exception {
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  private void createEmployeeTableWithColumnNamesWithUnderscores() throws Exception {
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, _name varchar(10), _age int)");
  }

  private List<FieldMapping> getEmployeeTableFieldMappings() {
    List<FieldMapping> fieldMappings = Arrays.asList(
        new FieldMapping("id", FieldType.STRING.name(), "id", JDBCType.VARCHAR.name(), false),
        new FieldMapping("name", FieldType.STRING.name(), "name", JDBCType.VARCHAR.name(), true),
        new FieldMapping("age", FieldType.INT.name(), "age", JDBCType.INTEGER.name(), true));
    return fieldMappings;
  }

  private List<FieldMapping> getEmployeeTableColumnNameWithUnderscoresFieldMappings() {
    List<FieldMapping> fieldMappings = Arrays.asList(
        new FieldMapping("id", FieldType.STRING.name(), "id", JDBCType.VARCHAR.name(), false),
        new FieldMapping("name", FieldType.STRING.name(), "_name", JDBCType.VARCHAR.name(), true),
        new FieldMapping("age", FieldType.INT.name(), "_age", JDBCType.INTEGER.name(), true));
    return fieldMappings;
  }

  @Test
  public void mappingSuccessWhenFieldMappingsAreExists() throws Exception {
    createEmployeeTable();

    cache = createCacheAndCreateJdbcMapping("FieldMappings");
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);

    RegionMapping mapping = service.getMappingForRegion(REGION_NAME);
    assertThat(mapping.getDataSourceName()).isEqualTo(DATA_SOURCE_NAME);
    assertThat(mapping.getTableName()).isEqualTo(REGION_TABLE_NAME);
    assertThat(mapping.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(mapping.getPdxName()).isEqualTo(Employee.class.getName());
    assertThat(mapping.getIds()).isEqualTo("id");
    assertThat(mapping.getFieldMappings().size()).isEqualTo(3);
    assertThat(mapping.getFieldMappings()).containsAll(getEmployeeTableFieldMappings());
  }

  @Test
  public void mappingSuccessWhenFieldMappingsAreOmitted() throws Exception {
    createEmployeeTable();

    cache = createCacheAndCreateJdbcMapping("NoFieldMappings");
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);

    RegionMapping mapping = service.getMappingForRegion(REGION_NAME);
    assertThat(mapping.getDataSourceName()).isEqualTo(DATA_SOURCE_NAME);
    assertThat(mapping.getTableName()).isEqualTo(REGION_TABLE_NAME);
    assertThat(mapping.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(mapping.getPdxName()).isEqualTo(Employee.class.getName());
    assertThat(mapping.getIds()).isEqualTo("id");
    assertThat(mapping.getFieldMappings().size()).isEqualTo(3);
    assertThat(mapping.getFieldMappings()).containsAll(getEmployeeTableFieldMappings());
  }

  @Test
  public void mappingSuccessWhenFieldMappingsAreOmittedWithNonSerializedClass() throws Exception {
    createEmployeeTable();

    cache = createCacheAndCreateJdbcMapping("NoFieldMappingsWithNonSerializedClass");
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);

    RegionMapping mapping = service.getMappingForRegion(REGION_NAME);
    assertThat(mapping.getDataSourceName()).isEqualTo(DATA_SOURCE_NAME);
    assertThat(mapping.getTableName()).isEqualTo(REGION_TABLE_NAME);
    assertThat(mapping.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(mapping.getPdxName()).isEqualTo(NonSerializedEmployee.class.getName());
    assertThat(mapping.getIds()).isEqualTo("id");
    assertThat(mapping.getFieldMappings().size()).isEqualTo(3);
    assertThat(mapping.getFieldMappings()).containsAll(getEmployeeTableFieldMappings());
  }

  @Test
  public void mappingFailureWhenConnectWrongDataSource() {
    Throwable throwable =
        catchThrowable(() -> createCacheAndCreateJdbcMappingWithWrongDataSource("NoFieldMappings"));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage(String.format("No datasource \"%s\" found when creating default field mapping",
            DATA_SOURCE_NAME));
  }

  @Test
  public void mappingFailureWhenTableNotExists() {
    Throwable throwable = catchThrowable(() -> createCacheAndCreateJdbcMapping("NoFieldMappings"));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage(String.format("No table was found that matches \"%s\"", REGION_TABLE_NAME));
  }

  @Test
  public void mappingFailureWhenPdxNotExists() throws Exception {
    createEmployeeTable();

    Throwable throwable =
        catchThrowable(() -> createCacheAndCreateJdbcMapping("WrongPdxName"));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessageContaining(
            "The pdx class \"org.apache.geode.connectors.jdbc.NoPdx\" could not be loaded because: java.lang.ClassNotFoundException: org.apache.geode.connectors.jdbc.NoPdx");
  }

  @Test
  public void mappingFailureWhenPdxFieldAndTableMetaDataUnMatch() throws Exception {
    createEmployeeTableWithColumnNamesWithUnderscores();

    Throwable throwable = catchThrowable(() -> createCacheAndCreateJdbcMapping("NoFieldMappings"));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage("No PDX field name matched the column name \"_name\"");
  }

  @Test
  public void mappingFailureWhenFieldMappingAndTableMetaDataUnMatch() throws Exception {
    createEmployeeTableWithColumnNamesWithUnderscores();

    Throwable throwable = catchThrowable(() -> createCacheAndCreateJdbcMapping("FieldMappings"));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessageContaining(
            String.format("Jdbc mapping for \"%s\" does not match table definition", REGION_NAME));
  }

  @Test
  public void mappingSuccessWhenPdxFieldAndTableMetaDataUnMatchButFieldMappingMatch()
      throws Exception {
    createEmployeeTableWithColumnNamesWithUnderscores();

    cache = createCacheAndCreateJdbcMapping("FieldMappingsColumnNamesWithUnderscores");
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);

    RegionMapping mapping = service.getMappingForRegion(REGION_NAME);
    assertThat(mapping.getDataSourceName()).isEqualTo(DATA_SOURCE_NAME);
    assertThat(mapping.getTableName()).isEqualTo(REGION_TABLE_NAME);
    assertThat(mapping.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(mapping.getPdxName()).isEqualTo(Employee.class.getName());
    assertThat(mapping.getIds()).isEqualTo("id");
    assertThat(mapping.getFieldMappings().size()).isEqualTo(3);
    assertThat(mapping.getFieldMappings())
        .containsAll(getEmployeeTableColumnNameWithUnderscoresFieldMappings());
  }
}
