/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.connectors.jdbc.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;


public abstract class TableMetaDataManagerIntegrationTest {

  protected static final String REGION_TABLE_NAME = "employees";
  protected static final String DB_NAME = "test";

  protected TableMetaDataManager manager;
  protected Connection connection;
  protected Statement statement;
  protected RegionMapping regionMapping;

  @Before
  public void setup() throws Exception {
    connection = getConnection();
    statement = connection.createStatement();
    manager = new TableMetaDataManager();
    regionMapping = new RegionMapping();
    regionMapping.setTableName(REGION_TABLE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    closeDB();
  }

  private void closeDB() throws Exception {
    if (statement == null) {
      statement = connection.createStatement();
    }
    statement.execute("Drop table IF EXISTS " + REGION_TABLE_NAME);
    statement.execute("Drop table IF EXISTS MYSCHEMA." + REGION_TABLE_NAME);
    statement.execute("Drop schema IF EXISTS MYSCHEMA");
    statement.close();

    if (connection != null) {
      connection.close();
    }
  }

  protected abstract Connection getConnection() throws SQLException;

  protected void createTable() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String quote = metaData.getIdentifierQuoteString();
    statement.execute("CREATE TABLE " + REGION_TABLE_NAME + " (" + quote + "id" + quote
        + " VARCHAR(10) primary key not null," + quote + "name" + quote + " VARCHAR(10)," + quote
        + "age" + quote + " int)");
  }

  protected void createTableWithSchema() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String quote = metaData.getIdentifierQuoteString();
    statement.execute("CREATE SCHEMA MYSCHEMA");
    statement.execute("CREATE TABLE MYSCHEMA." + REGION_TABLE_NAME + " (" + quote + "id" + quote
        + " VARCHAR(10) primary key not null," + quote + "name" + quote + " VARCHAR(10)," + quote
        + "age" + quote + " int)");
  }

  protected void createTableWithNoPrimaryKey() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String quote = metaData.getIdentifierQuoteString();
    statement.execute("CREATE TABLE " + REGION_TABLE_NAME + " (" + quote + "nonprimaryid" + quote
        + " VARCHAR(10)," + quote + "name" + quote + " VARCHAR(10)," + quote
        + "age" + quote + " int)");
  }

  protected void createTableWithMultiplePrimaryKeys() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String quote = metaData.getIdentifierQuoteString();
    statement.execute("CREATE TABLE " + REGION_TABLE_NAME + " (" + quote + "id" + quote
        + " VARCHAR(10)," + quote + "name" + quote + " VARCHAR(10)," + quote
        + "age" + quote + " int, PRIMARY KEY (id, name))");
  }

  @Test
  public void validateKeyColumnName() throws SQLException {
    createTable();
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, regionMapping);

    List<String> keyColumnNames = metaData.getKeyColumnNames();

    assertThat(keyColumnNames).isEqualTo(Arrays.asList("id"));
  }

  protected abstract void setSchemaOrCatalogOnMapping(RegionMapping regionMapping, String name);

  @Test
  public void validateKeyColumnNameWithSchema() throws SQLException {
    createTableWithSchema();
    setSchemaOrCatalogOnMapping(regionMapping, "MYSCHEMA");
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, regionMapping);

    List<String> keyColumnNames = metaData.getKeyColumnNames();

    assertThat(keyColumnNames).isEqualTo(Arrays.asList("id"));
  }

  @Test
  public void validateUnknownSchema() throws SQLException {
    createTable();
    regionMapping.setSchema("unknownSchema");
    assertThatThrownBy(
        () -> manager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class)
            .hasMessageContaining("No schema was found that matches \"unknownSchema\"");
  }

  @Test
  public void validateUnknownCatalog() throws SQLException {
    createTable();
    regionMapping.setCatalog("unknownCatalog");
    assertThatThrownBy(
        () -> manager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class)
            .hasMessageContaining("No catalog was found that matches \"unknownCatalog\"");
  }

  @Test
  public void validateMultipleKeyColumnNames() throws SQLException {
    createTableWithMultiplePrimaryKeys();
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, regionMapping);

    List<String> keyColumnNames = metaData.getKeyColumnNames();

    assertThat(keyColumnNames).isEqualTo(Arrays.asList("id", "name"));
  }

  @Test
  public void validateKeyColumnNameOnNonPrimaryKey() throws SQLException {
    createTableWithNoPrimaryKey();
    regionMapping.setIds("nonprimaryid");
    TableMetaDataView metaData =
        manager.getTableMetaDataView(connection, regionMapping);

    List<String> keyColumnNames = metaData.getKeyColumnNames();

    assertThat(keyColumnNames).isEqualTo(Arrays.asList("nonprimaryid"));
  }

  @Test
  public void validateKeyColumnNameOnNonPrimaryKeyWithInExactMatch() throws SQLException {
    createTableWithNoPrimaryKey();
    regionMapping.setIds("NonPrimaryId");
    TableMetaDataView metaData =
        manager.getTableMetaDataView(connection, regionMapping);

    List<String> keyColumnNames = metaData.getKeyColumnNames();

    assertThat(keyColumnNames).isEqualTo(Arrays.asList("NonPrimaryId"));
  }

  @Test
  public void validateColumnDataTypeForName() throws SQLException {
    createTable();
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, regionMapping);

    JDBCType nameDataType = metaData.getColumnDataType("name");

    assertThat(nameDataType).isEqualTo(JDBCType.VARCHAR);
  }

  @Test
  public void validateIsColumnNullableForName() throws SQLException {
    createTable();
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, regionMapping);

    boolean nullable = metaData.isColumnNullable("name");

    assertThat(nullable).isTrue();
  }

  @Test
  public void validateIsColumnNullableForId() throws SQLException {
    createTable();
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, regionMapping);

    boolean nullable = metaData.isColumnNullable("id");

    assertThat(nullable).isFalse();
  }

  @Test
  public void validateColumnDataTypeForId() throws SQLException {
    createTable();
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, regionMapping);

    JDBCType nameDataType = metaData.getColumnDataType("id");

    assertThat(nameDataType).isEqualTo(JDBCType.VARCHAR);
  }

  @Test
  public void validateColumnDataTypeForAge() throws SQLException {
    createTable();
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, regionMapping);

    JDBCType nameDataType = metaData.getColumnDataType("age");

    assertThat(nameDataType).isEqualTo(JDBCType.INTEGER);
  }

}
