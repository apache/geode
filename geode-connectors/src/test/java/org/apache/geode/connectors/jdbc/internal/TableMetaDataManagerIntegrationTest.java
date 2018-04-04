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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public abstract class TableMetaDataManagerIntegrationTest {

  private static final String REGION_TABLE_NAME = "employees";
  protected static final String DB_NAME = "test";

  private TableMetaDataManager manager;
  protected Connection connection;
  protected Statement statement;

  @Before
  public void setup() throws Exception {
    connection = getConnection();
    statement = connection.createStatement();
    createTable();
    manager = new TableMetaDataManager();
  }

  @After
  public void tearDown() throws Exception {
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

  protected abstract Connection getConnection() throws SQLException;

  protected void createTable() throws SQLException {
    statement.execute("CREATE TABLE " + REGION_TABLE_NAME
        + " (id VARCHAR(10) primary key not null, name VARCHAR(10), age int)");

  }

  @Test
  public void validateKeyColumnName() {
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, REGION_TABLE_NAME);

    String keyColumnName = metaData.getKeyColumnName();

    assertThat(keyColumnName).isEqualTo("id");
  }

  @Test
  public void validateColumnDataTypeForName() {
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, REGION_TABLE_NAME);

    int nameDataType = metaData.getColumnDataType("name");

    assertThat(nameDataType).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void validateColumnDataTypeForId() {
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, REGION_TABLE_NAME);

    int nameDataType = metaData.getColumnDataType("id");

    assertThat(nameDataType).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void validateColumnDataTypeForAge() {
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, REGION_TABLE_NAME);

    int nameDataType = metaData.getColumnDataType("age");

    assertThat(nameDataType).isEqualTo(Types.INTEGER);
  }

}
