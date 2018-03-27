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
package org.apache.geode.connectors.jdbc.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.Types;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.DatabaseConnectionRule;
import org.apache.geode.test.junit.rules.MySqlConnectionRule;

@Category(IntegrationTest.class)
public class TableMetaDataManagerMySqlIntegrationTest {

  private static final String DB_NAME = "test";
  private static final String REGION_TABLE_NAME = "employees";

  private TableMetaDataManager manager;
  private Connection connection;
  private Statement statement;

  @ClassRule
  public static DatabaseConnectionRule dbRule =
      new MySqlConnectionRule.Builder().file("src/test/resources/docker/mysql.yml")
          .serviceName("db").port(3306).database(DB_NAME).build();

  @Before
  public void setup() throws Exception {
    connection = dbRule.getConnection();
    statement = connection.createStatement();
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (id VARCHAR(10) primary key not null, name VARCHAR(10), age int)");
    manager = new TableMetaDataManager();
    DatabaseMetaData data = connection.getMetaData();
    data.getConnection();
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

  @Test
  public void validateKeyColumnName() {
    TableMetaDataView metaData = manager.getTableMetaDataView(connection, REGION_TABLE_NAME);

    String keyColumnName = metaData.getKeyColumnName();

    assertThat(keyColumnName).isEqualToIgnoringCase("id");
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
