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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TableMetaDataManagerTest {
  private static final String TABLE_NAME = "testTable";
  private static final String KEY_COLUMN = "keyColumn";

  private TableMetaDataManager tableMetaDataManager;
  private Connection connection;
  DatabaseMetaData databaseMetaData;
  ResultSet tablesResultSet;
  ResultSet primaryKeys;
  ResultSet columnResultSet;

  @Before
  public void setup() throws Exception {
    tableMetaDataManager = new TableMetaDataManager();
    connection = mock(Connection.class);
    databaseMetaData = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(databaseMetaData);
    tablesResultSet = mock(ResultSet.class);
    when(databaseMetaData.getTables(any(), any(), any(), any())).thenReturn(tablesResultSet);
    primaryKeys = mock(ResultSet.class);
    when(databaseMetaData.getPrimaryKeys(any(), any(), anyString())).thenReturn(primaryKeys);
    columnResultSet = mock(ResultSet.class);
    when(databaseMetaData.getColumns(any(), any(), eq(TABLE_NAME), any()))
        .thenReturn(columnResultSet);
  }

  @Test
  public void returnsSinglePrimaryKeyColumnName() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    assertThat(data.getKeyColumnName()).isEqualTo(KEY_COLUMN);
    verify(connection).getMetaData();
  }

  @Test
  public void secondCallDoesNotUseMetaData() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    verify(connection).getMetaData();
  }

  @Test
  public void throwsExceptionWhenFailsToGetTableMetadata() throws Exception {
    SQLException cause = new SQLException("sql message");
    when(connection.getMetaData()).thenThrow(cause);

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class).hasMessageContaining("sql message");
  }

  @Test
  public void throwsExceptionWhenDesiredTableNotFound() throws Exception {
    when(tablesResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn("otherTable");

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class)
        .hasMessage("no table was found that matches testTable");
  }

  @Test
  public void throwsExceptionIfTableHasCompositePrimaryKey() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true);

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class)
        .hasMessage("The table " + TABLE_NAME + " has more than one primary key column.");
  }

  @Test
  public void throwsExceptionWhenTwoTablesHasCaseInsensitiveSameName() throws Exception {
    when(tablesResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toUpperCase());

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class)
        .hasMessage("Duplicate tables that match region name");
  }

  @Test
  public void throwsExceptionWhenNoPrimaryKeyInTable() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(false);

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class)
        .hasMessage("The table " + TABLE_NAME + " does not have a primary key column.");
  }

  @Test
  public void unknownColumnsDataTypeIsZero() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    int dataType = data.getColumnDataType("unknownColumn");

    assertThat(dataType).isEqualTo(0);
  }

  @Test
  public void validateExpectedDataTypes() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);
    when(columnResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    String columnName1 = "columnName1";
    int columnDataType1 = 1;
    String columnName2 = "columnName2";
    int columnDataType2 = 2;
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn(columnName1).thenReturn(columnName2);
    when(columnResultSet.getInt("DATA_TYPE")).thenReturn(columnDataType1)
        .thenReturn(columnDataType2);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    int dataType1 = data.getColumnDataType(columnName1);
    int dataType2 = data.getColumnDataType(columnName2);

    assertThat(dataType1).isEqualTo(columnDataType1);
    assertThat(dataType2).isEqualTo(columnDataType2);
  }

  @Test
  public void lookingUpDataTypeWithNameThatDiffersInCaseWillFindIt() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);
    when(columnResultSet.next()).thenReturn(true).thenReturn(false);
    String columnName1 = "columnName1";
    int columnDataType1 = 1;
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn(columnName1);
    when(columnResultSet.getInt("DATA_TYPE")).thenReturn(columnDataType1);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    int dataType1 = data.getColumnDataType(columnName1.toUpperCase());

    assertThat(dataType1).isEqualTo(columnDataType1);
  }

  @Test
  public void throwsExceptionWhenTwoColumnsWithSameCaseInsensitiveNameExist() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);
    when(columnResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn("colName").thenReturn("COLNAME");

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class).hasMessage(
            "Column names must be different in case. Two columns both have the name colname");
  }


  private void setupPrimaryKeysMetaData() throws SQLException {
    when(primaryKeys.getString("COLUMN_NAME")).thenReturn(KEY_COLUMN);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME);
  }
}
