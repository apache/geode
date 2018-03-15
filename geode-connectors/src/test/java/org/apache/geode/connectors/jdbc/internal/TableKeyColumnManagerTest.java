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
public class TableKeyColumnManagerTest {
  private static final String TABLE_NAME = "testTable";
  private static final String KEY_COLUMN = "keyColumn";

  private TableKeyColumnManager tableKeyColumnManager;
  private Connection connection;

  @Before
  public void setup() throws Exception {
    tableKeyColumnManager = new TableKeyColumnManager();
    connection = mock(Connection.class);
  }

  @Test
  public void returnsSinglePrimaryKeyColumnName() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    assertThat(tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME))
        .isEqualTo(KEY_COLUMN);
    verify(connection).getMetaData();
  }

  @Test
  public void secondCallDoesNotUseMetaData() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    assertThat(tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME))
        .isEqualTo(KEY_COLUMN);
    assertThat(tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME))
        .isEqualTo(KEY_COLUMN);
    verify(connection).getMetaData();
  }

  @Test
  public void throwsExceptionWhenFailsToGetTableMetadata() throws Exception {
    when(connection.getMetaData()).thenThrow(SQLException.class);

    assertThatThrownBy(() -> tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class);
  }

  @Test
  public void throwsExceptionWhenDesiredTableNotFound() throws Exception {
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getTables(any(), any(), any(), any())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString("TABLE_NAME")).thenReturn("otherTable");

    assertThatThrownBy(() -> tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class);
  }

  @Test
  public void throwsExceptionIfTableHasCompositePrimaryKey() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true);

    assertThatThrownBy(() -> tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class);
  }

  @Test
  public void throwsExceptionWhenTwoTablesHasCaseInsensitiveSameName() throws Exception {
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getTables(any(), any(), any(), any())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(resultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME);
    when(resultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toUpperCase());

    assertThatThrownBy(() -> tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class)
        .hasMessage("Duplicate tables that match region name");
  }

  @Test
  public void throwsExceptionWhenNoPrimaryKeyInTable() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(false);

    assertThatThrownBy(() -> tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class);
  }

  private ResultSet getPrimaryKeysMetaData() throws SQLException {
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet resultSet = mock(ResultSet.class);
    ResultSet primaryKeys = mock(ResultSet.class);

    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getTables(any(), any(), any(), any())).thenReturn(resultSet);
    when(metadata.getPrimaryKeys(any(), any(), anyString())).thenReturn(primaryKeys);
    when(primaryKeys.getString("COLUMN_NAME")).thenReturn(KEY_COLUMN);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME);

    return primaryKeys;
  }
}
