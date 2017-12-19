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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionManagerUnitTest {

  private static final String REGION_NAME = "testRegion";
  private static final String TABLE_NAME = "testTable";
  private static final String CONFIG_NAME = "configName";
  private static final String KEY_COLUMN = "keyColumn";

  private JdbcConnectorService configService;
  private ConnectionManager manager;
  private Connection connection;
  private RegionMapping mapping;

  private Object key;
  private PdxInstance value;

  private ConnectionConfiguration connectionConfig;

  @Before
  public void setup() throws Exception {
    configService = mock(JdbcConnectorService.class);
    manager = spy(new ConnectionManager(configService));
    connection = mock(Connection.class);
    mapping = mock(RegionMapping.class);
    value = mock(PdxInstance.class);

    connectionConfig = new ConnectionConfiguration("name", "url", null, null, null);

    when(mapping.getTableName()).thenReturn(TABLE_NAME);
    when(mapping.getRegionToTableName()).thenReturn(TABLE_NAME);
    doReturn(connection).when(manager).getSQLConnection(connectionConfig);

    key = new Object();
  }

  @Test
  public void getsCorrectMapping() {
    manager.getMappingForRegion(REGION_NAME);

    verify(configService).getMappingForRegion(REGION_NAME);
  }

  @Test
  public void getsCorrectConnectionConfig() {
    manager.getConnectionConfig(CONFIG_NAME);

    verify(configService).getConnectionConfig(CONFIG_NAME);
  }

  @Test
  public void retrievesANewConnection() throws Exception {
    Connection returnedConnection = manager.getConnection(connectionConfig);

    assertThat(returnedConnection).isNotNull().isSameAs(connection);
  }

  @Test
  public void retrievesSameConnectionForSameConnectionConfig() throws Exception {
    Connection returnedConnection = manager.getConnection(connectionConfig);
    Connection secondReturnedConnection = manager.getConnection(connectionConfig);

    assertThat(returnedConnection).isNotNull().isSameAs(connection);
    assertThat(secondReturnedConnection).isNotNull().isSameAs(connection);
  }

  @Test
  public void retrievesDifferentConnectionForEachConfig() throws Exception {
    Connection secondConnection = mock(Connection.class);
    ConnectionConfiguration secondConnectionConfig =
        new ConnectionConfiguration("newName", "url", null, null, null);
    doReturn(secondConnection).when(manager).getSQLConnection(secondConnectionConfig);

    Connection returnedConnection = manager.getConnection(connectionConfig);
    Connection secondReturnedConnection = manager.getConnection(secondConnectionConfig);

    assertThat(returnedConnection).isNotNull().isSameAs(connection);
    assertThat(secondReturnedConnection).isNotNull().isSameAs(secondConnection);
    assertThat(returnedConnection).isNotSameAs(secondReturnedConnection);
  }

  @Test
  public void retrievesANewConnectionIfCachedOneIsClosed() throws Exception {
    manager.getConnection(connectionConfig);
    when(connection.isClosed()).thenReturn(true);
    Connection secondConnection = mock(Connection.class);
    doReturn(secondConnection).when(manager).getSQLConnection(connectionConfig);

    Connection secondReturnedConnection = manager.getConnection(connectionConfig);

    assertThat(secondReturnedConnection).isSameAs(secondConnection);
  }

  @Test
  public void closesAllConnections() throws Exception {
    Connection secondConnection = mock(Connection.class);
    ConnectionConfiguration secondConnectionConfig =
        new ConnectionConfiguration("newName", "url", null, null, null);
    doReturn(secondConnection).when(manager).getSQLConnection(secondConnectionConfig);
    manager.getConnection(connectionConfig);
    manager.getConnection(secondConnectionConfig);

    manager.close();

    verify(connection).close();
    verify(secondConnection).close();
  }

  @Test
  public void returnsCorrectColumnForDestroy() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    List<ColumnValue> columnValueList =
        manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.DESTROY);

    assertThat(columnValueList).hasSize(1);
    assertThat(columnValueList.get(0).getColumnName()).isEqualTo(KEY_COLUMN);
  }

  @Test
  public void returnsCorrectColumnForGet() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    List<ColumnValue> columnValueList =
        manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET);

    assertThat(columnValueList).hasSize(1);
    assertThat(columnValueList.get(0).getColumnName()).isEqualTo(KEY_COLUMN);
  }

  @Test
  public void throwsExceptionIfTableHasCompositePrimaryKey() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true);

    assertThatThrownBy(
        () -> manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET))
            .isInstanceOf(IllegalStateException.class);
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

    assertThatThrownBy(
        () -> manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Duplicate tables that match region name");
  }

  @Test
  public void throwsExceptionWhenDesiredTableNotFound() throws Exception {
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getTables(any(), any(), any(), any())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString("TABLE_NAME")).thenReturn("otherTable");

    assertThatThrownBy(
        () -> manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET))
            .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void throwsExceptionWhenNoPrimaryKeyInTable() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(false);

    assertThatThrownBy(
        () -> manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET))
            .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void throwsExceptionWhenFailsToGetTableMetadata() throws Exception {
    when(connection.getMetaData()).thenThrow(SQLException.class);

    assertThatThrownBy(
        () -> manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET))
            .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void returnsCorrectColumnsForUpsertOperations() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    String nonKeyColumn = "otherColumn";
    when(mapping.getColumnNameForField(KEY_COLUMN)).thenReturn(KEY_COLUMN);
    when(mapping.getColumnNameForField(nonKeyColumn)).thenReturn(nonKeyColumn);
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);
    when(value.getFieldNames()).thenReturn(Arrays.asList(KEY_COLUMN, nonKeyColumn));

    List<ColumnValue> columnValueList =
        manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.UPDATE);

    assertThat(columnValueList).hasSize(2);
    assertThat(columnValueList.get(0).getColumnName()).isEqualTo(nonKeyColumn);
    assertThat(columnValueList.get(1).getColumnName()).isEqualTo(KEY_COLUMN);
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
