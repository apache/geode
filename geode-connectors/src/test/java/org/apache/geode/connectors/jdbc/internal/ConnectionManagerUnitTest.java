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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Operation;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionManagerUnitTest {
  private static final String REGION_NAME = "testRegion";
  private static final String TABLE_NAME = "testTable";
  private static final String CONFIG_NAME = "configName";
  private static final String KEY_COLUMN = "keyColumn";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private JdbcConnectorService configService;
  private ConnectionManager manager;
  private Connection connection;
  private ConnectionConfiguration connectionConfig;
  private RegionMapping mapping;
  private Object key = new Object();
  private PdxInstance value = mock(PdxInstance.class);


  @Before
  public void setup() throws Exception {
    configService = mock(JdbcConnectorService.class);
    manager = spy(new ConnectionManager(configService));
    connection = mock(Connection.class);

    connectionConfig = getTestConnectionConfig("name", "url", null, null);
    doReturn(connection).when(manager).getSQLConnection(connectionConfig);

    mapping = mock(RegionMapping.class);
    when(mapping.getTableName()).thenReturn(TABLE_NAME);
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
        getTestConnectionConfig("newName", "url", null, null);
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
        getTestConnectionConfig("newName", "url", null, null);

    doReturn(secondConnection).when(manager).getSQLConnection(secondConnectionConfig);
    manager.getConnection(connectionConfig);
    manager.getConnection(secondConnectionConfig);

    manager.close();
    verify(connection).close();
    verify(secondConnection).close();
  }

  @Test
  public void returnsCorrectColumnForDestroy() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetadData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    List<ColumnValue> columnValueList =
        manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.DESTROY);
    assertThat(columnValueList).hasSize(1);
    assertThat(columnValueList.get(0).getColumnName()).isEqualTo(KEY_COLUMN);
  }

  @Test
  public void returnsCorrectColumnForGet() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetadData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    List<ColumnValue> columnValueList =
        manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET);
    assertThat(columnValueList).hasSize(1);
    assertThat(columnValueList.get(0).getColumnName()).isEqualTo(KEY_COLUMN);
  }

  @Test
  public void throwsExceptionIfTableHasCompositePrimaryKey() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetadData();
    when(primaryKeys.next()).thenReturn(true);

    thrown.expect(IllegalStateException.class);
    manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET);
  }

  @Test
  public void throwsExceptionWhenTwoTablesHasCaseInsensitiveSameName() throws Exception {
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metadata);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(resultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME);
    when(resultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toUpperCase());
    when(metadata.getTables(any(), any(), any(), any())).thenReturn(resultSet);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Duplicate tables that match region name");
    manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET);
  }

  @Test
  public void throwsExceptionWhenDesiredTableNotFound() throws Exception {
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metadata);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString("TABLE_NAME")).thenReturn("otherTable");
    when(metadata.getTables(any(), any(), any(), any())).thenReturn(resultSet);

    thrown.expect(IllegalStateException.class);
    manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET);
  }

  @Test
  public void throwsExceptionWhenNoPrimaryKeyInTable() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetadData();
    when(primaryKeys.next()).thenReturn(false);

    thrown.expect(IllegalStateException.class);
    manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET);
  }

  @Test
  public void throwsExceptionWhenFailsToGetTableMetadata() throws Exception {
    when(connection.getMetaData()).thenThrow(SQLException.class);

    thrown.expect(IllegalStateException.class);
    manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.GET);
  }

  @Test
  public void returnsCorrectColumnsForUpsertOperations() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetadData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    String nonKeyColumn = "otherColumn";
    when(mapping.getColumnNameForField(KEY_COLUMN)).thenReturn(KEY_COLUMN);
    when(mapping.getColumnNameForField(nonKeyColumn)).thenReturn(nonKeyColumn);
    when(value.getFieldNames()).thenReturn(Arrays.asList(KEY_COLUMN, nonKeyColumn));

    List<ColumnValue> columnValueList =
        manager.getColumnToValueList(connectionConfig, mapping, key, value, Operation.UPDATE);
    assertThat(columnValueList).hasSize(2);
    assertThat(columnValueList.get(0).getColumnName()).isEqualTo(nonKeyColumn);
    assertThat(columnValueList.get(1).getColumnName()).isEqualTo(KEY_COLUMN);
  }

  private ConnectionConfiguration getTestConnectionConfig(String name, String url, String user,
      String password) {
    ConnectionConfiguration config = new ConnectionConfiguration(name, url, user, password, null);
    return config;
  }

  private ResultSet getPrimaryKeysMetadData() throws SQLException {
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metadata);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME);
    when(metadata.getTables(any(), any(), any(), any())).thenReturn(resultSet);
    ResultSet primaryKeys = mock(ResultSet.class);
    when(metadata.getPrimaryKeys(any(), any(), anyString())).thenReturn(primaryKeys);
    when(primaryKeys.getString("COLUMN_NAME")).thenReturn(KEY_COLUMN);
    return primaryKeys;
  }
}
