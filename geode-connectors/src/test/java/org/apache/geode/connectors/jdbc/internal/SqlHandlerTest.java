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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;

import javax.sql.DataSource;

import junitparams.JUnitParamsRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.SqlHandler.DataSourceFactory;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;

@RunWith(JUnitParamsRunner.class)
public class SqlHandlerTest {
  private static final String DATA_SOURCE_NAME = "dataSourceName";
  private static final String REGION_NAME = "testRegion";
  private static final String TABLE_NAME = "testTable";
  private static final String KEY_COLUMN = "keyColumn";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private DataSource dataSource;
  private JdbcConnectorService connectorService;
  private DataSourceFactory dataSourceFactory;
  private TableMetaDataManager tableMetaDataManager;
  private TableMetaDataView tableMetaDataView;
  private Connection connection;
  private Region<Object, Object> region;
  private InternalCache cache;
  private SqlHandler handler;
  private PreparedStatement statement;
  private RegionMapping regionMapping;
  private PdxInstanceImpl value;
  private Object key;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {
    dataSource = mock(DataSource.class);
    region = mock(Region.class);
    when(region.getName()).thenReturn(REGION_NAME);
    cache = mock(InternalCache.class);
    connection = mock(Connection.class);
    when(region.getRegionService()).thenReturn(cache);
    tableMetaDataManager = mock(TableMetaDataManager.class);
    tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getTableName()).thenReturn(TABLE_NAME);
    when(tableMetaDataView.getKeyColumnName()).thenReturn(KEY_COLUMN);
    when(tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .thenReturn(tableMetaDataView);
    connectorService = mock(JdbcConnectorService.class);
    dataSourceFactory = mock(DataSourceFactory.class);
    when(dataSourceFactory.getDataSource(DATA_SOURCE_NAME)).thenReturn(dataSource);
    handler = new SqlHandler(tableMetaDataManager, connectorService, dataSourceFactory);
    key = "key";
    value = mock(PdxInstanceImpl.class);
    when(value.getPdxType()).thenReturn(mock(PdxType.class));

    regionMapping = mock(RegionMapping.class);
    when(regionMapping.getConnectionConfigName()).thenReturn(DATA_SOURCE_NAME);
    when(regionMapping.getRegionName()).thenReturn(REGION_NAME);
    when(regionMapping.getTableName()).thenReturn(TABLE_NAME);
    when(regionMapping.getRegionToTableName()).thenReturn(TABLE_NAME);
    when(connectorService.getMappingForRegion(REGION_NAME)).thenReturn(regionMapping);

    when(dataSource.getConnection()).thenReturn(this.connection);

    statement = mock(PreparedStatement.class);
    when(this.connection.prepareStatement(any())).thenReturn(statement);
  }

  @Test
  public void readThrowsIfNoKeyProvided() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    handler.read(region, null);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void readThrowsIfNoMapping() throws Exception {
    thrown.expect(JdbcConnectorException.class);
    handler.read(mock(Region.class), new Object());
  }

  @Test
  public void readThrowsIfNoConnectionConfig() throws Exception {
    @SuppressWarnings("unchecked")
    Region<Object, Object> region2 = mock(Region.class);
    when(region2.getName()).thenReturn("region2");
    RegionMapping regionMapping2 = mock(RegionMapping.class);
    when(regionMapping2.getConnectionConfigName()).thenReturn("bogus connection name");
    when(regionMapping2.getRegionName()).thenReturn("region2");
    when(connectorService.getMappingForRegion("region2")).thenReturn(regionMapping2);

    thrown.expect(JdbcConnectorException.class);
    handler.read(region2, new Object());
  }

  @Test
  public void readClosesPreparedStatementWhenFinished() throws Exception {
    setupEmptyResultSet();
    Object getKey = "getkey";
    handler.read(region, getKey);
    verify(statement).executeQuery();
    verify(statement).setObject(1, getKey);
    verify(statement).close();
  }

  @Test
  public void throwsExceptionIfQueryFails() throws Exception {
    when(statement.executeQuery()).thenThrow(SQLException.class);

    thrown.expect(SQLException.class);
    handler.read(region, new Object());
  }

  @Test
  public void writeThrowsExceptionIfValueIsNullAndNotDoingDestroy() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    handler.write(region, Operation.UPDATE, new Object(), null);
  }

  @Test
  public void writeWithCharField() throws Exception {
    String fieldName = "fieldName";
    Object fieldValue = 'S';
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setObject(1, fieldValue.toString());
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithDateField() throws Exception {
    String fieldName = "fieldName";
    Object fieldValue = new Date();
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setObject(1, fieldValue);
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithDateFieldWithDateTypeFromMetaData() throws Exception {
    String fieldName = "fieldName";
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Date(fieldValue.getTime());
    int dataType = Types.DATE;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setObject(1, expectedValueWritten);
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithDateFieldWithTimeTypeFromMetaData() throws Exception {
    String fieldName = "fieldName";
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Time(fieldValue.getTime());
    int dataType = Types.TIME;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setObject(1, expectedValueWritten);
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithDateFieldWithTimeWithTimezoneTypeFromMetaData() throws Exception {
    String fieldName = "fieldName";
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Time(fieldValue.getTime());
    int dataType = Types.TIME_WITH_TIMEZONE;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setObject(1, expectedValueWritten);
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithDateFieldWithTimestampTypeFromMetaData() throws Exception {
    String fieldName = "fieldName";
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Timestamp(fieldValue.getTime());
    int dataType = Types.TIMESTAMP;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setObject(1, expectedValueWritten);
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithDateFieldWithTimestampWithTimezoneTypeFromMetaData() throws Exception {
    String fieldName = "fieldName";
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Timestamp(fieldValue.getTime());
    int dataType = Types.TIMESTAMP_WITH_TIMEZONE;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setObject(1, expectedValueWritten);
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithNonCharField() throws Exception {
    String fieldName = "fieldName";
    int fieldValue = 100;
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setObject(1, fieldValue);
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithNullField() throws Exception {
    String fieldName = "fieldName";
    Object fieldValue = null;
    int dataType = 0;
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setNull(1, dataType);
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithNullFieldWithDataTypeFromMetaData() throws Exception {
    String fieldName = "fieldName";
    Object fieldValue = null;
    int dataType = 79;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
    when(regionMapping.getColumnNameForField(eq(fieldName), any())).thenReturn(fieldName);
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setNull(1, dataType);
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }


  @Test
  public void insertActionSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);
    verify(statement).setObject(1, createKey);
    verify(statement).executeUpdate();
    verify(statement).close();
  }

  @Test
  public void updateActionSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    Object updateKey = "updateKey";
    handler.write(region, Operation.UPDATE, updateKey, value);
    verify(statement).setObject(1, updateKey);
    verify(statement).executeUpdate();
    verify(statement).close();
  }

  @Test
  public void destroyActionSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    Object destroyKey = "destroyKey";
    handler.write(region, Operation.DESTROY, destroyKey, value);
    verify(statement).setObject(1, destroyKey);
    verify(statement, times(1)).setObject(anyInt(), any());
    verify(statement).close();
  }

  @Test
  public void destroyActionThatRemovesNoRowCompletesUnexceptionally() throws Exception {
    when(statement.executeUpdate()).thenReturn(0);
    Object destroyKey = "destroyKey";
    handler.write(region, Operation.DESTROY, destroyKey, value);
    verify(statement).setObject(1, destroyKey);
    verify(statement, times(1)).setObject(anyInt(), any());
    verify(statement).close();
  }

  @Test
  public void destroyThrowExceptionWhenFail() throws Exception {
    when(statement.executeUpdate()).thenThrow(SQLException.class);

    thrown.expect(SQLException.class);
    handler.write(region, Operation.DESTROY, new Object(), value);
  }

  @Test
  public void writesWithUnsupportedOperationThrows() throws Exception {
    thrown.expect(InternalGemFireException.class);
    handler.write(region, Operation.INVALIDATE, new Object(), value);
  }

  @Test
  public void preparedStatementClearedAfterExecution() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    handler.write(region, Operation.CREATE, new Object(), value);
    verify(statement).close();
  }

  @Test
  public void whenInsertFailsUpdateSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(0);

    PreparedStatement updateStatement = mock(PreparedStatement.class);
    when(updateStatement.executeUpdate()).thenReturn(1);
    when(connection.prepareStatement(any())).thenReturn(statement).thenReturn(updateStatement);

    handler.write(region, Operation.CREATE, new Object(), value);
    verify(statement).executeUpdate();
    verify(updateStatement).executeUpdate();
    verify(statement).close();
    verify(updateStatement).close();
  }

  @Test
  public void whenUpdateFailsInsertSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(0);

    PreparedStatement insertStatement = mock(PreparedStatement.class);
    when(insertStatement.executeUpdate()).thenReturn(1);
    when(connection.prepareStatement(any())).thenReturn(statement).thenReturn(insertStatement);

    Object putKey = "putKey";
    handler.write(region, Operation.UPDATE, putKey, value);
    verify(statement).executeUpdate();
    verify(insertStatement).executeUpdate();
    verify(statement).executeUpdate();
    verify(statement).setObject(1, putKey);
    verify(statement).close();
    verify(statement).executeUpdate();
    verify(statement).setObject(1, putKey);
    verify(insertStatement).close();
  }

  @Test
  public void whenInsertFailsWithExceptionUpdateSucceeds() throws Exception {
    when(statement.executeUpdate()).thenThrow(SQLException.class);

    PreparedStatement updateStatement = mock(PreparedStatement.class);
    when(updateStatement.executeUpdate()).thenReturn(1);
    when(connection.prepareStatement(any())).thenReturn(statement).thenReturn(updateStatement);

    handler.write(region, Operation.CREATE, new Object(), value);
    verify(statement).executeUpdate();
    verify(updateStatement).executeUpdate();
    verify(statement).close();
    verify(updateStatement).close();
  }

  @Test
  public void whenUpdateFailsWithExceptionInsertSucceeds() throws Exception {
    when(statement.executeUpdate()).thenThrow(SQLException.class);

    PreparedStatement insertStatement = mock(PreparedStatement.class);
    when(insertStatement.executeUpdate()).thenReturn(1);
    when(connection.prepareStatement(any())).thenReturn(statement).thenReturn(insertStatement);

    handler.write(region, Operation.UPDATE, new Object(), value);
    verify(statement).executeUpdate();
    verify(insertStatement).executeUpdate();
    verify(statement).close();
    verify(insertStatement).close();
  }

  @Test
  public void whenBothInsertAndUpdateFailExceptionIsThrown() throws Exception {
    when(statement.executeUpdate()).thenThrow(SQLException.class);

    PreparedStatement insertStatement = mock(PreparedStatement.class);
    when(insertStatement.executeUpdate()).thenThrow(SQLException.class);
    when(connection.prepareStatement(any())).thenReturn(statement).thenReturn(insertStatement);

    thrown.expect(SQLException.class);
    handler.write(region, Operation.UPDATE, new Object(), value);
    verify(statement).close();
    verify(insertStatement).close();
  }

  private void setupEmptyResultSet() throws SQLException {
    ResultSet result = mock(ResultSet.class);
    when(result.next()).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);
  }

  @Test
  public void returnsCorrectColumnForGet() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, regionMapping, key, value, Operation.GET);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).isEmpty();
    assertThat(entryColumnData.getEntryKeyColumnData().getColumnName()).isEqualTo(KEY_COLUMN);
  }

  @Test
  public void returnsCorrectColumnsForUpsertOperations() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    String nonKeyColumn = "otherColumn";
    when(regionMapping.getColumnNameForField(eq(KEY_COLUMN), any())).thenReturn(KEY_COLUMN);
    when(regionMapping.getColumnNameForField(eq(nonKeyColumn), any())).thenReturn(nonKeyColumn);
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);
    when(value.getFieldNames()).thenReturn(Arrays.asList(KEY_COLUMN, nonKeyColumn));

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, regionMapping, key, value, Operation.UPDATE);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryValueColumnData().get(0).getColumnName())
        .isEqualTo(nonKeyColumn);
    assertThat(entryColumnData.getEntryKeyColumnData().getColumnName()).isEqualTo(KEY_COLUMN);
  }

  @Test
  public void returnsCorrectColumnForDestroy() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, regionMapping, key, value, Operation.DESTROY);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).isEmpty();
    assertThat(entryColumnData.getEntryKeyColumnData().getColumnName()).isEqualTo(KEY_COLUMN);
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

  @Test
  public void handlesSQLExceptionFromGetConnection() throws Exception {
    doThrow(new SQLException("test exception")).when(dataSource).getConnection();

    assertThatThrownBy(() -> handler.getConnection(DATA_SOURCE_NAME))
        .isInstanceOf(SQLException.class).hasMessage("test exception");
  }

}
