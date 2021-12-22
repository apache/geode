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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.SqlHandler.DataSourceFactory;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class SqlHandlerTest {
  private static final String DATA_SOURCE_NAME = "dataSourceName";
  private static final String REGION_NAME = "testRegion";
  private static final String TABLE_NAME = "testTable";
  private static final String KEY_COLUMN = "keyColumn";

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
  private final String fieldName = "fieldName";
  private Set<String> columnNames;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {
    dataSource = mock(DataSource.class);
    region = mock(Region.class);
    when(region.getName()).thenReturn(REGION_NAME);
    cache = mock(InternalCache.class);
    PdxInstance pdxInstance = mock(PdxInstance.class);
    PdxInstanceFactory pdxInstanceFactory = mock(PdxInstanceFactory.class);
    when(pdxInstanceFactory.create()).thenReturn(pdxInstance);
    when(cache.createPdxInstanceFactory(any())).thenReturn(pdxInstanceFactory);
    connection = mock(Connection.class);
    when(region.getRegionService()).thenReturn(cache);
    tableMetaDataManager = mock(TableMetaDataManager.class);
    tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getQuotedTablePath()).thenReturn(TABLE_NAME);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList(KEY_COLUMN));
    final String IDS = KEY_COLUMN;
    when(tableMetaDataManager.getTableMetaDataView(any(), any()))
        .thenReturn(tableMetaDataView);
    connectorService = mock(JdbcConnectorService.class);
    dataSourceFactory = mock(DataSourceFactory.class);
    when(dataSourceFactory.getDataSource(DATA_SOURCE_NAME)).thenReturn(dataSource);
    key = "key";
    value = mock(PdxInstanceImpl.class);
    when(value.getPdxType()).thenReturn(mock(PdxType.class));
    when(value.getFieldNames()).thenReturn(Arrays.asList(fieldName));
    columnNames = new HashSet<>();
    columnNames.add(fieldName);
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(JDBCType.VARCHAR);
    regionMapping = mock(RegionMapping.class);
    when(regionMapping.getDataSourceName()).thenReturn(DATA_SOURCE_NAME);
    when(regionMapping.getRegionName()).thenReturn(REGION_NAME);
    when(regionMapping.getTableName()).thenReturn(TABLE_NAME);
    when(regionMapping.getIds()).thenReturn(IDS);
    FieldMapping fieldMapping = mock(FieldMapping.class);
    when(fieldMapping.getJdbcName()).thenReturn(fieldName);
    when(fieldMapping.getJdbcType()).thenReturn(JDBCType.VARCHAR.name());
    when(fieldMapping.getPdxName()).thenReturn(fieldName);
    when(fieldMapping.getPdxType()).thenReturn(FieldType.OBJECT.name());
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(fieldMapping));
    when(connectorService.getMappingForRegion(REGION_NAME)).thenReturn(regionMapping);
    JdbcConnectorService connectorService = mock(JdbcConnectorService.class);
    when(cache.getService(JdbcConnectorService.class)).thenReturn(connectorService);

    when(dataSource.getConnection()).thenReturn(connection);

    statement = mock(PreparedStatement.class);
    when(connection.prepareStatement(any())).thenReturn(statement);
    createSqlHandler();
  }

  @After
  public void cleanUp() {
    columnNames.clear();
  }

  private void createSqlHandler() {
    handler = new SqlHandler(cache, REGION_NAME, tableMetaDataManager, connectorService,
        dataSourceFactory);
  }

  @Test
  public void createSqlHandlerThrowsNoExceptionWithMatchingMapping() {
    createSqlHandler();
  }

  @Test
  public void createSqlHandlerHandlesSqlExceptionFromGetConnection() throws SQLException {
    doThrow(new SQLException("test exception")).when(dataSource).getConnection();

    assertThatThrownBy(this::createSqlHandler)
        .isInstanceOf(JdbcConnectorException.class).hasMessage(
            "Could not connect to datasource \"dataSourceName\" because: java.sql.SQLException: test exception");
  }

  @Test
  public void readThrowsIfNoKeyProvided() {
    assertThatThrownBy(() -> handler.read(region, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructorThrowsIfNoMapping() {
    assertThatThrownBy(
        () -> new SqlHandler(cache, "regionWithNoMapping", tableMetaDataManager, connectorService,
            dataSourceFactory)).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
                "JDBC mapping for region regionWithNoMapping not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
  }

  @Test
  public void constructorThrowsIfNoConnectionConfig() {
    when(regionMapping.getDataSourceName()).thenReturn("bogus data source name");

    assertThatThrownBy(() -> new SqlHandler(cache, REGION_NAME, tableMetaDataManager,
        connectorService,
        dataSourceFactory)).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
            "JDBC data-source named \"bogus data source name\" not found. Create it with gfsh 'create data-source --pooled --name=bogus data source name'.");
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

    assertThatThrownBy(() -> handler.read(region, new Object())).isInstanceOf(SQLException.class);
  }

  @Test
  public void writeThrowsExceptionIfValueIsNullAndNotDoingDestroy() {
    assertThatThrownBy(() -> handler.write(region, Operation.UPDATE, new Object(), null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void writeWithCharField() throws Exception {
    Object fieldValue = 'S';
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
    Object fieldValue = new Date();
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(JDBCType.NULL);
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
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Date(fieldValue.getTime());
    JDBCType dataType = JDBCType.DATE;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
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
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Time(fieldValue.getTime());
    JDBCType dataType = JDBCType.TIME;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
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
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Time(fieldValue.getTime());
    JDBCType dataType = JDBCType.TIME_WITH_TIMEZONE;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
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
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Timestamp(fieldValue.getTime());
    JDBCType dataType = JDBCType.TIMESTAMP;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
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
    Date fieldValue = new Date();
    Object expectedValueWritten = new java.sql.Timestamp(fieldValue.getTime());
    JDBCType dataType = JDBCType.TIMESTAMP_WITH_TIMEZONE;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
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
    int fieldValue = 100;
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
    Object fieldValue = null;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(JDBCType.NULL);
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setNull(1, JDBCType.NULL.getVendorTypeNumber());
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void writeWithNullFieldWithDataTypeFromMetaData() throws Exception {
    Object fieldValue = null;
    JDBCType dataType = JDBCType.VARCHAR;
    when(tableMetaDataView.getColumnDataType(fieldName)).thenReturn(dataType);
    when(value.getField(fieldName)).thenReturn(fieldValue);

    when(statement.executeUpdate()).thenReturn(1);
    Object createKey = "createKey";
    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setNull(1, dataType.getVendorTypeNumber());
    verify(statement).setObject(2, createKey);
    verify(statement).close();
  }

  @Test
  public void insertActionSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    when(value.getFieldNames()).thenReturn(Collections.emptyList());
    Object createKey = "createKey";

    handler.write(region, Operation.CREATE, createKey, value);

    verify(statement).setObject(1, createKey);
    verify(statement).executeUpdate();
    verify(statement).close();
  }

  @Test
  public void insertActionSucceedsWithCompositeKey() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    PdxInstance compositeKey = mock(PdxInstance.class);
    when(compositeKey.isDeserializable()).thenReturn(false);
    when(compositeKey.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(compositeKey.getField("fieldOne")).thenReturn(compositeKeyFieldValueOne);
    when(compositeKey.getField("fieldTwo")).thenReturn(compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn("fieldOne");
    when(fieldMapping1.getPdxName()).thenReturn("fieldOne");
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn("fieldTwo");
    when(fieldMapping2.getPdxName()).thenReturn("fieldTwo");
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    columnNames.clear();
    columnNames.add("fieldOne");
    columnNames.add("fieldTwo");
    when(tableMetaDataView.getColumnDataType("fieldOne")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwo")).thenReturn(JDBCType.VARCHAR);
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(fieldMapping1, fieldMapping2));
    when(value.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(regionMapping.getIds()).thenReturn("fieldOne,fieldTwo");
    createSqlHandler();

    handler.write(region, Operation.CREATE, compositeKey, value);

    verify(statement).setObject(1, compositeKeyFieldValueOne);
    verify(statement).setObject(2, compositeKeyFieldValueTwo);
    verify(statement, times(2)).setObject(anyInt(), any());
    verify(statement).executeUpdate();
    verify(statement).close();
  }

  @Test
  public void updateActionSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    Object updateKey = "updateKey";
    when(value.getFieldNames()).thenReturn(Collections.emptyList());

    handler.write(region, Operation.UPDATE, updateKey, value);

    verify(statement).setObject(1, updateKey);
    verify(statement).executeUpdate();
    verify(statement).close();
  }

  @Test
  public void updateActionSucceedsWithCompositeKey() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    PdxInstance compositeKey = mock(PdxInstance.class);
    when(compositeKey.isDeserializable()).thenReturn(false);
    when(compositeKey.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(compositeKey.getField("fieldOne")).thenReturn(compositeKeyFieldValueOne);
    when(compositeKey.getField("fieldTwo")).thenReturn(compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn("fieldOne");
    when(fieldMapping1.getPdxName()).thenReturn("fieldOne");
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn("fieldTwo");
    when(fieldMapping2.getPdxName()).thenReturn("fieldTwo");
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(fieldMapping1, fieldMapping2));
    when(value.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    columnNames.clear();
    columnNames.add("fieldOne");
    columnNames.add("fieldTwo");
    when(tableMetaDataView.getColumnDataType("fieldOne")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwo")).thenReturn(JDBCType.VARCHAR);
    when(regionMapping.getIds()).thenReturn("fieldOne,fieldTwo");
    createSqlHandler();

    handler.write(region, Operation.UPDATE, compositeKey, value);

    verify(statement).setObject(1, compositeKeyFieldValueOne);
    verify(statement).setObject(2, compositeKeyFieldValueTwo);
    verify(statement, times(2)).setObject(anyInt(), any());
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
  public void destroyActionSucceedsWithCompositeKey() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    Object destroyKeyFieldValueOne = "fieldValueOne";
    Object destroyKeyFieldValueTwo = "fieldValueTwo";
    PdxInstance destroyKey = mock(PdxInstance.class);
    when(destroyKey.isDeserializable()).thenReturn(false);
    when(destroyKey.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(destroyKey.getField("fieldOne")).thenReturn(destroyKeyFieldValueOne);
    when(destroyKey.getField("fieldTwo")).thenReturn(destroyKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn("fieldOne");
    when(fieldMapping1.getPdxName()).thenReturn("fieldOne");
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn("fieldTwo");
    when(fieldMapping2.getPdxName()).thenReturn("fieldTwo");
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    columnNames.clear();
    columnNames.add("fieldOne");
    columnNames.add("fieldTwo");
    when(tableMetaDataView.getColumnDataType("fieldOne")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwo")).thenReturn(JDBCType.VARCHAR);
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(fieldMapping1, fieldMapping2));
    when(regionMapping.getIds()).thenReturn("fieldOne,fieldTwo");
    createSqlHandler();

    handler.write(region, Operation.DESTROY, destroyKey, value);

    verify(statement).setObject(1, destroyKeyFieldValueOne);
    verify(statement).setObject(2, destroyKeyFieldValueTwo);
    verify(statement, times(2)).setObject(anyInt(), any());
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

    assertThatThrownBy(() -> handler.write(region, Operation.DESTROY, new Object(), value))
        .isInstanceOf(SQLException.class);
  }

  @Test
  public void writesWithUnsupportedOperationThrows() throws Exception {
    assertThatThrownBy(() -> handler.write(region, Operation.INVALIDATE, new Object(), value))
        .isInstanceOf(InternalGemFireException.class);
  }

  @Test
  public void preparedStatementClearedAfterExecution() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    when(value.getFieldNames()).thenReturn(Collections.emptyList());

    handler.write(region, Operation.CREATE, new Object(), value);

    verify(statement).close();
  }

  @Test
  public void whenInsertFailsUpdateSucceeds() throws Exception {
    when(statement.executeUpdate()).thenReturn(0);
    PreparedStatement updateStatement = mock(PreparedStatement.class);
    when(updateStatement.executeUpdate()).thenReturn(1);
    when(connection.prepareStatement(any())).thenReturn(statement).thenReturn(updateStatement);
    when(value.getFieldNames()).thenReturn(Collections.emptyList());

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
    when(value.getFieldNames()).thenReturn(Collections.emptyList());
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
    when(value.getFieldNames()).thenReturn(Collections.emptyList());

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
    when(value.getFieldNames()).thenReturn(Collections.emptyList());

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
    when(value.getFieldNames()).thenReturn(Collections.emptyList());

    assertThatThrownBy(() -> handler.write(region, Operation.UPDATE, new Object(), value))
        .isInstanceOf(SQLException.class);

    verify(statement).close();
    verify(insertStatement).close();
  }

  @Test
  public void whenInsertFailsWithExceptionAndNonUpdateFirstExceptionIsThrown() throws Exception {
    when(statement.executeUpdate()).thenThrow(SQLException.class);
    PreparedStatement updateStatement = mock(PreparedStatement.class);
    when(updateStatement.executeUpdate()).thenReturn(0);
    when(connection.prepareStatement(any())).thenReturn(statement).thenReturn(updateStatement);
    when(value.getFieldNames()).thenReturn(Collections.emptyList());

    assertThatThrownBy(() -> handler.write(region, Operation.CREATE, new Object(), value))
        .isInstanceOf(SQLException.class);

    verify(statement).close();
    verify(updateStatement).close();
  }

  private void setupEmptyResultSet() throws SQLException {
    ResultSet result = mock(ResultSet.class);
    when(result.next()).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);
  }

  @Test
  public void returnsCorrectColumnForGet() throws Exception {
    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, key, value, Operation.GET);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).isEmpty();
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo(KEY_COLUMN);
  }

  @Test
  public void returnsCorrectColumnForGetGivenCompositeKey() throws Exception {
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    PdxInstance compositeKey = mock(PdxInstance.class);
    when(compositeKey.isDeserializable()).thenReturn(false);
    when(compositeKey.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(compositeKey.getField("fieldOne")).thenReturn(compositeKeyFieldValueOne);
    when(compositeKey.getField("fieldTwo")).thenReturn(compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn("fieldOne");
    when(fieldMapping1.getPdxName()).thenReturn("fieldOne");
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn("fieldTwo");
    when(fieldMapping2.getPdxName()).thenReturn("fieldTwo");
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    columnNames.clear();
    columnNames.add("fieldOne");
    columnNames.add("fieldTwo");
    when(tableMetaDataView.getColumnDataType("fieldOne")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwo")).thenReturn(JDBCType.VARCHAR);
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(fieldMapping1, fieldMapping2));
    when(regionMapping.getIds()).thenReturn("fieldOne,fieldTwo");
    createSqlHandler();

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, compositeKey, value, Operation.GET);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).isEmpty();
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(2);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo("fieldOne");
    assertThat(entryColumnData.getEntryKeyColumnData().get(1).getColumnName())
        .isEqualTo("fieldTwo");
  }

  @Test
  public void getEntryColumnDataGivenWrongNumberOfCompositeKeyFieldsFails() {
    PdxInstance compositeKey = mock(PdxInstance.class);
    when(compositeKey.isDeserializable()).thenReturn(false);
    when(compositeKey.getFieldNames()).thenReturn(Arrays.asList("fieldOne"));
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn("fieldOne");
    when(fieldMapping1.getPdxName()).thenReturn("fieldOne");
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn("fieldTwo");
    when(fieldMapping2.getPdxName()).thenReturn("fieldTwo");
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(fieldMapping1, fieldMapping2));
    columnNames.clear();
    columnNames.add("fieldOne");
    columnNames.add("fieldTwo");
    when(tableMetaDataView.getColumnDataType("fieldOne")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwo")).thenReturn(JDBCType.VARCHAR);
    when(regionMapping.getIds()).thenReturn("fieldOne,fieldTwo");
    createSqlHandler();

    assertThatThrownBy(
        () -> handler.getEntryColumnData(tableMetaDataView, compositeKey, value, Operation.GET))
            .isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
                "The key \"" + compositeKey + "\" should have 2 fields but has 1 fields.");
  }

  @Test
  public void getEntryColumnDataGivenWrongFieldNameInCompositeKeyFails() {
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    PdxInstance compositeKey = mock(PdxInstance.class);
    when(compositeKey.isDeserializable()).thenReturn(false);
    when(compositeKey.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwoWrong"));
    when(compositeKey.getField("fieldOne")).thenReturn(compositeKeyFieldValueOne);
    when(compositeKey.getField("fieldTwoWrong")).thenReturn(compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn("fieldOne");
    when(fieldMapping1.getPdxName()).thenReturn("fieldOne");
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn("fieldTwo");
    when(fieldMapping2.getPdxName()).thenReturn("fieldTwo");
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping3 = mock(FieldMapping.class);
    String nonKeyColumn = "fieldTwoWrong";
    when(fieldMapping3.getJdbcName()).thenReturn(nonKeyColumn);
    when(fieldMapping3.getPdxName()).thenReturn(nonKeyColumn);
    when(fieldMapping3.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    when(regionMapping.getFieldMappings())
        .thenReturn(Arrays.asList(fieldMapping1, fieldMapping2, fieldMapping3));
    columnNames.clear();
    columnNames.add("fieldOne");
    columnNames.add("fieldTwo");
    columnNames.add("fieldTwoWrong");
    when(tableMetaDataView.getColumnDataType("fieldOne")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwo")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwoWrong")).thenReturn(JDBCType.VARCHAR);
    when(regionMapping.getIds()).thenReturn("fieldOne,fieldTwo");
    createSqlHandler();

    assertThatThrownBy(() -> handler.getEntryColumnData(tableMetaDataView, compositeKey, value,
        Operation.GET)).isInstanceOf(JdbcConnectorException.class).hasMessageContaining("The key \""
            + compositeKey
            + "\" has the field \"fieldTwoWrong\" which does not match any of the key columns: [fieldOne, fieldTwo]");
  }

  @Test
  public void getEntryColumnDataGivenUnknownFieldNameInCompositeKeyFails() {
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    PdxInstance compositeKey = mock(PdxInstance.class);
    when(compositeKey.isDeserializable()).thenReturn(false);
    when(compositeKey.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwoUnknown"));
    when(compositeKey.getField("fieldOne")).thenReturn(compositeKeyFieldValueOne);
    when(compositeKey.getField("fieldTwoUnknown")).thenReturn(compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn("fieldOne");
    when(fieldMapping1.getPdxName()).thenReturn("fieldOne");
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn("fieldTwo");
    when(fieldMapping2.getPdxName()).thenReturn("fieldTwo");
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping3 = mock(FieldMapping.class);
    String nonKeyColumn = "fieldTwoWrong";
    when(fieldMapping3.getJdbcName()).thenReturn(nonKeyColumn);
    when(fieldMapping3.getPdxName()).thenReturn(nonKeyColumn);
    when(fieldMapping3.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    when(regionMapping.getFieldMappings())
        .thenReturn(Arrays.asList(fieldMapping1, fieldMapping2, fieldMapping3));
    columnNames.clear();
    columnNames.add("fieldOne");
    columnNames.add("fieldTwo");
    columnNames.add("fieldTwoWrong");
    when(tableMetaDataView.getColumnDataType("fieldOne")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwo")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwoWrong")).thenReturn(JDBCType.VARCHAR);
    when(regionMapping.getIds()).thenReturn("fieldOne,fieldTwo");
    createSqlHandler();

    assertThatThrownBy(
        () -> handler.getEntryColumnData(tableMetaDataView, compositeKey, value, Operation.GET))
            .isInstanceOf(JdbcConnectorException.class)
            .hasMessageContaining("The key \"" + compositeKey
                + "\" has the field \"fieldTwoUnknown\" which does not match any of the key columns: [fieldOne, fieldTwo]");
  }

  @Test
  public void returnsCorrectColumnsForUpdate() {
    testGetEntryColumnDataForCreateOrUpdate(Operation.UPDATE);
  }

  @Test
  public void returnsCorrectColumnsForCreate() {
    testGetEntryColumnDataForCreateOrUpdate(Operation.CREATE);
  }

  private void testGetEntryColumnDataForCreateOrUpdate(Operation operation) {
    String nonKeyColumn = "otherColumn";
    when(value.getFieldNames()).thenReturn(Arrays.asList(KEY_COLUMN, nonKeyColumn));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn(KEY_COLUMN);
    when(fieldMapping1.getPdxName()).thenReturn(KEY_COLUMN);
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn(nonKeyColumn);
    when(fieldMapping2.getPdxName()).thenReturn(nonKeyColumn);
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(fieldMapping1, fieldMapping2));
    columnNames.clear();
    columnNames.add(KEY_COLUMN);
    columnNames.add("otherColumn");
    when(tableMetaDataView.getColumnDataType("otherColumn")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType(KEY_COLUMN)).thenReturn(JDBCType.VARCHAR);
    createSqlHandler();

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, key, value, operation);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryValueColumnData().get(0).getColumnName())
        .isEqualTo(nonKeyColumn);
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo(KEY_COLUMN);
  }


  @Test
  public void returnsCorrectColumnsForUpdateWithExtraPdxField() {
    testGetEntryColumnDataForCreateOrUpdateWithExtraPdxField(Operation.UPDATE);
  }

  @Test
  public void returnsCorrectColumnsForCreateWithExtraPdxField() {
    testGetEntryColumnDataForCreateOrUpdateWithExtraPdxField(Operation.CREATE);
  }

  private void testGetEntryColumnDataForCreateOrUpdateWithExtraPdxField(Operation operation) {
    String nonKeyColumn = "otherColumn";
    when(value.getFieldNames()).thenReturn(Arrays.asList(KEY_COLUMN, nonKeyColumn, "extraField"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn(KEY_COLUMN);
    when(fieldMapping1.getPdxName()).thenReturn(KEY_COLUMN);
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn(nonKeyColumn);
    when(fieldMapping2.getPdxName()).thenReturn(nonKeyColumn);
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(fieldMapping1, fieldMapping2));
    columnNames.clear();
    columnNames.add(KEY_COLUMN);
    columnNames.add("otherColumn");
    when(tableMetaDataView.getColumnDataType("otherColumn")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType(KEY_COLUMN)).thenReturn(JDBCType.VARCHAR);
    createSqlHandler();

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, key, value, operation);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryValueColumnData().get(0).getColumnName())
        .isEqualTo(nonKeyColumn);
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo(KEY_COLUMN);
  }

  @Test
  public void returnsCorrectColumnsForUpdateWithCompositeKey() {
    testGetEntryColumnDataForCreateOrUpdateWithCompositeKey(Operation.UPDATE);
  }

  @Test
  public void returnsCorrectColumnsForCreateWithCompositeKey() {
    testGetEntryColumnDataForCreateOrUpdateWithCompositeKey(Operation.CREATE);
  }

  private void testGetEntryColumnDataForCreateOrUpdateWithCompositeKey(Operation operation) {
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    PdxInstance compositeKey = mock(PdxInstance.class);
    when(compositeKey.isDeserializable()).thenReturn(false);
    when(compositeKey.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(compositeKey.getField("fieldOne")).thenReturn(compositeKeyFieldValueOne);
    when(compositeKey.getField("fieldTwo")).thenReturn(compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn("fieldOne");
    when(fieldMapping1.getPdxName()).thenReturn("fieldOne");
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn("fieldTwo");
    when(fieldMapping2.getPdxName()).thenReturn("fieldTwo");
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping3 = mock(FieldMapping.class);
    String nonKeyColumn = "otherColumn";
    when(fieldMapping3.getJdbcName()).thenReturn(nonKeyColumn);
    when(fieldMapping3.getPdxName()).thenReturn(nonKeyColumn);
    when(fieldMapping3.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    columnNames.clear();
    columnNames.add("fieldOne");
    columnNames.add("fieldTwo");
    columnNames.add("otherColumn");
    when(tableMetaDataView.getColumnDataType("fieldOne")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwo")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("otherColumn")).thenReturn(JDBCType.VARCHAR);
    when(regionMapping.getIds()).thenReturn("fieldOne,fieldTwo");
    when(regionMapping.getFieldMappings())
        .thenReturn(Arrays.asList(fieldMapping1, fieldMapping2, fieldMapping3));
    createSqlHandler();
    when(value.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo", nonKeyColumn));
    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, compositeKey, value, operation);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryValueColumnData().get(0).getColumnName())
        .isEqualTo(nonKeyColumn);
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(2);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo("fieldOne");
    assertThat(entryColumnData.getEntryKeyColumnData().get(1).getColumnName())
        .isEqualTo("fieldTwo");
  }

  @Test
  public void returnsCorrectColumnForDestroyWithCompositeKey() {
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    PdxInstance compositeKey = mock(PdxInstance.class);
    when(compositeKey.isDeserializable()).thenReturn(false);
    when(compositeKey.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(compositeKey.getField("fieldOne")).thenReturn(compositeKeyFieldValueOne);
    when(compositeKey.getField("fieldTwo")).thenReturn(compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    FieldMapping fieldMapping1 = mock(FieldMapping.class);
    when(fieldMapping1.getJdbcName()).thenReturn("fieldOne");
    when(fieldMapping1.getPdxName()).thenReturn("fieldOne");
    when(fieldMapping1.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    FieldMapping fieldMapping2 = mock(FieldMapping.class);
    when(fieldMapping2.getJdbcName()).thenReturn("fieldTwo");
    when(fieldMapping2.getPdxName()).thenReturn("fieldTwo");
    when(fieldMapping2.getJdbcType()).thenReturn(JDBCType.VARCHAR.getName());
    columnNames.clear();
    columnNames.add("fieldOne");
    columnNames.add("fieldTwo");
    when(tableMetaDataView.getColumnDataType("fieldOne")).thenReturn(JDBCType.VARCHAR);
    when(tableMetaDataView.getColumnDataType("fieldTwo")).thenReturn(JDBCType.VARCHAR);
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(fieldMapping1, fieldMapping2));
    when(regionMapping.getIds()).thenReturn("fieldOne,fieldTwo");
    createSqlHandler();

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, compositeKey, value, Operation.DESTROY);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).isEmpty();
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(2);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo("fieldOne");
    assertThat(entryColumnData.getEntryKeyColumnData().get(1).getColumnName())
        .isEqualTo("fieldTwo");
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getValue())
        .isEqualTo(compositeKeyFieldValueOne);
    assertThat(entryColumnData.getEntryKeyColumnData().get(1).getValue())
        .isEqualTo(compositeKeyFieldValueTwo);
  }

  @Test
  public void returnsCorrectColumnForDestroy() {
    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, key, value, Operation.DESTROY);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).isEmpty();
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo(KEY_COLUMN);
  }

  @Test
  public void getEntryColumnDataWhenMultipleIdColumnsGivenNonPdxInstanceFails() {
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    Object nonCompositeKey = Integer.valueOf(123);

    assertThatThrownBy(() -> handler.getEntryColumnData(tableMetaDataView, nonCompositeKey, value,
        Operation.DESTROY)).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
            "The key \"123\" of class \"java.lang.Integer\" must be a PdxInstance because multiple columns are configured as ids.");
  }

  @Test
  public void getEntryColumnDataWhenMultipleIdColumnsGivenDeserializablePdxInstanceFails()
      throws Exception {
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    PdxInstance nonCompositeKey = mock(PdxInstance.class);
    when(nonCompositeKey.isDeserializable()).thenReturn(true);

    assertThatThrownBy(() -> handler.getEntryColumnData(tableMetaDataView, nonCompositeKey, value,
        Operation.DESTROY)).isInstanceOf(JdbcConnectorException.class)
            .hasMessageContaining("The key \"" + nonCompositeKey
                + "\" must be a PdxInstance created with PdxInstanceFactory.neverDeserialize");
  }

  @Test
  public void handlesSQLExceptionFromGetConnection() throws Exception {
    doThrow(new SQLException("test exception")).when(dataSource).getConnection();

    assertThatThrownBy(() -> handler.getConnection())
        .isInstanceOf(SQLException.class).hasMessage("test exception");
  }
}
