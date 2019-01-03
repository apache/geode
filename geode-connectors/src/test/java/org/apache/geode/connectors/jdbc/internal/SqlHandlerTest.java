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
import org.json.JSONObject;
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
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList(KEY_COLUMN));
    final String IDS = "ids";
    when(tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME, IDS))
        .thenReturn(tableMetaDataView);
    connectorService = mock(JdbcConnectorService.class);
    dataSourceFactory = mock(DataSourceFactory.class);
    when(dataSourceFactory.getDataSource(DATA_SOURCE_NAME)).thenReturn(dataSource);
    handler = new SqlHandler(tableMetaDataManager, connectorService, dataSourceFactory);
    key = "key";
    value = mock(PdxInstanceImpl.class);
    when(value.getPdxType()).thenReturn(mock(PdxType.class));

    regionMapping = mock(RegionMapping.class);
    when(regionMapping.getDataSourceName()).thenReturn(DATA_SOURCE_NAME);
    when(regionMapping.getRegionName()).thenReturn(REGION_NAME);
    when(regionMapping.getTableName()).thenReturn(TABLE_NAME);
    when(regionMapping.getIds()).thenReturn(IDS);
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
    Region region = mock(Region.class);
    when(region.getName()).thenReturn("myRegionName");
    thrown.expect(JdbcConnectorException.class);
    thrown.expectMessage(
        "JDBC mapping for region myRegionName not found. Create the mapping with the gfsh command 'create jdbc-mapping'.");
    handler.read(region, new Object());
  }

  @Test
  public void readThrowsIfNoConnectionConfig() throws Exception {
    @SuppressWarnings("unchecked")
    Region<Object, Object> region2 = mock(Region.class);
    when(region2.getName()).thenReturn("region2");
    RegionMapping regionMapping2 = mock(RegionMapping.class);
    when(regionMapping2.getDataSourceName()).thenReturn("bogus data source name");
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
  public void insertActionSucceedsWithCompositeKey() throws Exception {
    when(statement.executeUpdate()).thenReturn(1);
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    JSONObject compositeKey = new JSONObject();
    compositeKey.put("fieldOne", compositeKeyFieldValueOne);
    compositeKey.put("fieldTwo", compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(regionMapping.getColumnNameForField("fieldOne", tableMetaDataView)).thenReturn("fieldOne");
    when(regionMapping.getColumnNameForField("fieldTwo", tableMetaDataView)).thenReturn("fieldTwo");

    handler.write(region, Operation.CREATE, compositeKey.toString(), value);

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
    JSONObject compositeKey = new JSONObject();
    compositeKey.put("fieldOne", compositeKeyFieldValueOne);
    compositeKey.put("fieldTwo", compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(regionMapping.getColumnNameForField("fieldOne", tableMetaDataView)).thenReturn("fieldOne");
    when(regionMapping.getColumnNameForField("fieldTwo", tableMetaDataView)).thenReturn("fieldTwo");

    handler.write(region, Operation.UPDATE, compositeKey.toString(), value);

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
    JSONObject destroyKey = new JSONObject();
    destroyKey.put("fieldOne", destroyKeyFieldValueOne);
    destroyKey.put("fieldTwo", destroyKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(regionMapping.getColumnNameForField("fieldOne", tableMetaDataView)).thenReturn("fieldOne");
    when(regionMapping.getColumnNameForField("fieldTwo", tableMetaDataView)).thenReturn("fieldTwo");

    handler.write(region, Operation.DESTROY, destroyKey.toString(), value);

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
    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, regionMapping, key, value, Operation.GET);

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
    JSONObject compositeKey = new JSONObject();
    compositeKey.put("fieldOne", compositeKeyFieldValueOne);
    compositeKey.put("fieldTwo", compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(regionMapping.getColumnNameForField("fieldOne", tableMetaDataView)).thenReturn("fieldOne");
    when(regionMapping.getColumnNameForField("fieldTwo", tableMetaDataView)).thenReturn("fieldTwo");

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, regionMapping, compositeKey.toString(), value,
            Operation.GET);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).isEmpty();
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(2);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo("fieldOne");
    assertThat(entryColumnData.getEntryKeyColumnData().get(1).getColumnName())
        .isEqualTo("fieldTwo");
  }

  @Test
  public void getEntryColumnDataGivenWrongNumberOfCompositeKeyFieldsFails() throws Exception {
    Object compositeKeyFieldValueOne = "fieldValueOne";
    JSONObject compositeKey = new JSONObject();
    compositeKey.put("fieldOne", compositeKeyFieldValueOne);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(regionMapping.getColumnNameForField("fieldOne", tableMetaDataView)).thenReturn("fieldOne");
    when(regionMapping.getColumnNameForField("fieldTwo", tableMetaDataView)).thenReturn("fieldTwo");
    thrown.expect(JdbcConnectorException.class);
    thrown.expectMessage(
        "The key \"" + compositeKey.toString() + "\" should have 2 fields but has 1 fields.");

    handler.getEntryColumnData(tableMetaDataView, regionMapping, compositeKey.toString(), value,
        Operation.GET);
  }

  @Test
  public void getEntryColumnDataGivenWrongFieldNameInCompositeKeyFails() throws Exception {
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    JSONObject compositeKey = new JSONObject();
    compositeKey.put("fieldOne", compositeKeyFieldValueOne);
    compositeKey.put("fieldTwoWrong", compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(regionMapping.getColumnNameForField("fieldOne", tableMetaDataView)).thenReturn("fieldOne");
    when(regionMapping.getColumnNameForField("fieldTwo", tableMetaDataView)).thenReturn("fieldTwo");
    thrown.expect(JdbcConnectorException.class);
    thrown.expectMessage("The key \"" + compositeKey.toString()
        + "\" has the field \"fieldTwoWrong\" which does not match any of the key columns: [fieldOne, fieldTwo]");

    handler.getEntryColumnData(tableMetaDataView, regionMapping, compositeKey.toString(), value,
        Operation.GET);
  }

  @Test
  public void returnsCorrectColumnsForUpdate() throws Exception {
    testGetEntryColumnDataForCreateOrUpdate(Operation.UPDATE);
  }

  @Test
  public void returnsCorrectColumnsForCreate() throws Exception {
    testGetEntryColumnDataForCreateOrUpdate(Operation.CREATE);
  }

  private void testGetEntryColumnDataForCreateOrUpdate(Operation operation) {
    String nonKeyColumn = "otherColumn";
    when(regionMapping.getColumnNameForField(eq(KEY_COLUMN), any())).thenReturn(KEY_COLUMN);
    when(regionMapping.getColumnNameForField(eq(nonKeyColumn), any())).thenReturn(nonKeyColumn);
    when(value.getFieldNames()).thenReturn(Arrays.asList(KEY_COLUMN, nonKeyColumn));

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, regionMapping, key, value, operation);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryValueColumnData().get(0).getColumnName())
        .isEqualTo(nonKeyColumn);
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo(KEY_COLUMN);
  }

  @Test
  public void returnsCorrectColumnsForUpdateWithCompositeKey() throws Exception {
    testGetEntryColumnDataForCreateOrUpdateWithCompositeKey(Operation.UPDATE);
  }

  @Test
  public void returnsCorrectColumnsForCreateWithCompositeKey() throws Exception {
    testGetEntryColumnDataForCreateOrUpdateWithCompositeKey(Operation.CREATE);
  }

  private void testGetEntryColumnDataForCreateOrUpdateWithCompositeKey(Operation operation) {
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    JSONObject compositeKey = new JSONObject();
    compositeKey.put("fieldOne", compositeKeyFieldValueOne);
    compositeKey.put("fieldTwo", compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(regionMapping.getColumnNameForField("fieldOne", tableMetaDataView)).thenReturn("fieldOne");
    when(regionMapping.getColumnNameForField("fieldTwo", tableMetaDataView)).thenReturn("fieldTwo");
    String nonKeyColumn = "otherColumn";
    when(regionMapping.getColumnNameForField(eq(nonKeyColumn), any())).thenReturn(nonKeyColumn);
    when(value.getFieldNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo", nonKeyColumn));

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, regionMapping, compositeKey.toString(), value,
            operation);

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
  public void returnsCorrectColumnForDestroyWithCompositeKey() throws Exception {
    Object compositeKeyFieldValueOne = "fieldValueOne";
    Object compositeKeyFieldValueTwo = "fieldValueTwo";
    JSONObject compositeKey = new JSONObject();
    compositeKey.put("fieldOne", compositeKeyFieldValueOne);
    compositeKey.put("fieldTwo", compositeKeyFieldValueTwo);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    when(regionMapping.getColumnNameForField("fieldOne", tableMetaDataView)).thenReturn("fieldOne");
    when(regionMapping.getColumnNameForField("fieldTwo", tableMetaDataView)).thenReturn("fieldTwo");

    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, regionMapping, compositeKey.toString(), value,
            Operation.DESTROY);

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
  public void returnsCorrectColumnForDestroy() throws Exception {
    EntryColumnData entryColumnData =
        handler.getEntryColumnData(tableMetaDataView, regionMapping, key, value, Operation.DESTROY);

    assertThat(entryColumnData.getEntryKeyColumnData()).isNotNull();
    assertThat(entryColumnData.getEntryValueColumnData()).isEmpty();
    assertThat(entryColumnData.getEntryKeyColumnData()).hasSize(1);
    assertThat(entryColumnData.getEntryKeyColumnData().get(0).getColumnName())
        .isEqualTo(KEY_COLUMN);
  }

  @Test
  public void getEntryColumnDataWhenMultipleIdColumnsGivenNonStringFails() throws Exception {
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    Object nonCompositeKey = Integer.valueOf(123);
    thrown.expect(JdbcConnectorException.class);
    thrown.expectMessage(
        "The key \"123\" of class \"java.lang.Integer\" must be a java.lang.String because multiple columns are configured as ids.");

    handler.getEntryColumnData(tableMetaDataView, regionMapping, nonCompositeKey, value,
        Operation.DESTROY);
  }

  @Test
  public void getEntryColumnDataWhenMultipleIdColumnsGivenNonJsonStringFails() throws Exception {
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("fieldOne", "fieldTwo"));
    String nonJsonKey = "myKey";
    thrown.expect(JdbcConnectorException.class);
    thrown.expectMessage(
        "The key \"myKey\" must be a valid JSON string because multiple columns are configured as ids. Details: Value myKey of type java.lang.String cannot be converted to JSONObject");

    handler.getEntryColumnData(tableMetaDataView, regionMapping, nonJsonKey, value,
        Operation.DESTROY);
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
