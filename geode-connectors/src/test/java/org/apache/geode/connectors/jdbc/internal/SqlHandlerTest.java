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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SqlHandlerTest {
  private static final String REGION_NAME = "testRegion";
  private static final String TABLE_NAME = "testTable";
  private static final Object COLUMN_VALUE_1 = "columnValue1";
  private static final String COLUMN_NAME_1 = "columnName1";
  private static final Object COLUMN_VALUE_2 = "columnValue2";
  private static final String COLUMN_NAME_2 = "columnName2";
  private static final String KEY_COLUMN = "keyColumn";
  private static final String COLUMN_STRING_VALUE_1 = "columnStringValue1";
  private static final String PDX_FIELD_NAME_1 = COLUMN_NAME_1.toLowerCase();
  private static final String PDX_FIELD_NAME_2 = COLUMN_NAME_2.toLowerCase();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private DataSourceManager manager;
  private JdbcDataSource dataSource;
  private ConnectionConfiguration connectionConfig;
  private JdbcConnectorService connectorService;
  private TableKeyColumnManager tableKeyColumnManager;
  private Connection connection;
  private Region region;
  private InternalCache cache;
  private SqlHandler handler;
  private PreparedStatement statement;
  private RegionMapping regionMapping;
  private PdxInstanceImpl value;
  private Object key;

  @Before
  public void setup() throws Exception {
    manager = mock(DataSourceManager.class);
    dataSource = mock(JdbcDataSource.class);
    connectionConfig = mock(ConnectionConfiguration.class);
    when(connectionConfig.getUrl()).thenReturn("fake:url");
    region = mock(Region.class);
    cache = mock(InternalCache.class);
    connection = mock(Connection.class);
    when(region.getRegionService()).thenReturn(cache);
    tableKeyColumnManager = mock(TableKeyColumnManager.class);
    when(tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME)).thenReturn(KEY_COLUMN);
    connectorService = mock(JdbcConnectorService.class);
    handler = new SqlHandler(manager, tableKeyColumnManager, connectorService);
    key = "key";
    value = mock(PdxInstanceImpl.class);
    when(value.getPdxType()).thenReturn(mock(PdxType.class));

    when(connectorService.getConnectionConfig(any())).thenReturn(connectionConfig);

    regionMapping = mock(RegionMapping.class);
    when(regionMapping.getRegionName()).thenReturn(REGION_NAME);
    when(regionMapping.getTableName()).thenReturn(TABLE_NAME);
    when(regionMapping.getRegionToTableName()).thenReturn(TABLE_NAME);
    when(connectorService.getMappingForRegion(any())).thenReturn(regionMapping);


    when(manager.getDataSource(any())).thenReturn(this.dataSource);
    when(dataSource.getConnection()).thenReturn(this.connection);

    statement = mock(PreparedStatement.class);
    when(this.connection.prepareStatement(any())).thenReturn(statement);
  }

  @Test
  public void readReturnsNullIfNoKeyProvided() {
    thrown.expect(IllegalArgumentException.class);
    handler.read(region, null);
  }

  @Test
  public void usesPdxFactoryForClassWhenExists() throws Exception {
    setupEmptyResultSet();
    String pdxClassName = "classname";
    when(regionMapping.getPdxClassName()).thenReturn(pdxClassName);
    handler.read(region, new Object());

    verify(cache).createPdxInstanceFactory(pdxClassName);
    verifyNoMoreInteractions(cache);
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
  public void usesPdxFactoryForNoPdxClassWhenClassNonExistent() throws Exception {
    setupEmptyResultSet();
    handler.read(region, new Object());

    verify(cache).createPdxInstanceFactory("no class", false);
    verifyNoMoreInteractions(cache);
  }

  @Test
  public void readReturnsNullIfNoResultsReturned() throws Exception {
    setupEmptyResultSet();
    assertThat(handler.read(region, new Object())).isNull();
  }

  @Test
  public void throwsExceptionIfQueryFails() throws Exception {
    when(statement.executeQuery()).thenThrow(SQLException.class);

    thrown.expect(IllegalStateException.class);
    handler.read(region, new Object());
  }

  @Test
  public void readReturnsDataFromAllResultColumns() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);

    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean())).thenReturn(factory);

    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_1)).thenReturn(PDX_FIELD_NAME_1);
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_2)).thenReturn(PDX_FIELD_NAME_2);
    handler.read(region, new Object());
    verify(factory).writeObject(PDX_FIELD_NAME_1, COLUMN_VALUE_1);
    verify(factory).writeObject(PDX_FIELD_NAME_2, COLUMN_VALUE_2);
    verify(factory).create();
  }

  @Test
  public void readWritesStringFieldGivenPdxStringFieldType() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupStringResultSet(result);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);

    PdxInstanceFactory factory = setupPdxInstanceFactory(FieldType.STRING);

    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_1)).thenReturn(PDX_FIELD_NAME_1);
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_2)).thenReturn(PDX_FIELD_NAME_2);
    handler.read(region, new Object());
    verify(factory).writeString(PDX_FIELD_NAME_1, COLUMN_STRING_VALUE_1);
    verify(factory).writeObject(PDX_FIELD_NAME_2, COLUMN_VALUE_2);
    verify(factory).create();
  }

  private PdxInstanceFactory setupPdxInstanceFactory(FieldType fieldType) {
    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    String pdxClassName = "myPdxClassName";
    when(cache.createPdxInstanceFactory(pdxClassName)).thenReturn(factory);

    TypeRegistry pdxTypeRegistry = mock(TypeRegistry.class);
    when(cache.getPdxRegistry()).thenReturn(pdxTypeRegistry);
    PdxType pdxType = mock(PdxType.class);

    when(regionMapping.getPdxClassName()).thenReturn(pdxClassName);
    when(pdxTypeRegistry.getPdxTypeForField(PDX_FIELD_NAME_1, pdxClassName)).thenReturn(pdxType);
    PdxField pdxField = mock(PdxField.class);
    when(pdxType.getPdxField(PDX_FIELD_NAME_1)).thenReturn(pdxField);
    when(pdxField.getFieldType()).thenReturn(fieldType);

    return factory;
  }

  @Test
  public void readWritesObjectFieldGivenPdxTypeWithFieldMissing() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);

    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    String pdxClassName = "myPdxClassName";
    when(cache.createPdxInstanceFactory(pdxClassName)).thenReturn(factory);

    TypeRegistry pdxTypeRegistry = mock(TypeRegistry.class);
    when(cache.getPdxRegistry()).thenReturn(pdxTypeRegistry);
    PdxType pdxType = mock(PdxType.class);

    when(regionMapping.getPdxClassName()).thenReturn(pdxClassName);
    when(pdxTypeRegistry.getPdxTypeForField(PDX_FIELD_NAME_1, pdxClassName)).thenReturn(pdxType);
    when(pdxType.getPdxField(PDX_FIELD_NAME_1)).thenReturn(null);

    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_1)).thenReturn(PDX_FIELD_NAME_1);
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_2)).thenReturn(PDX_FIELD_NAME_2);
    handler.read(region, new Object());
    verify(factory).writeObject(PDX_FIELD_NAME_1, COLUMN_VALUE_1);
    verify(factory).writeObject(PDX_FIELD_NAME_2, COLUMN_VALUE_2);
    verify(factory).create();
  }

  @Test
  public void readResultOmitsKeyColumnIfNotInValue() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);
    when(tableKeyColumnManager.getKeyColumnName(connection, TABLE_NAME)).thenReturn(COLUMN_NAME_1);

    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean())).thenReturn(factory);

    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_2)).thenReturn(PDX_FIELD_NAME_2);
    handler.read(region, new Object());
    verify(factory).writeObject(PDX_FIELD_NAME_2, COLUMN_VALUE_2);
    verify(factory, times(1)).writeObject(any(), any());
    verify(factory).create();
  }

  @Test
  public void throwsExceptionIfMoreThatOneResultReturned() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result);
    when(result.next()).thenReturn(true);
    when(result.getStatement()).thenReturn(mock(PreparedStatement.class));
    when(statement.executeQuery()).thenReturn(result);

    // when(manager.getKeyColumnName(any(), anyString())).thenReturn("key");
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean()))
        .thenReturn(mock(PdxInstanceFactory.class));

    thrown.expect(IllegalStateException.class);
    handler.read(region, new Object());
  }

  @Test
  public void writeThrowsExceptionIfValueIsNullAndNotDoingDestroy() {
    thrown.expect(IllegalArgumentException.class);
    handler.write(region, Operation.UPDATE, new Object(), null);
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

    thrown.expect(IllegalStateException.class);
    handler.write(region, Operation.DESTROY, new Object(), value);
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

    thrown.expect(IllegalStateException.class);
    handler.write(region, Operation.UPDATE, new Object(), value);
    verify(statement).close();
    verify(insertStatement).close();
  }

  @Test
  public void whenStatementUpdatesMultipleRowsExceptionThrown() throws Exception {
    when(statement.executeUpdate()).thenReturn(2);
    thrown.expect(IllegalStateException.class);
    handler.write(region, Operation.CREATE, new Object(), value);
    verify(statement).close();
  }

  private void setupResultSet(ResultSet result, FieldType fieldType) throws SQLException {
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(result.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(2);
    switch (fieldType) {
      case STRING:
        when(result.getString(1)).thenReturn(COLUMN_STRING_VALUE_1);
        break;
      case OBJECT:
        when(result.getObject(1)).thenReturn(COLUMN_VALUE_1);
        break;
      default:
        throw new IllegalStateException("unhandled fieldType " + fieldType);
    }
    when(metaData.getColumnName(1)).thenReturn(COLUMN_NAME_1);

    when(result.getObject(2)).thenReturn(COLUMN_VALUE_2);
    when(metaData.getColumnName(2)).thenReturn(COLUMN_NAME_2);
  }

  private void setupResultSet(ResultSet result) throws SQLException {
    setupResultSet(result, FieldType.OBJECT);
  }

  private void setupStringResultSet(ResultSet result) throws SQLException {
    setupResultSet(result, FieldType.STRING);
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

    List<ColumnValue> columnValueList =
        handler.getColumnToValueList(connection, regionMapping, key, value, Operation.GET);

    assertThat(columnValueList).hasSize(1);
    assertThat(columnValueList.get(0).getColumnName()).isEqualTo(KEY_COLUMN);
  }

  @Test
  public void returnsCorrectColumnsForUpsertOperations() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    String nonKeyColumn = "otherColumn";
    when(regionMapping.getColumnNameForField(KEY_COLUMN)).thenReturn(KEY_COLUMN);
    when(regionMapping.getColumnNameForField(nonKeyColumn)).thenReturn(nonKeyColumn);
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);
    when(value.getFieldNames()).thenReturn(Arrays.asList(KEY_COLUMN, nonKeyColumn));

    List<ColumnValue> columnValueList =
        handler.getColumnToValueList(connection, regionMapping, key, value, Operation.UPDATE);

    assertThat(columnValueList).hasSize(2);
    assertThat(columnValueList.get(0).getColumnName()).isEqualTo(nonKeyColumn);
    assertThat(columnValueList.get(1).getColumnName()).isEqualTo(KEY_COLUMN);
  }

  @Test
  public void returnsCorrectColumnForDestroy() throws Exception {
    ResultSet primaryKeys = getPrimaryKeysMetaData();
    when(primaryKeys.next()).thenReturn(true).thenReturn(false);

    List<ColumnValue> columnValueList =
        handler.getColumnToValueList(connection, regionMapping, key, value, Operation.DESTROY);

    assertThat(columnValueList).hasSize(1);
    assertThat(columnValueList.get(0).getColumnName()).isEqualTo(KEY_COLUMN);
  }

  @Test
  public void usesMappedPdxFieldNameWhenReading() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);

    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean())).thenReturn(factory);

    List<ColumnValue> columnList = new ArrayList<>();

    String fieldName1 = "pdxFieldName1";
    String fieldName2 = "pdxFieldName2";
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_1)).thenReturn(fieldName1);
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_2)).thenReturn(fieldName2);
    handler.executeReadStatement(region, statement, columnList, regionMapping, "keyColumn");
    verify(factory).writeObject(fieldName1, COLUMN_VALUE_1);
    verify(factory).writeObject(fieldName2, COLUMN_VALUE_2);
    verify(factory).create();
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

    assertThatThrownBy(() -> handler.getConnection(connectionConfig))
        .isInstanceOf(IllegalStateException.class).hasMessage("Could not connect to fake:url");
  }


}
