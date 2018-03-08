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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.junit.categories.UnitTest;

@RunWith(JUnitParamsRunner.class)
@Category(UnitTest.class)
public class SqlHandlerTest {
  private static final String CONNECTION_CONFIG_NAME = "testConnectionConfig";
  private static final String REGION_NAME = "testRegion";
  private static final String TABLE_NAME = "testTable";
  private static final String COLUMN_NAME_1 = "columnName1";
  private static final Object COLUMN_VALUE_1 = "columnValue1";
  private static final Object COLUMN_VALUE_2 = "columnValue2";
  private static final String COLUMN_NAME_2 = "columnName2";
  private static final String KEY_COLUMN = "keyColumn";
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
    when(connectionConfig.getName()).thenReturn(CONNECTION_CONFIG_NAME);
    when(connectionConfig.getUrl()).thenReturn("fake:url");
    region = mock(Region.class);
    when(region.getName()).thenReturn(REGION_NAME);
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

    when(connectorService.getConnectionConfig(CONNECTION_CONFIG_NAME)).thenReturn(connectionConfig);

    regionMapping = mock(RegionMapping.class);
    when(regionMapping.getConnectionConfigName()).thenReturn(CONNECTION_CONFIG_NAME);
    when(regionMapping.getRegionName()).thenReturn(REGION_NAME);
    when(regionMapping.getTableName()).thenReturn(TABLE_NAME);
    when(regionMapping.getRegionToTableName()).thenReturn(TABLE_NAME);
    when(connectorService.getMappingForRegion(REGION_NAME)).thenReturn(regionMapping);


    when(manager.getDataSource(any())).thenReturn(this.dataSource);
    when(dataSource.getConnection()).thenReturn(this.connection);

    statement = mock(PreparedStatement.class);
    when(this.connection.prepareStatement(any())).thenReturn(statement);
  }

  @Test
  public void verifyCloseCallsManagerClose() {
    handler.close();

    verify(manager).close();
  }

  @Test
  public void readThrowsIfNoKeyProvided() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    handler.read(region, null);
  }

  @Test
  public void readThrowsIfNoMapping() throws Exception {
    thrown.expect(JdbcConnectorException.class);
    handler.read(mock(Region.class), new Object());
  }

  @Test
  public void readThrowsIfNoConnectionConfig() throws Exception {
    Region region2 = mock(Region.class);
    when(region2.getName()).thenReturn("region2");
    RegionMapping regionMapping2 = mock(RegionMapping.class);
    when(regionMapping2.getConnectionConfigName()).thenReturn("bogus connection name");
    when(regionMapping2.getRegionName()).thenReturn("region2");
    when(connectorService.getMappingForRegion("region2")).thenReturn(regionMapping2);

    thrown.expect(JdbcConnectorException.class);
    handler.read(region2, new Object());
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

    thrown.expect(SQLException.class);
    handler.read(region, new Object());
  }

  @Test
  public void readReturnsDataFromAllResultColumns() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSetForTwoObjectColumns(result);
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
  @Parameters(source = FieldType.class)
  public void readWritesFieldGivenPdxFieldType(FieldType fieldType) throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result, fieldType);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);
    PdxInstanceFactory factory = setupPdxInstanceFactory(fieldType);
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_1)).thenReturn(PDX_FIELD_NAME_1);

    handler.read(region, new Object());

    verifyPdxFactoryWrite(factory, fieldType);
    verify(factory).create();
  }

  @Test
  @Parameters(source = FieldType.class)
  public void readOfNullWritesFieldGivenPdxFieldType(FieldType fieldType) throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result, fieldType, null);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);
    PdxInstanceFactory factory = setupPdxInstanceFactory(fieldType);
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_1)).thenReturn(PDX_FIELD_NAME_1);

    handler.read(region, new Object());

    verifyPdxFactoryWrite(factory, fieldType, null);
    verify(factory).create();
  }

  @Test
  public void readOfCharFieldWithEmptyStringWritesCharZero() throws Exception {
    FieldType fieldType = FieldType.CHAR;
    ResultSet result = mock(ResultSet.class);
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(result.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(1);
    when(metaData.getColumnName(1)).thenReturn(COLUMN_NAME_1);
    when(result.getString(1)).thenReturn("");
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);
    PdxInstanceFactory factory = setupPdxInstanceFactory(fieldType);
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_1)).thenReturn(PDX_FIELD_NAME_1);

    handler.read(region, new Object());

    char expectedValue = 0;
    verifyPdxFactoryWrite(factory, fieldType, expectedValue);
    verify(factory).create();
  }

  @Test
  @Parameters({"BOOLEAN_ARRAY", "OBJECT_ARRAY", "CHAR_ARRAY", "SHORT_ARRAY", "INT_ARRAY",
      "LONG_ARRAY", "FLOAT_ARRAY", "DOUBLE_ARRAY", "STRING_ARRAY", "ARRAY_OF_BYTE_ARRAYS"})
  public void throwsExceptionWhenReadWritesUnsupportedType(FieldType fieldType) throws Exception {
    ResultSet result = mock(ResultSet.class);
    String returnValue = "ReturnValue";
    setupResultSetForObject(result, returnValue);
    when(result.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(result);

    PdxInstanceFactory factory = setupPdxInstanceFactory(fieldType);

    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_1)).thenReturn(PDX_FIELD_NAME_1);
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_2)).thenReturn(PDX_FIELD_NAME_2);
    thrown.expect(JdbcConnectorException.class);
    thrown.expectMessage("Could not convert ");
    handler.read(region, new Object());
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
  public void readThrowsGivenPdxTypeWithFieldMissing() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSet(result, FieldType.OBJECT);
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


    thrown.expect(JdbcConnectorException.class);
    thrown.expectMessage("Could not find PdxType");
    handler.read(region, new Object());
  }

  @Test
  public void readResultOmitsKeyColumnIfNotInValue() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSetForTwoObjectColumns(result);
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
  public void throwsExceptionIfMoreThanOneResultReturned() throws Exception {
    ResultSet result = mock(ResultSet.class);
    setupResultSetForTwoObjectColumns(result);
    when(result.next()).thenReturn(true);
    when(result.getStatement()).thenReturn(mock(PreparedStatement.class));
    when(statement.executeQuery()).thenReturn(result);

    when(cache.createPdxInstanceFactory(anyString(), anyBoolean()))
        .thenReturn(mock(PdxInstanceFactory.class));

    thrown.expect(JdbcConnectorException.class);
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
    when(regionMapping.getColumnNameForField(fieldName)).thenReturn(fieldName);
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
  public void writeWithNonCharField() throws Exception {
    String fieldName = "fieldName";
    int fieldValue = 100;
    when(regionMapping.getColumnNameForField(fieldName)).thenReturn(fieldName);
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

  private static byte[][] arrayOfByteArray = new byte[][] {{1, 2}, {3, 4}};

  private <T> T getValueByFieldType(FieldType fieldType) {
    switch (fieldType) {
      case STRING:
        return (T) "stringValue";
      case CHAR:
        return (T) Character.valueOf('A');
      case SHORT:
        return (T) Short.valueOf((short) 36);
      case INT:
        return (T) Integer.valueOf(36);
      case LONG:
        return (T) Long.valueOf(36);
      case FLOAT:
        return (T) Float.valueOf(36);
      case DOUBLE:
        return (T) Double.valueOf(36);
      case BYTE:
        return (T) Byte.valueOf((byte) 36);
      case BOOLEAN:
        return (T) Boolean.TRUE;
      case DATE:
        return (T) new Date(1000);
      case BYTE_ARRAY:
        return (T) new byte[] {1, 2};
      case BOOLEAN_ARRAY:
        return (T) new boolean[] {true, false};
      case CHAR_ARRAY:
        return (T) new char[] {1, 2};
      case SHORT_ARRAY:
        return (T) new short[] {1, 2};
      case INT_ARRAY:
        return (T) new int[] {1, 2};
      case LONG_ARRAY:
        return (T) new long[] {1, 2};
      case FLOAT_ARRAY:
        return (T) new float[] {1, 2};
      case DOUBLE_ARRAY:
        return (T) new double[] {1, 2};
      case STRING_ARRAY:
        return (T) new String[] {"1", "2"};
      case OBJECT_ARRAY:
        return (T) new Object[] {1, 2};
      case ARRAY_OF_BYTE_ARRAYS:
        return (T) arrayOfByteArray;
      case OBJECT:
        return (T) "objectValue";
      default:
        throw new IllegalStateException("unhandled fieldType " + fieldType);
    }
  }

  private void verifyPdxFactoryWrite(PdxInstanceFactory factory, FieldType fieldType) {
    verifyPdxFactoryWrite(factory, fieldType, getValueByFieldType(fieldType));
  }

  private void verifyPdxFactoryWrite(PdxInstanceFactory factory, FieldType fieldType,
      Object value) {
    switch (fieldType) {
      case STRING:
        verify(factory).writeString(PDX_FIELD_NAME_1, (String) value);
        break;
      case CHAR:
        verify(factory).writeChar(PDX_FIELD_NAME_1, value == null ? 0 : (char) value);
        break;
      case SHORT:
        verify(factory).writeShort(PDX_FIELD_NAME_1, value == null ? 0 : (short) value);
        break;
      case INT:
        verify(factory).writeInt(PDX_FIELD_NAME_1, value == null ? 0 : (int) value);
        break;
      case LONG:
        verify(factory).writeLong(PDX_FIELD_NAME_1, value == null ? 0 : (long) value);
        break;
      case FLOAT:
        verify(factory).writeFloat(PDX_FIELD_NAME_1, value == null ? 0 : (float) value);
        break;
      case DOUBLE:
        verify(factory).writeDouble(PDX_FIELD_NAME_1, value == null ? 0 : (double) value);
        break;
      case BYTE:
        verify(factory).writeByte(PDX_FIELD_NAME_1, value == null ? 0 : (byte) value);
        break;
      case BOOLEAN:
        verify(factory).writeBoolean(PDX_FIELD_NAME_1, value == null ? false : (boolean) value);
        break;
      case DATE:
        verify(factory).writeDate(PDX_FIELD_NAME_1, (Date) value);
        break;
      case BYTE_ARRAY:
        verify(factory).writeByteArray(PDX_FIELD_NAME_1, (byte[]) value);
        break;
      case BOOLEAN_ARRAY:
        verify(factory).writeBooleanArray(PDX_FIELD_NAME_1, (boolean[]) value);
        break;
      case CHAR_ARRAY:
        verify(factory).writeCharArray(PDX_FIELD_NAME_1, (char[]) value);
        break;
      case SHORT_ARRAY:
        verify(factory).writeShortArray(PDX_FIELD_NAME_1, (short[]) value);
        break;
      case INT_ARRAY:
        verify(factory).writeIntArray(PDX_FIELD_NAME_1, (int[]) value);
        break;
      case LONG_ARRAY:
        verify(factory).writeLongArray(PDX_FIELD_NAME_1, (long[]) value);
        break;
      case FLOAT_ARRAY:
        verify(factory).writeFloatArray(PDX_FIELD_NAME_1, (float[]) value);
        break;
      case DOUBLE_ARRAY:
        verify(factory).writeDoubleArray(PDX_FIELD_NAME_1, (double[]) value);
        break;
      case STRING_ARRAY:
        verify(factory).writeStringArray(PDX_FIELD_NAME_1, (String[]) value);
        break;
      case OBJECT_ARRAY:
        verify(factory).writeObjectArray(PDX_FIELD_NAME_1, (Object[]) value);
        break;
      case ARRAY_OF_BYTE_ARRAYS:
        verify(factory).writeArrayOfByteArrays(PDX_FIELD_NAME_1, (byte[][]) value);
        break;
      case OBJECT:
        verify(factory).writeObject(PDX_FIELD_NAME_1, value);
        break;
      default:
        throw new IllegalStateException("unhandled fieldType " + fieldType);
    }
  }

  private void setupResultSetForObject(ResultSet result, Object objectToReturn)
      throws SQLException {
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(result.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(2);
    when(result.getObject(1)).thenReturn(objectToReturn);
    when(metaData.getColumnName(1)).thenReturn(COLUMN_NAME_1);

    when(result.getObject(2)).thenReturn(COLUMN_VALUE_2);
    when(metaData.getColumnName(2)).thenReturn(COLUMN_NAME_2);

  }


  private void setupResultSetForTwoObjectColumns(ResultSet result) throws SQLException {
    setupResultSet(result, FieldType.OBJECT);
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(result.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(2);
    when(metaData.getColumnName(1)).thenReturn(COLUMN_NAME_1);
    when(metaData.getColumnName(2)).thenReturn(COLUMN_NAME_2);
    when(result.getObject(1)).thenReturn(COLUMN_VALUE_1);
    when(result.getObject(2)).thenReturn(COLUMN_VALUE_2);
  }

  private void setupResultSet(ResultSet result, FieldType fieldType) throws SQLException {
    setupResultSet(result, fieldType, getValueByFieldType(fieldType));
  }

  private void setupResultSet(ResultSet result, FieldType fieldType, Object value)
      throws SQLException {
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(result.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(1);
    when(metaData.getColumnName(1)).thenReturn(COLUMN_NAME_1);

    switch (fieldType) {
      case STRING:
        when(result.getString(1)).thenReturn((String) value);
        break;
      case CHAR:
        Character charValue = (Character) value;
        when(result.getString(1)).thenReturn(value == null ? null : charValue.toString());
        break;
      case SHORT:
        when(result.getShort(1)).thenReturn(value == null ? 0 : (Short) value);
        break;
      case INT:
        when(result.getInt(1)).thenReturn(value == null ? 0 : (Integer) value);
        break;
      case LONG:
        when(result.getLong(1)).thenReturn(value == null ? 0 : (Long) value);
        break;
      case FLOAT:
        when(result.getFloat(1)).thenReturn(value == null ? 0 : (Float) value);
        break;
      case DOUBLE:
        when(result.getDouble(1)).thenReturn(value == null ? 0 : (Double) value);
        break;
      case BYTE:
        when(result.getByte(1)).thenReturn(value == null ? 0 : (Byte) value);
        break;
      case BOOLEAN:
        when(result.getBoolean(1)).thenReturn(value == null ? false : (Boolean) value);
        break;
      case DATE:
        Date date = (Date) value;
        java.sql.Timestamp sqlTimeStamp = null;
        if (date != null) {
          sqlTimeStamp = new java.sql.Timestamp(date.getTime());
        }
        when(result.getTimestamp(1)).thenReturn(sqlTimeStamp);
        break;
      case BYTE_ARRAY:
        when(result.getBytes(1)).thenReturn((byte[]) value);
        break;
      case BOOLEAN_ARRAY:
        when(result.getObject(1)).thenReturn(value);
        break;
      case CHAR_ARRAY:
        when(result.getObject(1)).thenReturn(value);
        break;
      case SHORT_ARRAY:
        when(result.getObject(1)).thenReturn(value);
        break;
      case INT_ARRAY:
        when(result.getObject(1)).thenReturn(value);
        break;
      case LONG_ARRAY:
        when(result.getObject(1)).thenReturn(value);
        break;
      case FLOAT_ARRAY:
        when(result.getObject(1)).thenReturn(value);
        break;
      case DOUBLE_ARRAY:
        when(result.getObject(1)).thenReturn(value);
        break;
      case STRING_ARRAY:
        when(result.getObject(1)).thenReturn(value);
        break;
      case OBJECT_ARRAY:
        when(result.getObject(1)).thenReturn(value);
        break;
      case ARRAY_OF_BYTE_ARRAYS:
        when(result.getObject(1)).thenReturn(value);
        break;
      case OBJECT:
        when(result.getObject(1)).thenReturn(value);
        break;
      default:
        throw new IllegalStateException("unhandled fieldType " + fieldType);
    }

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
    ResultSet resultSet = mock(ResultSet.class);
    setupResultSetForTwoObjectColumns(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery()).thenReturn(resultSet);
    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean())).thenReturn(factory);
    String fieldName1 = "pdxFieldName1";
    when(regionMapping.getFieldNameForColumn(COLUMN_NAME_1)).thenReturn(fieldName1);

    handler.createPdxInstance(resultSet, region, regionMapping, "keyColumn");

    verify(factory).writeObject(fieldName1, COLUMN_VALUE_1);
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
        .isInstanceOf(SQLException.class).hasMessage("test exception");
  }


}
