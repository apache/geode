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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.junit.categories.UnitTest;

@RunWith(JUnitParamsRunner.class)
@Category(UnitTest.class)
public class SqlToPdxInstanceCreatorTest {

  private static final String COLUMN_NAME_1 = "columnName1";
  private static final Object COLUMN_VALUE_1 = "columnValue1";
  private static final Object COLUMN_VALUE_2 = "columnValue2";
  private static final String COLUMN_NAME_2 = "columnName2";
  private static final String KEY_COLUMN = "keyColumn";
  private static final String PDX_FIELD_NAME_1 = COLUMN_NAME_1.toLowerCase();
  private static final String PDX_FIELD_NAME_2 = COLUMN_NAME_2.toLowerCase();

  private InternalCache cache;
  private SqlToPdxInstanceCreator sqlToPdxInstanceCreator;
  private RegionMapping regionMapping;
  private ResultSet resultSet;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    cache = mock(InternalCache.class);
    regionMapping = mock(RegionMapping.class);
    resultSet = mock(ResultSet.class);
    sqlToPdxInstanceCreator =
        new SqlToPdxInstanceCreator(cache, regionMapping, resultSet, KEY_COLUMN);
  }

  @Test
  public void usesPdxFactoryForClassWhenExists() throws Exception {
    String pdxClassName = "classname";
    when(regionMapping.getPdxClassName()).thenReturn(pdxClassName);
    when(resultSet.next()).thenReturn(false);

    sqlToPdxInstanceCreator.create();

    verify(cache).createPdxInstanceFactory(pdxClassName);
    verifyNoMoreInteractions(cache);
  }

  @Test
  public void usesPdxFactoryForNoPdxClassWhenClassNonExistent() throws Exception {
    when(resultSet.next()).thenReturn(false);

    sqlToPdxInstanceCreator.create();

    verify(cache).createPdxInstanceFactory("no class", false);
    verifyNoMoreInteractions(cache);
  }

  @Test
  public void readReturnsNullIfNoResultsReturned() throws Exception {
    when(resultSet.next()).thenReturn(false);
    PdxInstance pdxInstance = sqlToPdxInstanceCreator.create();
    assertThat(pdxInstance).isNull();
  }

  @Test
  public void readResultOmitsKeyColumnIfNotInValue() throws Exception {
    setupResultSetForTwoObjectColumns(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean())).thenReturn(factory);
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_2), any()))
        .thenReturn(PDX_FIELD_NAME_2);
    sqlToPdxInstanceCreator =
        new SqlToPdxInstanceCreator(cache, regionMapping, resultSet, COLUMN_NAME_1);

    sqlToPdxInstanceCreator.create();

    verify(factory).writeObject(PDX_FIELD_NAME_2, COLUMN_VALUE_2);
    verify(factory, times(1)).writeObject(any(), any());
    verify(factory).create();
  }

  @Test
  public void readReturnsDataFromAllResultColumns() throws Exception {
    setupResultSetForTwoObjectColumns(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean())).thenReturn(factory);
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_1), any()))
        .thenReturn(PDX_FIELD_NAME_1);
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_2), any()))
        .thenReturn(PDX_FIELD_NAME_2);

    sqlToPdxInstanceCreator.create();

    verify(factory).writeObject(PDX_FIELD_NAME_1, COLUMN_VALUE_1);
    verify(factory).writeObject(PDX_FIELD_NAME_2, COLUMN_VALUE_2);
    verify(factory).create();
  }

  @Test
  public void usesMappedPdxFieldNameWhenReading() throws Exception {
    setupResultSetForTwoObjectColumns(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean())).thenReturn(factory);
    String fieldName1 = "pdxFieldName1";
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_1), any())).thenReturn(fieldName1);

    sqlToPdxInstanceCreator.create();

    verify(factory).writeObject(fieldName1, COLUMN_VALUE_1);
    verify(factory).create();
  }

  @Test
  @Parameters(source = FieldType.class)
  public void readWritesFieldGivenPdxFieldType(FieldType fieldType) throws Exception {
    setupResultSet(resultSet, fieldType);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    PdxInstanceFactory factory = setupPdxInstanceFactory(fieldType);
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_1), any()))
        .thenReturn(PDX_FIELD_NAME_1);

    sqlToPdxInstanceCreator.create();

    verifyPdxFactoryWrite(factory, fieldType);
    verify(factory).create();
  }

  @Test
  @Parameters(source = FieldType.class)
  public void readOfNullWritesFieldGivenPdxFieldType(FieldType fieldType) throws Exception {
    setupResultSet(resultSet, fieldType, null);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    PdxInstanceFactory factory = setupPdxInstanceFactory(fieldType);
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_1), any()))
        .thenReturn(PDX_FIELD_NAME_1);

    sqlToPdxInstanceCreator.create();

    verifyPdxFactoryWrite(factory, fieldType, null);
    verify(factory).create();
  }

  @Test
  public void readOfCharFieldWithEmptyStringWritesCharZero() throws Exception {
    FieldType fieldType = FieldType.CHAR;
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(1);
    when(metaData.getColumnName(1)).thenReturn(COLUMN_NAME_1);
    when(resultSet.getString(1)).thenReturn("");
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    PdxInstanceFactory factory = setupPdxInstanceFactory(fieldType);
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_1), any()))
        .thenReturn(PDX_FIELD_NAME_1);

    sqlToPdxInstanceCreator.create();

    char expectedValue = 0;
    verifyPdxFactoryWrite(factory, fieldType, expectedValue);
    verify(factory).create();
  }

  @Test
  @Parameters({"BOOLEAN_ARRAY", "OBJECT_ARRAY", "CHAR_ARRAY", "SHORT_ARRAY", "INT_ARRAY",
      "LONG_ARRAY", "FLOAT_ARRAY", "DOUBLE_ARRAY", "STRING_ARRAY", "ARRAY_OF_BYTE_ARRAYS"})
  public void throwsExceptionWhenReadWritesUnsupportedType(FieldType fieldType) throws Exception {
    String returnValue = "ReturnValue";
    setupPdxInstanceFactory(fieldType);
    setupResultSetForObject(resultSet, returnValue);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_1), any()))
        .thenReturn(PDX_FIELD_NAME_1);
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_2), any()))
        .thenReturn(PDX_FIELD_NAME_2);

    thrown.expect(JdbcConnectorException.class);
    thrown.expectMessage("Could not convert ");
    sqlToPdxInstanceCreator.create();
  }

  @Test
  public void throwsExceptionIfMoreThanOneResultReturned() throws Exception {
    setupResultSetForTwoObjectColumns(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getStatement()).thenReturn(mock(PreparedStatement.class));
    when(cache.createPdxInstanceFactory(anyString(), anyBoolean()))
        .thenReturn(mock(PdxInstanceFactory.class));

    thrown.expect(JdbcConnectorException.class);
    sqlToPdxInstanceCreator.create();
  }

  @Test
  public void readThrowsGivenPdxTypeWithFieldMissing() throws Exception {
    setupResultSet(resultSet, FieldType.OBJECT);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    String pdxClassName = "myPdxClassName";
    when(cache.createPdxInstanceFactory(pdxClassName)).thenReturn(factory);
    TypeRegistry pdxTypeRegistry = mock(TypeRegistry.class);
    when(cache.getPdxRegistry()).thenReturn(pdxTypeRegistry);
    PdxType pdxType = mock(PdxType.class);
    when(regionMapping.getPdxClassName()).thenReturn(pdxClassName);
    when(pdxTypeRegistry.getPdxTypeForField(PDX_FIELD_NAME_1, pdxClassName)).thenReturn(pdxType);
    when(pdxType.getPdxField(PDX_FIELD_NAME_1)).thenReturn(null);
    when(regionMapping.getFieldNameForColumn(eq(COLUMN_NAME_1), any()))
        .thenReturn(PDX_FIELD_NAME_1);

    thrown.expect(JdbcConnectorException.class);
    thrown.expectMessage("Could not find PdxType");
    sqlToPdxInstanceCreator.create();
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

}
