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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.WritablePdxInstance;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class SqlToPdxInstanceTest {

  private static final String COLUMN_NAME_1 = "columnName1";
  private static final String COLUMN_NAME_2 = "columnName2";
  private static final String PDX_FIELD_NAME_1 = "pdxFieldName1";
  private static final String PDX_FIELD_NAME_2 = "pdxFieldName2";

  private SqlToPdxInstance sqlToPdxInstance;
  private final WritablePdxInstance writablePdxInstance = mock(WritablePdxInstance.class);

  private ResultSet resultSet;
  private final ResultSetMetaData metaData = mock(ResultSetMetaData.class);

  @Before
  public void setup() throws Exception {
    resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(1);
    when(metaData.getColumnName(1)).thenReturn(COLUMN_NAME_1);
    sqlToPdxInstance = new SqlToPdxInstance();
    PdxInstance pdxTemplate = mock(PdxInstance.class);
    when(pdxTemplate.createWriter()).thenReturn(writablePdxInstance);
    sqlToPdxInstance.setPdxTemplate(pdxTemplate);
  }

  @Test
  @Parameters(source = FieldType.class)
  public void createPdxInstanceFromSqlDataTest(FieldType fieldType) throws SQLException {
    int columnIndex = 1;
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);
    Object columnValue = setupGetFieldValueResultSet(fieldType, columnIndex);
    Object expectedFieldValue = getFieldValueFromColumnValue(columnValue, fieldType);

    PdxInstance result = sqlToPdxInstance.create(resultSet);

    assertThat(result).isSameAs(writablePdxInstance);
    verify((WritablePdxInstance) result).setField(PDX_FIELD_NAME_1, expectedFieldValue);
    verifyNoMoreInteractions(result);
  }

  @Test
  public void createReturnsNullIfNoResultsReturned() throws Exception {
    when(resultSet.next()).thenReturn(false);

    PdxInstance pdxInstance = createPdxInstance();

    assertThat(pdxInstance).isNull();
  }

  @Test
  public void readReturnsDataFromAllResultColumns() throws Exception {
    when(metaData.getColumnCount()).thenReturn(2);
    when(metaData.getColumnName(2)).thenReturn(COLUMN_NAME_2);
    when(resultSet.getString(1)).thenReturn("column1");
    when(resultSet.getString(2)).thenReturn("column2");
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, FieldType.STRING);
    sqlToPdxInstance.addMapping(COLUMN_NAME_2, PDX_FIELD_NAME_2, FieldType.STRING);

    PdxInstance result = createPdxInstance();

    assertThat(result).isSameAs(writablePdxInstance);
    verify((WritablePdxInstance) result).setField(PDX_FIELD_NAME_1, "column1");
    verify((WritablePdxInstance) result).setField(PDX_FIELD_NAME_2, "column2");
    verifyNoMoreInteractions(result);
  }

  @Test
  public void skipsUnmappedColumns() throws Exception {
    when(metaData.getColumnCount()).thenReturn(2);
    when(metaData.getColumnName(2)).thenReturn(COLUMN_NAME_2);
    when(resultSet.getString(1)).thenReturn("column1");
    when(resultSet.getString(2)).thenReturn("column2");
    sqlToPdxInstance.addMapping(COLUMN_NAME_2, PDX_FIELD_NAME_2, FieldType.STRING);

    assertThatThrownBy(this::createPdxInstance).isInstanceOf(JdbcConnectorException.class)
        .hasMessageContaining("The jdbc-mapping does not contain the column name \""
            + COLUMN_NAME_1
            + "\". This is probably caused by a column being added to the table after the jdbc-mapping was created.");
  }

  @Test
  public void fieldsAreNotWrittenIfNoColumns() throws Exception {
    FieldType fieldType = FieldType.CHAR;
    when(metaData.getColumnCount()).thenReturn(0);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    PdxInstance result = createPdxInstance();

    assertThat(result).isSameAs(writablePdxInstance);
    verifyNoMoreInteractions(result);
  }

  @Test
  public void readOfCharFieldWithEmptyStringWritesCharZero() throws Exception {
    char expectedValue = 0;
    FieldType fieldType = FieldType.CHAR;
    when(metaData.getColumnType(1)).thenReturn(Types.CHAR);
    when(resultSet.getString(1)).thenReturn("");
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  public void readOfDateFieldWithDateColumnWritesDate() throws Exception {
    FieldType fieldType = FieldType.DATE;
    when(metaData.getColumnType(1)).thenReturn(Types.DATE);
    java.sql.Date sqlDate = java.sql.Date.valueOf("1979-09-11");
    Date expectedValue = new Date(sqlDate.getTime());
    when(resultSet.getDate(1)).thenReturn(sqlDate);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  public void readOfByteArrayFieldWithBlob() throws Exception {
    FieldType fieldType = FieldType.BYTE_ARRAY;
    when(metaData.getColumnType(1)).thenReturn(Types.BLOB);
    byte[] expectedValue = new byte[] {1, 2, 3};
    Blob blob = mock(Blob.class);
    when(blob.length()).thenReturn((long) expectedValue.length);
    when(blob.getBytes(1, expectedValue.length)).thenReturn(expectedValue);
    when(resultSet.getBlob(1)).thenReturn(blob);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  public void readOfByteArrayFieldWithNullBlob() throws Exception {
    FieldType fieldType = FieldType.BYTE_ARRAY;
    when(metaData.getColumnType(1)).thenReturn(Types.BLOB);
    when(resultSet.getBlob(1)).thenReturn(null);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    PdxInstance result = createPdxInstance();

    verifyResult(null, result);
    verify(resultSet).getBlob(1);
  }

  @Test
  public void readOfByteArrayFieldWithHugeBlobThrows() throws Exception {
    FieldType fieldType = FieldType.BYTE_ARRAY;
    when(metaData.getColumnType(1)).thenReturn(Types.BLOB);
    Blob blob = mock(Blob.class);
    when(blob.length()).thenReturn((long) Integer.MAX_VALUE + 1);
    when(resultSet.getBlob(1)).thenReturn(blob);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    assertThatThrownBy(this::createPdxInstance).isInstanceOf(JdbcConnectorException.class)
        .hasMessageContaining(
            "Blob of length 2147483648 is too big to be converted to a byte array.");
  }

  @Test
  public void readOfObjectFieldWithBlob() throws Exception {
    FieldType fieldType = FieldType.OBJECT;
    when(metaData.getColumnType(1)).thenReturn(Types.BLOB);
    byte[] expectedValue = new byte[] {1, 2, 3};
    Blob blob = mock(Blob.class);
    when(blob.length()).thenReturn((long) expectedValue.length);
    when(blob.getBytes(1, expectedValue.length)).thenReturn(expectedValue);
    when(resultSet.getBlob(1)).thenReturn(blob);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  public void readOfDateFieldWithTimeColumnWritesDate() throws Exception {
    FieldType fieldType = FieldType.DATE;
    when(metaData.getColumnType(1)).thenReturn(Types.TIME);
    java.sql.Time sqlTime = java.sql.Time.valueOf("22:33:44");
    Date expectedValue = new Date(sqlTime.getTime());
    when(resultSet.getTime(1)).thenReturn(sqlTime);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  public void readOfDateFieldWithTimestampColumnWritesDate() throws Exception {
    FieldType fieldType = FieldType.DATE;
    when(metaData.getColumnType(1)).thenReturn(Types.TIMESTAMP);
    java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf("1979-09-11 22:33:44.567");
    Date expectedValue = new Date(sqlTimestamp.getTime());
    when(resultSet.getTimestamp(1)).thenReturn(sqlTimestamp);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  public void readOfObjectFieldWithDateColumnWritesDate() throws Exception {
    FieldType fieldType = FieldType.OBJECT;
    java.sql.Date sqlDate = java.sql.Date.valueOf("1979-09-11");
    Date expectedValue = new Date(sqlDate.getTime());
    when(resultSet.getObject(1)).thenReturn(sqlDate);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  public void readOfObjectFieldWithJavaUtilDateWritesDate() throws Exception {
    FieldType fieldType = FieldType.OBJECT;
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);
    Date expectedValue = new Date();
    when(resultSet.getObject(1)).thenReturn(expectedValue);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  public void readOfObjectFieldWithTimeColumnWritesDate() throws Exception {
    FieldType fieldType = FieldType.OBJECT;
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);
    java.sql.Time sqlTime = java.sql.Time.valueOf("22:33:44");
    Date expectedValue = new Date(sqlTime.getTime());
    when(resultSet.getObject(1)).thenReturn(sqlTime);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  public void readOfObjectFieldWithTimestampColumnWritesDate() throws Exception {
    FieldType fieldType = FieldType.OBJECT;
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);
    java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf("1979-09-11 22:33:44.567");
    Date expectedValue = new Date(sqlTimestamp.getTime());
    when(resultSet.getObject(1)).thenReturn(sqlTimestamp);

    PdxInstance result = createPdxInstance();

    verifyResult(expectedValue, result);
  }

  @Test
  @Parameters({"BOOLEAN_ARRAY", "OBJECT_ARRAY", "CHAR_ARRAY", "SHORT_ARRAY", "INT_ARRAY",
      "LONG_ARRAY", "FLOAT_ARRAY", "DOUBLE_ARRAY", "STRING_ARRAY", "ARRAY_OF_BYTE_ARRAYS"})
  public void throwsExceptionWhenReadWritesUnsupportedType(FieldType fieldType) throws Exception {
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, fieldType);
    String returnValue = "ReturnValue";
    when(resultSet.getObject(1)).thenReturn(returnValue);

    assertThatThrownBy(this::createPdxInstance).isInstanceOf(JdbcConnectorException.class)
        .hasMessageContaining("Could not convert ");
  }

  @Test
  public void throwsExceptionIfMoreThanOneResultReturned() throws Exception {
    when(metaData.getColumnCount()).thenReturn(2);
    when(metaData.getColumnName(2)).thenReturn(COLUMN_NAME_2);
    sqlToPdxInstance.addMapping(COLUMN_NAME_1, PDX_FIELD_NAME_1, FieldType.STRING);
    sqlToPdxInstance.addMapping(COLUMN_NAME_2, PDX_FIELD_NAME_2, FieldType.STRING);
    when(resultSet.next()).thenReturn(true);

    assertThatThrownBy(this::createPdxInstance).isInstanceOf(JdbcConnectorException.class)
        .hasMessageContaining("Multiple rows returned for query: ");
  }

  private PdxInstance createPdxInstance() throws SQLException {
    return sqlToPdxInstance.create(resultSet);
  }

  private void verifyResult(Object expectedValue, PdxInstance result) {
    assertThat(result).isSameAs(writablePdxInstance);
    verify((WritablePdxInstance) result).setField(PDX_FIELD_NAME_1, expectedValue);
    verifyNoMoreInteractions(result);
  }

  private Object getFieldValueFromColumnValue(Object columnValue, FieldType fieldType) {
    Object result = columnValue;
    if (fieldType == FieldType.CHAR) {
      String columnString = (String) columnValue;
      result = columnString.charAt(0);
    } else if (fieldType == FieldType.DATE) {
      java.sql.Timestamp columnTimestamp = (java.sql.Timestamp) columnValue;
      result = new java.util.Date(columnTimestamp.getTime());
    }
    return result;
  }

  private Object setupGetFieldValueResultSet(FieldType fieldType, int columnIndex)
      throws SQLException {
    Object value = getValueByFieldType(fieldType);
    switch (fieldType) {
      case STRING:
        when(resultSet.getString(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case CHAR:
        when(resultSet.getString(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case SHORT:
        when(resultSet.getShort(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case INT:
        when(resultSet.getInt(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case LONG:
        when(resultSet.getLong(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case FLOAT:
        when(resultSet.getFloat(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case DOUBLE:
        when(resultSet.getDouble(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case BYTE:
        when(resultSet.getByte(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case BOOLEAN:
        when(resultSet.getBoolean(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case DATE:
        when(resultSet.getTimestamp(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case BYTE_ARRAY:
        when(resultSet.getBytes(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case BOOLEAN_ARRAY:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case CHAR_ARRAY:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case SHORT_ARRAY:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case INT_ARRAY:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case LONG_ARRAY:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case FLOAT_ARRAY:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case DOUBLE_ARRAY:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case STRING_ARRAY:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case OBJECT_ARRAY:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case ARRAY_OF_BYTE_ARRAYS:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      case OBJECT:
        when(resultSet.getObject(columnIndex)).thenReturn(getValueByFieldType(fieldType));
        break;
      default:
        throw new IllegalStateException("unhandled fieldType " + fieldType);
    }
    return value;
  }

  private static final byte[][] arrayOfByteArray = new byte[][] {{1, 2}, {3, 4}};

  @SuppressWarnings("unchecked")
  private <T> T getValueByFieldType(FieldType fieldType) {
    switch (fieldType) {
      case STRING:
        return (T) "stringValue";
      case CHAR:
        return (T) "charValue";
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
        return (T) new java.sql.Timestamp(1000);
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
}
