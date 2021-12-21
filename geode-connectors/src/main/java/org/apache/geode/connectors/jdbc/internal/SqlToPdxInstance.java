/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.connectors.jdbc.internal;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.WritablePdxInstance;

public class SqlToPdxInstance {
  private PdxInstance pdxTemplate;
  private final Map<String, PdxFieldInfo> columnToPdxFieldMap = new HashMap<>();

  // for unit testing
  PdxInstance getPdxTemplate() {
    return pdxTemplate;
  }

  public void setPdxTemplate(PdxInstance template) {
    pdxTemplate = template;
  }

  public void addMapping(String columnName, String pdxFieldName, FieldType pdxFieldType) {
    columnToPdxFieldMap.put(columnName, new PdxFieldInfo(pdxFieldName, pdxFieldType));
  }

  // for unit testing
  Map<String, PdxFieldInfo> getColumnToPdxFieldMap() {
    return columnToPdxFieldMap;
  }

  static class PdxFieldInfo {
    private final String name;
    private final FieldType type;

    public PdxFieldInfo(String name, FieldType type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public FieldType getType() {
      return type;
    }
  }

  public PdxInstance create(ResultSet resultSet) throws SQLException {
    if (!resultSet.next()) {
      return null;
    }
    WritablePdxInstance result = pdxTemplate.createWriter();
    ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      String columnName = metaData.getColumnName(i);
      PdxFieldInfo fieldInfo = columnToPdxFieldMap.get(columnName);
      if (fieldInfo == null) {
        throw new JdbcConnectorException(
            "The jdbc-mapping does not contain the column name \"" + columnName + "\"."
                + " This is probably caused by a column being added to the table after the jdbc-mapping was created.");
      }
      Object fieldValue = getFieldValue(resultSet, i, fieldInfo.getType(), metaData);
      result.setField(fieldInfo.getName(), fieldValue);
    }
    if (resultSet.next()) {
      throw new JdbcConnectorException(
          "Multiple rows returned for query: " + resultSet.getStatement());
    }
    return result;
  }

  /**
   * @throws SQLException if the column value get fails
   */
  Object getFieldValue(ResultSet resultSet, int columnIndex, FieldType fieldType,
      ResultSetMetaData metaData)
      throws SQLException {
    switch (fieldType) {
      case STRING:
        return resultSet.getString(columnIndex);
      case CHAR:
        return getCharValue(resultSet, columnIndex);
      case SHORT:
        return resultSet.getShort(columnIndex);
      case INT:
        return resultSet.getInt(columnIndex);
      case LONG:
        return resultSet.getLong(columnIndex);
      case FLOAT:
        return resultSet.getFloat(columnIndex);
      case DOUBLE:
        return resultSet.getDouble(columnIndex);
      case BYTE:
        return resultSet.getByte(columnIndex);
      case BOOLEAN:
        return resultSet.getBoolean(columnIndex);
      case DATE:
        return getDateValue(resultSet, columnIndex, metaData);
      case BYTE_ARRAY:
        return getByteArrayValue(resultSet, columnIndex, metaData);
      case BOOLEAN_ARRAY:
        return convertJdbcObjectToJavaType(boolean[].class, resultSet.getObject(columnIndex));
      case CHAR_ARRAY:
        return convertJdbcObjectToJavaType(char[].class, resultSet.getObject(columnIndex));
      case SHORT_ARRAY:
        return convertJdbcObjectToJavaType(short[].class, resultSet.getObject(columnIndex));
      case INT_ARRAY:
        return convertJdbcObjectToJavaType(int[].class, resultSet.getObject(columnIndex));
      case LONG_ARRAY:
        return convertJdbcObjectToJavaType(long[].class, resultSet.getObject(columnIndex));
      case FLOAT_ARRAY:
        return convertJdbcObjectToJavaType(float[].class, resultSet.getObject(columnIndex));
      case DOUBLE_ARRAY:
        return convertJdbcObjectToJavaType(double[].class, resultSet.getObject(columnIndex));
      case STRING_ARRAY:
        return convertJdbcObjectToJavaType(String[].class, resultSet.getObject(columnIndex));
      case OBJECT_ARRAY:
        return convertJdbcObjectToJavaType(Object[].class, resultSet.getObject(columnIndex));
      case ARRAY_OF_BYTE_ARRAYS:
        return convertJdbcObjectToJavaType(byte[][].class, resultSet.getObject(columnIndex));
      case OBJECT:
        return getObjectValue(resultSet, columnIndex, metaData);
      default:
        throw new IllegalStateException("unhandled pdx field type: " + fieldType);
    }
  }

  private Object getObjectValue(ResultSet resultSet, int columnIndex, ResultSetMetaData metaData)
      throws SQLException {
    Object v;
    if (isBlobColumn(columnIndex, metaData)) {
      v = getBlobData(resultSet, columnIndex);
    } else {
      v = resultSet.getObject(columnIndex);
      if (v instanceof java.util.Date) {
        if (v instanceof java.sql.Date) {
          java.sql.Date sqlDate = (java.sql.Date) v;
          v = new java.util.Date(sqlDate.getTime());
        } else if (v instanceof java.sql.Time) {
          java.sql.Time sqlTime = (java.sql.Time) v;
          v = new java.util.Date(sqlTime.getTime());
        } else if (v instanceof java.sql.Timestamp) {
          java.sql.Timestamp sqlTimestamp = (java.sql.Timestamp) v;
          v = new java.util.Date(sqlTimestamp.getTime());
        }
      }
    }
    return v;
  }

  private Object getByteArrayValue(ResultSet resultSet, int columnIndex, ResultSetMetaData metaData)
      throws SQLException {
    byte[] byteData;
    if (isBlobColumn(columnIndex, metaData)) {
      byteData = getBlobData(resultSet, columnIndex);
    } else {
      byteData = resultSet.getBytes(columnIndex);
    }
    return byteData;
  }

  private Object getCharValue(ResultSet resultSet, int columnIndex) throws SQLException {
    char charValue = 0;
    String columnValue = resultSet.getString(columnIndex);
    if (columnValue != null && columnValue.length() > 0) {
      charValue = columnValue.charAt(0);
    }
    return charValue;
  }

  private java.util.Date getDateValue(ResultSet resultSet, int columnIndex,
      ResultSetMetaData metaData)
      throws SQLException {
    java.util.Date sqlDate;
    int columnType = metaData.getColumnType(columnIndex);
    switch (columnType) {
      case Types.DATE:
        sqlDate = resultSet.getDate(columnIndex);
        break;
      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
        sqlDate = resultSet.getTime(columnIndex);
        break;
      default:
        sqlDate = resultSet.getTimestamp(columnIndex);
        break;
    }
    java.util.Date pdxDate = null;
    if (sqlDate != null) {
      pdxDate = new java.util.Date(sqlDate.getTime());
    }
    return pdxDate;
  }

  private boolean isBlobColumn(int columnIndex, ResultSetMetaData metaData) throws SQLException {
    int columnType = metaData.getColumnType(columnIndex);
    return Types.BLOB == columnType;
  }

  /**
   * If the given column contains a Blob returns its data as a byte array;
   * otherwise return null.
   *
   * @throws JdbcConnectorException if blob is too big to fit in a byte array
   */
  private byte[] getBlobData(ResultSet resultSet, int columnIndex) throws SQLException {
    Blob blob = resultSet.getBlob(columnIndex);
    if (blob == null) {
      return null;
    }
    try {
      long blobLength = blob.length();
      if (blobLength > Integer.MAX_VALUE) {
        throw new JdbcConnectorException(
            "Blob of length " + blobLength + " is too big to be converted to a byte array.");
      }
      return blob.getBytes(1, (int) blobLength);
    } finally {
      blob.free();
    }
  }

  private <T> T convertJdbcObjectToJavaType(Class<T> javaType, Object jdbcObject) {
    try {
      return javaType.cast(jdbcObject);
    } catch (ClassCastException classCastException) {
      throw JdbcConnectorException.createException("Could not convert "
          + jdbcObject.getClass().getTypeName() + " to " + javaType.getTypeName(),
          classCastException);
    }
  }

}
