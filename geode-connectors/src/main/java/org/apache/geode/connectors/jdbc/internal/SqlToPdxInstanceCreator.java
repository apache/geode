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

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;

class SqlToPdxInstanceCreator {
  private final InternalCache cache;
  private final RegionMapping regionMapping;
  private final ResultSet resultSet;
  private final TableMetaDataView tableMetaData;
  private final PdxInstanceFactory factory;

  public SqlToPdxInstanceCreator(InternalCache cache, RegionMapping regionMapping,
      ResultSet resultSet, TableMetaDataView tableMetaData) {
    this.cache = cache;
    this.regionMapping = regionMapping;
    this.resultSet = resultSet;
    this.tableMetaData = tableMetaData;
    this.factory = createPdxInstanceFactory();
  }

  public PdxInstance create() throws SQLException {
    if (!resultSet.next()) {
      return null;
    }
    TypeRegistry typeRegistry = cache.getPdxRegistry();
    ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      String columnName = metaData.getColumnName(i);
      String fieldName = regionMapping.getFieldNameForColumn(columnName, typeRegistry);
      FieldType fieldType = getFieldType(typeRegistry, fieldName);
      writeField(columnName, i, fieldName, fieldType);
    }
    if (resultSet.next()) {
      throw new JdbcConnectorException(
          "Multiple rows returned for query: " + resultSet.getStatement());
    }
    return factory.create();
  }

  private PdxInstanceFactory createPdxInstanceFactory() {
    String valueClassName = regionMapping.getPdxName();
    if (valueClassName != null) {
      return cache.createPdxInstanceFactory(valueClassName);
    } else {
      return cache.createPdxInstanceFactory("no class", false);
    }
  }

  /**
   * @throws SQLException if the column value get fails
   */
  private void writeField(String columnName, int columnIndex, String fieldName, FieldType fieldType)
      throws SQLException {
    switch (fieldType) {
      case STRING:
        factory.writeString(fieldName, resultSet.getString(columnIndex));
        break;
      case CHAR:
        char charValue = 0;
        String columnValue = resultSet.getString(columnIndex);
        if (columnValue != null && columnValue.length() > 0) {
          charValue = columnValue.toCharArray()[0];
        }
        factory.writeChar(fieldName, charValue);
        break;
      case SHORT:
        factory.writeShort(fieldName, resultSet.getShort(columnIndex));
        break;
      case INT:
        factory.writeInt(fieldName, resultSet.getInt(columnIndex));
        break;
      case LONG:
        factory.writeLong(fieldName, resultSet.getLong(columnIndex));
        break;
      case FLOAT:
        factory.writeFloat(fieldName, resultSet.getFloat(columnIndex));
        break;
      case DOUBLE:
        factory.writeDouble(fieldName, resultSet.getDouble(columnIndex));
        break;
      case BYTE:
        factory.writeByte(fieldName, resultSet.getByte(columnIndex));
        break;
      case BOOLEAN:
        factory.writeBoolean(fieldName, resultSet.getBoolean(columnIndex));
        break;
      case DATE: {
        int columnType = this.tableMetaData.getColumnDataType(columnName);
        java.util.Date sqlDate;
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
        factory.writeDate(fieldName, pdxDate);
        break;
      }
      case BYTE_ARRAY:
        byte[] byteData;
        if (isBlobColumn(columnName)) {
          byteData = getBlobData(columnIndex);
        } else {
          byteData = resultSet.getBytes(columnIndex);
        }
        factory.writeByteArray(fieldName, byteData);
        break;
      case BOOLEAN_ARRAY:
        factory.writeBooleanArray(fieldName,
            convertJdbcObjectToJavaType(boolean[].class, resultSet.getObject(columnIndex)));
        break;
      case CHAR_ARRAY:
        factory.writeCharArray(fieldName,
            convertJdbcObjectToJavaType(char[].class, resultSet.getObject(columnIndex)));
        break;
      case SHORT_ARRAY:
        factory.writeShortArray(fieldName,
            convertJdbcObjectToJavaType(short[].class, resultSet.getObject(columnIndex)));
        break;
      case INT_ARRAY:
        factory.writeIntArray(fieldName,
            convertJdbcObjectToJavaType(int[].class, resultSet.getObject(columnIndex)));
        break;
      case LONG_ARRAY:
        factory.writeLongArray(fieldName,
            convertJdbcObjectToJavaType(long[].class, resultSet.getObject(columnIndex)));
        break;
      case FLOAT_ARRAY:
        factory.writeFloatArray(fieldName,
            convertJdbcObjectToJavaType(float[].class, resultSet.getObject(columnIndex)));
        break;
      case DOUBLE_ARRAY:
        factory.writeDoubleArray(fieldName,
            convertJdbcObjectToJavaType(double[].class, resultSet.getObject(columnIndex)));
        break;
      case STRING_ARRAY:
        factory.writeStringArray(fieldName,
            convertJdbcObjectToJavaType(String[].class, resultSet.getObject(columnIndex)));
        break;
      case OBJECT_ARRAY:
        factory.writeObjectArray(fieldName,
            convertJdbcObjectToJavaType(Object[].class, resultSet.getObject(columnIndex)));
        break;
      case ARRAY_OF_BYTE_ARRAYS:
        factory.writeArrayOfByteArrays(fieldName,
            convertJdbcObjectToJavaType(byte[][].class, resultSet.getObject(columnIndex)));
        break;
      case OBJECT: {
        Object v;
        if (isBlobColumn(columnName)) {
          v = getBlobData(columnIndex);
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
        factory.writeObject(fieldName, v);
        break;
      }
    }
  }

  private boolean isBlobColumn(String columnName) throws SQLException {
    return this.tableMetaData.getColumnDataType(columnName) == Types.BLOB;
  }

  /**
   * If the given column contains a Blob returns its data as a byte array;
   * otherwise return null.
   *
   * @throws JdbcConnectorException if blob is too big to fit in a byte array
   */
  private byte[] getBlobData(int columnIndex) throws SQLException {
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

  private FieldType getFieldType(TypeRegistry typeRegistry, String fieldName) {
    String pdxClassName = regionMapping.getPdxName();
    if (pdxClassName == null) {
      return FieldType.OBJECT;
    }
    PdxType pdxType = typeRegistry.getPdxTypeForField(fieldName, pdxClassName);
    if (pdxType != null) {
      PdxField pdxField = pdxType.getPdxField(fieldName);
      if (pdxField != null) {
        return pdxField.getFieldType();
      }
    }
    throw new JdbcConnectorException("Could not find PdxType for field " + fieldName
        + ". Add class " + pdxClassName + " with " + fieldName + " to pdx registry.");
  }
}
