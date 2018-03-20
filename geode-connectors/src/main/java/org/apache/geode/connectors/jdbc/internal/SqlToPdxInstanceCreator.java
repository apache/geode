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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
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
  private final String keyColumnName;

  public SqlToPdxInstanceCreator(InternalCache cache, RegionMapping regionMapping,
      ResultSet resultSet, String keyColumnName) {
    this.cache = cache;
    this.regionMapping = regionMapping;
    this.resultSet = resultSet;
    this.keyColumnName = keyColumnName;
  }

  public PdxInstance create() throws SQLException {
    PdxInstanceFactory factory = getPdxInstanceFactory(cache, regionMapping);
    PdxInstance pdxInstance = null;
    if (resultSet.next()) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      int ColumnsNumber = metaData.getColumnCount();
      TypeRegistry typeRegistry = cache.getPdxRegistry();
      for (int i = 1; i <= ColumnsNumber; i++) {
        String columnName = metaData.getColumnName(i);
        if (regionMapping.isPrimaryKeyInValue() || !keyColumnName.equalsIgnoreCase(columnName)) {
          String fieldName = regionMapping.getFieldNameForColumn(columnName, typeRegistry);
          FieldType fieldType =
              getFieldType(typeRegistry, regionMapping.getPdxClassName(), fieldName);
          writeField(factory, resultSet, i, fieldName, fieldType);
        }
      }
      if (resultSet.next()) {
        throw new JdbcConnectorException(
            "Multiple rows returned for query: " + resultSet.getStatement().toString());
      }
      pdxInstance = factory.create();
    }
    return pdxInstance;
  }

  private PdxInstanceFactory getPdxInstanceFactory(InternalCache cache,
      RegionMapping regionMapping) {
    String valueClassName = regionMapping.getPdxClassName();
    PdxInstanceFactory factory;
    if (valueClassName != null) {
      factory = cache.createPdxInstanceFactory(valueClassName);
    } else {
      factory = cache.createPdxInstanceFactory("no class", false);
    }
    return factory;
  }

  /**
   * @throws SQLException if the column value get fails
   */
  private void writeField(PdxInstanceFactory factory, ResultSet resultSet, int columnIndex,
      String fieldName, FieldType fieldType) throws SQLException {
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
      case DATE:
        java.sql.Timestamp sqlDate = resultSet.getTimestamp(columnIndex);
        java.util.Date pdxDate = null;
        if (sqlDate != null) {
          pdxDate = new java.util.Date(sqlDate.getTime());
        }
        factory.writeDate(fieldName, pdxDate);
        break;
      case BYTE_ARRAY:
        factory.writeByteArray(fieldName, resultSet.getBytes(columnIndex));
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
      case OBJECT:
        factory.writeObject(fieldName, resultSet.getObject(columnIndex));
        break;
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

  private FieldType getFieldType(TypeRegistry typeRegistry, String pdxClassName, String fieldName) {
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
