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

import java.sql.JDBCType;
import java.util.Set;

import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.TypeRegistry;

public class SqlToPdxInstanceCreator {
  private final InternalCache cache;
  private final RegionMapping regionMapping;

  public SqlToPdxInstanceCreator(InternalCache cache, RegionMapping regionMapping) {
    this.cache = cache;
    this.regionMapping = regionMapping;
  }

  public SqlToPdxInstance create() {
    TypeRegistry typeRegistry = cache.getPdxRegistry();
    SqlToPdxInstance result = new SqlToPdxInstance();
    PdxInstanceFactory templateFactory = createPdxInstanceFactory();
    for (FieldMapping columnMapping : regionMapping.getFieldMappings()) {
      String columnName = columnMapping.getJdbcName();
      String fieldName = columnMapping.getPdxName();
      FieldType fieldType;
      if (fieldName.isEmpty()) {
        Set<PdxField> pdxFields =
            typeRegistry.findFieldThatMatchesName(regionMapping.getPdxName(), columnName);
        JDBCType columnType = JDBCType.valueOf(columnMapping.getJdbcType());
        if (pdxFields.isEmpty()) {
          fieldName = columnName;
          fieldType = computeFieldType(columnMapping.isJdbcNullable(), columnType);
        } else {
          fieldName = pdxFields.iterator().next().getFieldName();
          fieldType = findFieldType(pdxFields, columnMapping.isJdbcNullable(), columnType);
        }
      } else {
        fieldType = FieldType.valueOf(columnMapping.getPdxType());
      }
      result.addMapping(columnName, fieldName, fieldType);
      writeField(templateFactory, columnMapping, fieldName, fieldType);
    }
    result.setPdxTemplate(templateFactory.create());
    return result;
  }

  private PdxInstanceFactory createPdxInstanceFactory() {
    String valueClassName = regionMapping.getPdxName();
    return cache.createPdxInstanceFactory(valueClassName);
  }

  private void writeField(PdxInstanceFactory factory, FieldMapping columnMapping, String fieldName,
      FieldType fieldType) {
    switch (fieldType) {
      case STRING:
        factory.writeString(fieldName, null);
        break;
      case CHAR:
        factory.writeChar(fieldName, (char) 0);
        break;
      case SHORT:
        factory.writeShort(fieldName, (short) 0);
        break;
      case INT:
        factory.writeInt(fieldName, 0);
        break;
      case LONG:
        factory.writeLong(fieldName, 0L);
        break;
      case FLOAT:
        factory.writeFloat(fieldName, 0);
        break;
      case DOUBLE:
        factory.writeDouble(fieldName, 0);
        break;
      case BYTE:
        factory.writeByte(fieldName, (byte) 0);
        break;
      case BOOLEAN:
        factory.writeBoolean(fieldName, false);
        break;
      case DATE:
        factory.writeDate(fieldName, null);
        break;
      case BYTE_ARRAY:
        factory.writeByteArray(fieldName, null);
        break;
      case BOOLEAN_ARRAY:
        factory.writeBooleanArray(fieldName, null);
        break;
      case CHAR_ARRAY:
        factory.writeCharArray(fieldName, null);
        break;
      case SHORT_ARRAY:
        factory.writeShortArray(fieldName, null);
        break;
      case INT_ARRAY:
        factory.writeIntArray(fieldName, null);
        break;
      case LONG_ARRAY:
        factory.writeLongArray(fieldName, null);
        break;
      case FLOAT_ARRAY:
        factory.writeFloatArray(fieldName, null);
        break;
      case DOUBLE_ARRAY:
        factory.writeDoubleArray(fieldName, null);
        break;
      case STRING_ARRAY:
        factory.writeStringArray(fieldName, null);
        break;
      case OBJECT_ARRAY:
        factory.writeObjectArray(fieldName, null);
        break;
      case ARRAY_OF_BYTE_ARRAYS:
        factory.writeArrayOfByteArrays(fieldName, null);
        break;
      case OBJECT:
        factory.writeObject(fieldName, null);
        break;
      default:
        throw new IllegalStateException("unhandled pdx field type " + fieldType);
    }
  }

  public static FieldType findFieldType(Set<PdxField> pdxFields, boolean columnNullable,
      JDBCType columnType) {
    if (pdxFields.size() == 1) {
      return pdxFields.iterator().next().getFieldType();
    } else {
      FieldType fieldTypeBasedOnJDBC = computeFieldType(columnNullable, columnType);
      // TODO find best type in pdxFields
      return pdxFields.iterator().next().getFieldType();
    }
  }

  static FieldType computeFieldType(boolean isNullable, JDBCType jdbcType) {
    switch (jdbcType) {
      case NULL:
        throw new IllegalStateException("unexpected NULL jdbc column type");
      case BOOLEAN:
        return computeType(isNullable, FieldType.BOOLEAN);
      case BIT: // 1 bit
        return computeType(isNullable, FieldType.BOOLEAN);
      case TINYINT: // unsigned 8 bits
        return computeType(isNullable, FieldType.SHORT);
      case SMALLINT: // signed 16 bits
        return computeType(isNullable, FieldType.SHORT);
      case INTEGER: // signed 32 bits
        return computeType(isNullable, FieldType.INT);
      case BIGINT: // signed 64 bits
        return computeType(isNullable, FieldType.LONG);
      case FLOAT:
        return computeType(isNullable, FieldType.DOUBLE);
      case REAL:
        return computeType(isNullable, FieldType.FLOAT);
      case DOUBLE:
        return computeType(isNullable, FieldType.DOUBLE);
      case CHAR:
        return FieldType.STRING;
      case VARCHAR:
        return FieldType.STRING;
      case LONGVARCHAR:
        return FieldType.STRING;
      case DATE:
        return FieldType.DATE;
      case TIME:
        return FieldType.DATE;
      case TIMESTAMP:
        return FieldType.DATE;
      case BINARY:
        return FieldType.BYTE_ARRAY;
      case VARBINARY:
        return FieldType.BYTE_ARRAY;
      case LONGVARBINARY:
        return FieldType.BYTE_ARRAY;
      case BLOB:
        return FieldType.BYTE_ARRAY;
      case NCHAR:
        return FieldType.STRING;
      case NVARCHAR:
        return FieldType.STRING;
      case LONGNVARCHAR:
        return FieldType.STRING;
      case TIME_WITH_TIMEZONE:
        return FieldType.DATE;
      case TIMESTAMP_WITH_TIMEZONE:
        return FieldType.DATE;
      default:
        return FieldType.OBJECT;
    }
  }

  private static FieldType computeType(boolean isNullable, FieldType nonNullType) {
    if (isNullable) {
      return FieldType.OBJECT;
    }
    return nonNullType;

  }
}
