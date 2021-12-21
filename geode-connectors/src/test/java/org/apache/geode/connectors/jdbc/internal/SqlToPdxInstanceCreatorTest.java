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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Arrays;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class SqlToPdxInstanceCreatorTest {

  private static final String COLUMN_NAME_1 = "columnName1";
  private static final Object COLUMN_VALUE_1 = "columnValue1";
  private static final Object COLUMN_VALUE_2 = "columnValue2";
  private static final String COLUMN_NAME_2 = "columnName2";
  private static final String KEY_COLUMN = "keyColumn";
  private static final String PDX_FIELD_NAME_1 = COLUMN_NAME_1.toLowerCase();
  private static final String PDX_FIELD_NAME_2 = COLUMN_NAME_2.toLowerCase();
  private static final String PDX_CLASS_NAME = "myPdxClassName";

  private InternalCache cache;
  private RegionMapping regionMapping;
  private FieldMapping columnMapping = mock(FieldMapping.class);
  private final PdxInstance pdxTemplate = mock(PdxInstance.class);

  @Before
  public void setup() throws Exception {
    cache = mock(InternalCache.class);
    regionMapping = mock(RegionMapping.class);
    columnMapping = mock(FieldMapping.class);
    when(columnMapping.getJdbcName()).thenReturn(COLUMN_NAME_1);
    when(columnMapping.getPdxName()).thenReturn(PDX_FIELD_NAME_1);
    when(columnMapping.getPdxType()).thenReturn(FieldType.OBJECT.name());
    when(regionMapping.getFieldMappings()).thenReturn(Arrays.asList(columnMapping));
  }

  @Test
  @Parameters(source = FieldType.class)
  public void readWritesFieldGivenPdxFieldType(FieldType fieldType) throws Exception {
    PdxInstanceFactory factory = setupPdxInstanceFactory(fieldType);
    when(columnMapping.getJdbcType()).thenReturn(JDBCType.NULL.name());
    when(columnMapping.getPdxType()).thenReturn(fieldType.name());

    createSqlToPdxInstance();

    verifyPdxFactoryWrite(factory, fieldType);
    verify(factory).create();
  }

  private SqlToPdxInstance createSqlToPdxInstance() throws SQLException {
    SqlToPdxInstanceCreator sqlToPdxInstanceCreator =
        new SqlToPdxInstanceCreator(cache, regionMapping);
    return sqlToPdxInstanceCreator.create();
  }

  private PdxInstanceFactory setupPdxInstanceFactory(FieldType fieldType) {
    PdxInstanceFactory factory = mock(PdxInstanceFactory.class);
    when(factory.create()).thenReturn(pdxTemplate);
    when(cache.createPdxInstanceFactory(PDX_CLASS_NAME)).thenReturn(factory);

    when(regionMapping.getPdxName()).thenReturn(PDX_CLASS_NAME);
    if (fieldType != null) {
      when(columnMapping.getPdxType()).thenReturn(fieldType.name());
    }
    return factory;
  }

  private void verifyPdxFactoryWrite(PdxInstanceFactory factory, FieldType fieldType) {
    switch (fieldType) {
      case STRING:
        verify(factory).writeString(PDX_FIELD_NAME_1, null);
        break;
      case CHAR:
        verify(factory).writeChar(PDX_FIELD_NAME_1, (char) 0);
        break;
      case SHORT:
        verify(factory).writeShort(PDX_FIELD_NAME_1, (short) 0);
        break;
      case INT:
        verify(factory).writeInt(PDX_FIELD_NAME_1, 0);
        break;
      case LONG:
        verify(factory).writeLong(PDX_FIELD_NAME_1, 0);
        break;
      case FLOAT:
        verify(factory).writeFloat(PDX_FIELD_NAME_1, 0);
        break;
      case DOUBLE:
        verify(factory).writeDouble(PDX_FIELD_NAME_1, 0);
        break;
      case BYTE:
        verify(factory).writeByte(PDX_FIELD_NAME_1, (byte) 0);
        break;
      case BOOLEAN:
        verify(factory).writeBoolean(PDX_FIELD_NAME_1, false);
        break;
      case DATE:
        verify(factory).writeDate(PDX_FIELD_NAME_1, null);
        break;
      case BYTE_ARRAY:
        verify(factory).writeByteArray(PDX_FIELD_NAME_1, null);
        break;
      case BOOLEAN_ARRAY:
        verify(factory).writeBooleanArray(PDX_FIELD_NAME_1, null);
        break;
      case CHAR_ARRAY:
        verify(factory).writeCharArray(PDX_FIELD_NAME_1, null);
        break;
      case SHORT_ARRAY:
        verify(factory).writeShortArray(PDX_FIELD_NAME_1, null);
        break;
      case INT_ARRAY:
        verify(factory).writeIntArray(PDX_FIELD_NAME_1, null);
        break;
      case LONG_ARRAY:
        verify(factory).writeLongArray(PDX_FIELD_NAME_1, null);
        break;
      case FLOAT_ARRAY:
        verify(factory).writeFloatArray(PDX_FIELD_NAME_1, null);
        break;
      case DOUBLE_ARRAY:
        verify(factory).writeDoubleArray(PDX_FIELD_NAME_1, null);
        break;
      case STRING_ARRAY:
        verify(factory).writeStringArray(PDX_FIELD_NAME_1, null);
        break;
      case OBJECT_ARRAY:
        verify(factory).writeObjectArray(PDX_FIELD_NAME_1, null);
        break;
      case ARRAY_OF_BYTE_ARRAYS:
        verify(factory).writeArrayOfByteArrays(PDX_FIELD_NAME_1, null);
        break;
      case OBJECT:
        verify(factory).writeObject(PDX_FIELD_NAME_1, null);
        break;
      default:
        throw new IllegalStateException("unhandled fieldType " + fieldType);
    }
  }
}
