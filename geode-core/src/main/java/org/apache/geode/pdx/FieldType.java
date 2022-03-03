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
package org.apache.geode.pdx;

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.pdx.internal.DataSize;

/**
 * Every field of a pdx must have one of these types. The type of that field can never change. If
 * you do want to be able to change a field's class then its field type needs to be {@link #OBJECT}.
 * Some field types are always serialized with a certain number of bytes; these are called
 * "fixed-width". Others are serialized with a variable number of bytes; these are called
 * "variable-width".
 *
 * @since GemFire 6.6.2
 */
public enum FieldType {
  BOOLEAN(true, DataSize.BOOLEAN_SIZE, "boolean", new byte[] {0}, false),
  BYTE(true, DataSize.BYTE_SIZE, "byte", new byte[] {0}, 0),
  CHAR(true, DataSize.CHAR_SIZE, "char", new byte[] {0, 0}, (char) 0),
  SHORT(true, DataSize.SHORT_SIZE, "short", new byte[] {0, 0}, 0),
  INT(true, DataSize.INTEGER_SIZE, "int", new byte[] {0, 0, 0, 0}, 0),
  LONG(true, DataSize.LONG_SIZE, "long", new byte[] {0, 0, 0, 0, 0, 0, 0, 0}, 0),
  FLOAT(true, DataSize.FLOAT_SIZE, "float", new byte[] {0, 0, 0, 0}, 0),
  DOUBLE(true, DataSize.DOUBLE_SIZE, "double", new byte[] {0, 0, 0, 0, 0, 0, 0, 0}, 0),
  DATE(true, DataSize.DATE_SIZE, "Date", new byte[] {-1, -1, -1, -1, -1, -1, -1, -1}, null),
  STRING(false, -1, "String", new byte[] {DSCODE.NULL_STRING.toByte()}, null),
  OBJECT(false, -1, "Object", new byte[] {DSCODE.NULL.toByte()}, null),
  BOOLEAN_ARRAY(false, -1, "boolean[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  CHAR_ARRAY(false, -1, "char[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  BYTE_ARRAY(false, -1, "byte[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  SHORT_ARRAY(false, -1, "short[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  INT_ARRAY(false, -1, "int[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  LONG_ARRAY(false, -1, "long[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  FLOAT_ARRAY(false, -1, "float[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  DOUBLE_ARRAY(false, -1, "double[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  STRING_ARRAY(false, -1, "String[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  OBJECT_ARRAY(false, -1, "Object[]", new byte[] {StaticSerialization.NULL_ARRAY}, null),
  ARRAY_OF_BYTE_ARRAYS(false, -1, "byte[][]", new byte[] {StaticSerialization.NULL_ARRAY}, null);

  private final boolean isFixedWidth;
  /**
   * For fixed width fields. All others have -1.
   */
  private final int width;
  private final String name;
  private final ByteBuffer defaultSerializedValue;
  private final Object defaultValue;

  FieldType(boolean isFixedWidth, int width, String name, byte[] defaultBytes,
      Object defaultValue) {
    this.isFixedWidth = isFixedWidth;
    this.width = width;
    this.name = name;
    defaultSerializedValue = ByteBuffer.wrap(defaultBytes).asReadOnlyBuffer();
    this.defaultValue = defaultValue;
  }

  /**
   * Returns true if the type is "fixed-width"; false if it is "variable-width".
   */
  public boolean isFixedWidth() {
    return isFixedWidth;
  }

  /**
   * Returns the number of bytes used to serialize fixed-width fields; -1 is returned for
   * variable-width fields.
   */
  public int getWidth() {
    return width;
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * Returns a ByteBuffer that contains the serialized encoding of this type's default value.
   */
  public ByteBuffer getDefaultBytes() {
    return defaultSerializedValue;
  }

  /**
   * Given a Class return the corresponding FieldType.
   */
  public static FieldType get(Class<?> c) {
    if (c.equals(boolean.class)) {
      return BOOLEAN;
    } else if (c.equals(byte.class)) {
      return BYTE;
    } else if (c.equals(char.class)) {
      return CHAR;
    } else if (c.equals(short.class)) {
      return SHORT;
    } else if (c.equals(int.class)) {
      return INT;
    } else if (c.equals(long.class)) {
      return LONG;
    } else if (c.equals(float.class)) {
      return FLOAT;
    } else if (c.equals(double.class)) {
      return DOUBLE;
    } else if (c.equals(String.class)) {
      return STRING;
    } else if (c.equals(Date.class)) {
      return DATE;
    } else if (c.isArray()) {
      if (c.equals(boolean[].class)) {
        return BOOLEAN_ARRAY;
      } else if (c.equals(byte[].class)) {
        return BYTE_ARRAY;
      } else if (c.equals(char[].class)) {
        return CHAR_ARRAY;
      } else if (c.equals(short[].class)) {
        return SHORT_ARRAY;
      } else if (c.equals(int[].class)) {
        return INT_ARRAY;
      } else if (c.equals(long[].class)) {
        return LONG_ARRAY;
      } else if (c.equals(float[].class)) {
        return FLOAT_ARRAY;
      } else if (c.equals(double[].class)) {
        return DOUBLE_ARRAY;
      } else if (c.equals(String[].class)) {
        return STRING_ARRAY;
      } else if (c.equals(byte[][].class)) {
        return ARRAY_OF_BYTE_ARRAYS;
      } else {
        return OBJECT_ARRAY;
      }
    } else {
      return OBJECT;
    }
  }

  public Object getDefaultValue() {
    return defaultValue;
  }
}
