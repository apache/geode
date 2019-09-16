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
package org.apache.geode.internal.offheap;

import java.io.DataInput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DSCODE;

/**
 * Determines the data type of the bytes in an off-heap MemoryBlock. This is used by the tests for
 * inspection of the off-heap memory.
 *
 * @since Geode 1.0
 */
public class DataType {

  public static String getDataType(byte[] bytes) {
    final DataInput in = getDataInput(bytes);
    byte header = 0;
    try {
      header = in.readByte();
    } catch (IOException e) {
      return "IOException: " + e.getMessage();
    }
    try {
      if (header == DSCODE.DS_FIXED_ID_BYTE.toByte()) {
        return "org.apache.geode.internal.serialization.DataSerializableFixedID:"
            + InternalDataSerializer.getDSFIDFactory().create(in.readByte(), in).getClass()
                .getName();
      }
      if (header == DSCODE.DS_FIXED_ID_SHORT.toByte()) {
        return "org.apache.geode.internal.serialization.DataSerializableFixedID:"
            + InternalDataSerializer.getDSFIDFactory().create(in.readShort(), in).getClass()
                .getName();
      }
      if (header == DSCODE.DS_FIXED_ID_INT.toByte()) {
        return "org.apache.geode.internal.serialization.DataSerializableFixedID:"
            + InternalDataSerializer.getDSFIDFactory().create(in.readInt(), in).getClass()
                .getName();
      }
      if (header == DSCODE.DS_NO_FIXED_ID.toByte()) {
        return "org.apache.geode.internal.serialization.DataSerializableFixedID:"
            + DataSerializer.readClass(in).getName();
      }
      if (header == DSCODE.NULL.toByte()) {
        return "null";
      }
      if (header == DSCODE.NULL_STRING.toByte() || header == DSCODE.STRING.toByte()
          || header == DSCODE.HUGE_STRING.toByte() || header == DSCODE.STRING_BYTES.toByte()
          || header == DSCODE.HUGE_STRING_BYTES.toByte()) {
        return "java.lang.String";
      }
      if (header == DSCODE.CLASS.toByte()) {
        return "java.lang.Class";
      }
      if (header == DSCODE.DATE.toByte()) {
        return "java.util.Date";
      }
      if (header == DSCODE.FILE.toByte()) {
        return "java.io.File";
      }
      if (header == DSCODE.INET_ADDRESS.toByte()) {
        return "java.net.InetAddress";
      }
      if (header == DSCODE.BOOLEAN.toByte()) {
        return "java.lang.Boolean";
      }
      if (header == DSCODE.CHARACTER.toByte()) {
        return "java.lang.Character";
      }
      if (header == DSCODE.BYTE.toByte()) {
        return "java.lang.Byte";
      }
      if (header == DSCODE.SHORT.toByte()) {
        return "java.lang.Short";
      }
      if (header == DSCODE.INTEGER.toByte()) {
        return "java.lang.Integer";
      }
      if (header == DSCODE.LONG.toByte()) {
        return "java.lang.Long";
      }
      if (header == DSCODE.FLOAT.toByte()) {
        return "java.lang.Float";
      }
      if (header == DSCODE.DOUBLE.toByte()) {
        return "java.lang.Double";
      }
      if (header == DSCODE.BYTE_ARRAY.toByte()) {
        return "byte[]";
      }
      if (header == DSCODE.ARRAY_OF_BYTE_ARRAYS.toByte()) {
        return "byte[][]";
      }
      if (header == DSCODE.SHORT_ARRAY.toByte()) {
        return "short[]";
      }
      if (header == DSCODE.STRING_ARRAY.toByte()) {
        return "java.lang.String[]";
      }
      if (header == DSCODE.INT_ARRAY.toByte()) {
        return "int[]";
      }
      if (header == DSCODE.LONG_ARRAY.toByte()) {
        return "long[]";
      }
      if (header == DSCODE.FLOAT_ARRAY.toByte()) {
        return "float[]";
      }
      if (header == DSCODE.DOUBLE_ARRAY.toByte()) {
        return "double[]";
      }
      if (header == DSCODE.BOOLEAN_ARRAY.toByte()) {
        return "boolean[]";
      }
      if (header == DSCODE.CHAR_ARRAY.toByte()) {
        return "char[]";
      }
      if (header == DSCODE.OBJECT_ARRAY.toByte()) {
        return "java.lang.Object[]";
      }
      if (header == DSCODE.ARRAY_LIST.toByte()) {
        return "java.util.ArrayList";
      }
      if (header == DSCODE.LINKED_LIST.toByte()) {
        return "java.util.LinkedList";
      }
      if (header == DSCODE.HASH_SET.toByte()) {
        return "java.util.HashSet";
      }
      if (header == DSCODE.LINKED_HASH_SET.toByte()) {
        return "java.util.LinkedHashSet";
      }
      if (header == DSCODE.HASH_MAP.toByte()) {
        return "java.util.HashMap";
      }
      if (header == DSCODE.IDENTITY_HASH_MAP.toByte()) {
        return "java.util.IdentityHashMap";
      }
      if (header == DSCODE.HASH_TABLE.toByte()) {
        return "java.util.Hashtable";
      }
      // ConcurrentHashMap is written as java.io.serializable
      // if (header == DSCODE.CONCURRENT_HASH_MAP.toByte()) {
      // return "java.util.concurrent.ConcurrentHashMap";
      if (header == DSCODE.PROPERTIES.toByte()) {
        return "java.util.Properties";
      }
      if (header == DSCODE.TIME_UNIT.toByte()) {
        return "java.util.concurrent.TimeUnit";
      }
      if (header == DSCODE.USER_CLASS.toByte()) {
        byte userClassDSId = in.readByte();
        return "DataSerializer: with Id:" + userClassDSId;
      }
      if (header == DSCODE.USER_CLASS_2.toByte()) {
        short userClass2DSId = in.readShort();
        return "DataSerializer: with Id:" + userClass2DSId;
      }
      if (header == DSCODE.USER_CLASS_4.toByte()) {
        int userClass4DSId = in.readInt();
        return "DataSerializer: with Id:" + userClass4DSId;
      }
      if (header == DSCODE.VECTOR.toByte()) {
        return "java.util.Vector";
      }
      if (header == DSCODE.STACK.toByte()) {
        return "java.util.Stack";
      }
      if (header == DSCODE.TREE_MAP.toByte()) {
        return "java.util.TreeMap";
      }
      if (header == DSCODE.TREE_SET.toByte()) {
        return "java.util.TreeSet";
      }
      if (header == DSCODE.BOOLEAN_TYPE.toByte()) {
        return "java.lang.Boolean.class";
      }
      if (header == DSCODE.CHARACTER_TYPE.toByte()) {
        return "java.lang.Character.class";
      }
      if (header == DSCODE.BYTE_TYPE.toByte()) {
        return "java.lang.Byte.class";
      }
      if (header == DSCODE.SHORT_TYPE.toByte()) {
        return "java.lang.Short.class";
      }
      if (header == DSCODE.INTEGER_TYPE.toByte()) {
        return "java.lang.Integer.class";
      }
      if (header == DSCODE.LONG_TYPE.toByte()) {
        return "java.lang.Long.class";
      }
      if (header == DSCODE.FLOAT_TYPE.toByte()) {
        return "java.lang.Float.class";
      }
      if (header == DSCODE.DOUBLE_TYPE.toByte()) {
        return "java.lang.Double.class";
      }
      if (header == DSCODE.VOID_TYPE.toByte()) {
        return "java.lang.Void.class";
      }
      if (header == DSCODE.USER_DATA_SERIALIZABLE.toByte()) {
        Instantiator instantiator = InternalInstantiator.getInstantiator(in.readByte());
        return "org.apache.geode.Instantiator:" + instantiator.getInstantiatedClass().getName();
      }
      if (header == DSCODE.USER_DATA_SERIALIZABLE_2.toByte()) {
        Instantiator instantiator = InternalInstantiator.getInstantiator(in.readShort());
        return "org.apache.geode.Instantiator:" + instantiator.getInstantiatedClass().getName();
      }
      if (header == DSCODE.USER_DATA_SERIALIZABLE_4.toByte()) {
        Instantiator instantiator = InternalInstantiator.getInstantiator(in.readInt());
        return "org.apache.geode.Instantiator:" + instantiator.getInstantiatedClass().getName();
      }
      if (header == DSCODE.DATA_SERIALIZABLE.toByte()) {
        return "org.apache.geode.DataSerializable:" + DataSerializer.readClass(in).getName();
      }
      if (header == DSCODE.SERIALIZABLE.toByte()) {
        String name = null;
        try {
          Object obj = InternalDataSerializer.basicReadObject(getDataInput(bytes));
          name = obj.getClass().getName();
        } catch (ClassNotFoundException e) {
          name = e.getMessage();
        }
        return "java.io.Serializable:" + name;
      }
      if (header == DSCODE.PDX.toByte()) {
        int typeId = in.readInt();
        return "pdxType:" + typeId;
      }
      if (header == DSCODE.PDX_ENUM.toByte()) {
        in.readByte(); // dsId is not needed
        int enumId = InternalDataSerializer.readArrayLength(in);
        return "pdxEnum:" + enumId;
      }
      if (header == DSCODE.GEMFIRE_ENUM.toByte()) {
        String name = DataSerializer.readString(in);
        return "java.lang.Enum:" + name;
      }
      if (header == DSCODE.PDX_INLINE_ENUM.toByte()) {
        String name = DataSerializer.readString(in);
        return "java.lang.Enum:" + name;
      }
      if (header == DSCODE.BIG_INTEGER.toByte()) {
        return "java.math.BigInteger";
      }
      if (header == DSCODE.BIG_DECIMAL.toByte()) {
        return "java.math.BigDecimal";
      }
      if (header == DSCODE.UUID.toByte()) {
        return "java.util.UUID";
      }
      if (header == DSCODE.TIMESTAMP.toByte()) {
        return "java.sql.Timestamp";
      }
      return "Unknown header byte: " + header;
    } catch (IOException e) {
      throw new Error(e);
    } catch (ClassNotFoundException e) {
      throw new Error(e);
    }
  }

  public static DataInput getDataInput(byte[] bytes) {
    return new ByteArrayDataInput(bytes);
  }
}
