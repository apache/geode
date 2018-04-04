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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.HeaderByte;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;

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
      if (header == HeaderByte.DS_FIXED_ID_BYTE.toByte()) {
        return "org.apache.geode.internal.DataSerializableFixedID:"
            + DSFIDFactory.create(in.readByte(), in).getClass().getName();
      }
      if (header == HeaderByte.DS_FIXED_ID_SHORT.toByte()) {
        return "org.apache.geode.internal.DataSerializableFixedID:"
            + DSFIDFactory.create(in.readShort(), in).getClass().getName();
      }
      if (header == HeaderByte.DS_FIXED_ID_INT.toByte()) {
        return "org.apache.geode.internal.DataSerializableFixedID:"
            + DSFIDFactory.create(in.readInt(), in).getClass().getName();
      }
      if (header == HeaderByte.DS_NO_FIXED_ID.toByte()) {
        return "org.apache.geode.internal.DataSerializableFixedID:"
            + DataSerializer.readClass(in).getName();
      }
      if (header == HeaderByte.NULL.toByte()) {
        return "null";
      }
      if (header == HeaderByte.NULL_STRING.toByte() || header == HeaderByte.STRING.toByte()
          || header == HeaderByte.HUGE_STRING.toByte() || header == HeaderByte.STRING_BYTES.toByte()
          || header == HeaderByte.HUGE_STRING_BYTES.toByte()) {
        return "java.lang.String";
      }
      if (header == HeaderByte.CLASS.toByte()) {
        return "java.lang.Class";
      }
      if (header == HeaderByte.DATE.toByte()) {
        return "java.util.Date";
      }
      if (header == HeaderByte.FILE.toByte()) {
        return "java.io.File";
      }
      if (header == HeaderByte.INET_ADDRESS.toByte()) {
        return "java.net.InetAddress";
      }
      if (header == HeaderByte.BOOLEAN.toByte()) {
        return "java.lang.Boolean";
      }
      if (header == HeaderByte.CHARACTER.toByte()) {
        return "java.lang.Character";
      }
      if (header == HeaderByte.BYTE.toByte()) {
        return "java.lang.Byte";
      }
      if (header == HeaderByte.SHORT.toByte()) {
        return "java.lang.Short";
      }
      if (header == HeaderByte.INTEGER.toByte()) {
        return "java.lang.Integer";
      }
      if (header == HeaderByte.LONG.toByte()) {
        return "java.lang.Long";
      }
      if (header == HeaderByte.FLOAT.toByte()) {
        return "java.lang.Float";
      }
      if (header == HeaderByte.DOUBLE.toByte()) {
        return "java.lang.Double";
      }
      if (header == HeaderByte.BYTE_ARRAY.toByte()) {
        return "byte[]";
      }
      if (header == HeaderByte.ARRAY_OF_BYTE_ARRAYS.toByte()) {
        return "byte[][]";
      }
      if (header == HeaderByte.SHORT_ARRAY.toByte()) {
        return "short[]";
      }
      if (header == HeaderByte.STRING_ARRAY.toByte()) {
        return "java.lang.String[]";
      }
      if (header == HeaderByte.INT_ARRAY.toByte()) {
        return "int[]";
      }
      if (header == HeaderByte.LONG_ARRAY.toByte()) {
        return "long[]";
      }
      if (header == HeaderByte.FLOAT_ARRAY.toByte()) {
        return "float[]";
      }
      if (header == HeaderByte.DOUBLE_ARRAY.toByte()) {
        return "double[]";
      }
      if (header == HeaderByte.BOOLEAN_ARRAY.toByte()) {
        return "boolean[]";
      }
      if (header == HeaderByte.CHAR_ARRAY.toByte()) {
        return "char[]";
      }
      if (header == HeaderByte.OBJECT_ARRAY.toByte()) {
        return "java.lang.Object[]";
      }
      if (header == HeaderByte.ARRAY_LIST.toByte()) {
        return "java.util.ArrayList";
      }
      if (header == HeaderByte.LINKED_LIST.toByte()) {
        return "java.util.LinkedList";
      }
      if (header == HeaderByte.HASH_SET.toByte()) {
        return "java.util.HashSet";
      }
      if (header == HeaderByte.LINKED_HASH_SET.toByte()) {
        return "java.util.LinkedHashSet";
      }
      if (header == HeaderByte.HASH_MAP.toByte()) {
        return "java.util.HashMap";
      }
      if (header == HeaderByte.IDENTITY_HASH_MAP.toByte()) {
        return "java.util.IdentityHashMap";
      }
      if (header == HeaderByte.HASH_TABLE.toByte()) {
        return "java.util.Hashtable";
      }
      // ConcurrentHashMap is written as java.io.serializable
      // if (header == HeaderByte.CONCURRENT_HASH_MAP.toByte()) {
      // return "java.util.concurrent.ConcurrentHashMap";
      if (header == HeaderByte.PROPERTIES.toByte()) {
        return "java.util.Properties";
      }
      if (header == HeaderByte.TIME_UNIT.toByte()) {
        return "java.util.concurrent.TimeUnit";
      }
      if (header == HeaderByte.USER_CLASS.toByte()) {
        byte userClassDSId = in.readByte();
        return "DataSerializer: with Id:" + userClassDSId;
      }
      if (header == HeaderByte.USER_CLASS_2.toByte()) {
        short userClass2DSId = in.readShort();
        return "DataSerializer: with Id:" + userClass2DSId;
      }
      if (header == HeaderByte.USER_CLASS_4.toByte()) {
        int userClass4DSId = in.readInt();
        return "DataSerializer: with Id:" + userClass4DSId;
      }
      if (header == HeaderByte.VECTOR.toByte()) {
        return "java.util.Vector";
      }
      if (header == HeaderByte.STACK.toByte()) {
        return "java.util.Stack";
      }
      if (header == HeaderByte.TREE_MAP.toByte()) {
        return "java.util.TreeMap";
      }
      if (header == HeaderByte.TREE_SET.toByte()) {
        return "java.util.TreeSet";
      }
      if (header == HeaderByte.BOOLEAN_TYPE.toByte()) {
        return "java.lang.Boolean.class";
      }
      if (header == HeaderByte.CHARACTER_TYPE.toByte()) {
        return "java.lang.Character.class";
      }
      if (header == HeaderByte.BYTE_TYPE.toByte()) {
        return "java.lang.Byte.class";
      }
      if (header == HeaderByte.SHORT_TYPE.toByte()) {
        return "java.lang.Short.class";
      }
      if (header == HeaderByte.INTEGER_TYPE.toByte()) {
        return "java.lang.Integer.class";
      }
      if (header == HeaderByte.LONG_TYPE.toByte()) {
        return "java.lang.Long.class";
      }
      if (header == HeaderByte.FLOAT_TYPE.toByte()) {
        return "java.lang.Float.class";
      }
      if (header == HeaderByte.DOUBLE_TYPE.toByte()) {
        return "java.lang.Double.class";
      }
      if (header == HeaderByte.VOID_TYPE.toByte()) {
        return "java.lang.Void.class";
      }
      if (header == HeaderByte.USER_DATA_SERIALIZABLE.toByte()) {
        Instantiator instantiator = InternalInstantiator.getInstantiator(in.readByte());
        return "org.apache.geode.Instantiator:" + instantiator.getInstantiatedClass().getName();
      }
      if (header == HeaderByte.USER_DATA_SERIALIZABLE_2.toByte()) {
        Instantiator instantiator = InternalInstantiator.getInstantiator(in.readShort());
        return "org.apache.geode.Instantiator:" + instantiator.getInstantiatedClass().getName();
      }
      if (header == HeaderByte.USER_DATA_SERIALIZABLE_4.toByte()) {
        Instantiator instantiator = InternalInstantiator.getInstantiator(in.readInt());
        return "org.apache.geode.Instantiator:" + instantiator.getInstantiatedClass().getName();
      }
      if (header == HeaderByte.DATA_SERIALIZABLE.toByte()) {
        return "org.apache.geode.DataSerializable:" + DataSerializer.readClass(in).getName();
      }
      if (header == HeaderByte.SERIALIZABLE.toByte()) {
        String name = null;
        try {
          Object obj = InternalDataSerializer.basicReadObject(getDataInput(bytes));
          name = obj.getClass().getName();
        } catch (ClassNotFoundException e) {
          name = e.getMessage();
        }
        return "java.io.Serializable:" + name;
      }
      if (header == HeaderByte.PDX.toByte()) {
        int typeId = in.readInt();
        return "pdxType:" + typeId;
      }
      if (header == HeaderByte.PDX_ENUM.toByte()) {
        in.readByte(); // dsId is not needed
        int enumId = InternalDataSerializer.readArrayLength(in);
        return "pdxEnum:" + enumId;
      }
      if (header == HeaderByte.GEMFIRE_ENUM.toByte()) {
        String name = DataSerializer.readString(in);
        return "java.lang.Enum:" + name;
      }
      if (header == HeaderByte.PDX_INLINE_ENUM.toByte()) {
        String name = DataSerializer.readString(in);
        return "java.lang.Enum:" + name;
      }
      if (header == HeaderByte.BIG_INTEGER.toByte()) {
        return "java.math.BigInteger";
      }
      if (header == HeaderByte.BIG_DECIMAL.toByte()) {
        return "java.math.BigDecimal";
      }
      if (header == HeaderByte.UUID.toByte()) {
        return "java.util.UUID";
      }
      if (header == HeaderByte.TIMESTAMP.toByte()) {
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
    return new DataInputStream(new ByteArrayInputStream(bytes));
  }
}
