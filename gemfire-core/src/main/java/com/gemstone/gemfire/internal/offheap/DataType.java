/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.offheap;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.internal.EnumInfo;
import com.gemstone.gemfire.pdx.internal.PdxType;

/**
 * Determines the data type of the bytes in an off-heap MemoryBlock. This is
 * used by the tests for inspection of the off-heap memory.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
public class DataType implements DSCODE {

  public static final String getDataType(byte[] bytes) {
    final DataInput in = getDataInput(bytes);
    byte header = 0;
    try {
      header = in.readByte();
    } catch (IOException e) {
      return "IOException: " + e.getMessage();
    }
    try {
      switch (header) {
      case DS_FIXED_ID_BYTE:
      {
        return "com.gemstone.gemfire.internal.DataSerializableFixedID:" 
          + DSFIDFactory.create(in.readByte(), in).getClass().getName();
      }
      case DS_FIXED_ID_SHORT:
      {
        return "com.gemstone.gemfire.internal.DataSerializableFixedID:" 
            + DSFIDFactory.create(in.readShort(), in).getClass().getName();
      }
      case DS_FIXED_ID_INT:
      {
        return "com.gemstone.gemfire.internal.DataSerializableFixedID:" 
            + DSFIDFactory.create(in.readInt(), in).getClass().getName();
      }
      case DS_NO_FIXED_ID:
        return "com.gemstone.gemfire.internal.DataSerializableFixedID:" + DataSerializer.readClass(in).getName();
      case NULL:
        return "null";
      case NULL_STRING:
      case STRING:
      case HUGE_STRING:
      case STRING_BYTES:
      case HUGE_STRING_BYTES:
        return "java.lang.String";
      case CLASS:
        return "java.lang.Class";
      case DATE:
        return "java.util.Date";
      case FILE:
        return "java.io.File";
      case INET_ADDRESS:
        return "java.net.InetAddress";
      case BOOLEAN:
        return "java.lang.Boolean";
      case CHARACTER:
        return "java.lang.Character";
      case BYTE:
        return "java.lang.Byte";
      case SHORT:
        return "java.lang.Short";
      case INTEGER:
        return "java.lang.Integer";
      case LONG:
        return "java.lang.Long";
      case FLOAT:
        return "java.lang.Float";
      case DOUBLE:
        return "java.lang.Double";
      case BYTE_ARRAY:
        return "byte[]";
      case ARRAY_OF_BYTE_ARRAYS:
        return "byte[][]";
      case SHORT_ARRAY:
        return "short[]";
      case STRING_ARRAY:
        return "java.lang.String[]";
      case INT_ARRAY:
        return "int[]";
      case LONG_ARRAY:
        return "long[]";
      case FLOAT_ARRAY:
        return "float[]";
      case DOUBLE_ARRAY:
        return "double[]";
      case BOOLEAN_ARRAY:
        return "boolean[]";
      case CHAR_ARRAY:
        return "char[]";
      case OBJECT_ARRAY:
        return "java.lang.Object[]";
      case ARRAY_LIST:
        return "java.util.ArrayList";
      case LINKED_LIST:
        return "java.util.LinkedList";
      case HASH_SET:
        return "java.util.HashSet";
      case LINKED_HASH_SET:
        return "java.util.LinkedHashSet";
      case HASH_MAP:
        return "java.util.HashMap";
      case IDENTITY_HASH_MAP:
        return "java.util.IdentityHashMap";
      case HASH_TABLE:
        return "java.util.Hashtable";
      //ConcurrentHashMap is written as java.io.serializable
      //case CONCURRENT_HASH_MAP:
      //  return "java.util.concurrent.ConcurrentHashMap";
      case PROPERTIES:
        return "java.util.Properties";
      case TIME_UNIT:
        return "java.util.concurrent.TimeUnit";
      case USER_CLASS:
        byte userClassDSId = in.readByte();
        return "DataSerializer: with Id:" + userClassDSId;
      case USER_CLASS_2:
        short userClass2DSId = in.readShort();
        return "DataSerializer: with Id:" + userClass2DSId;
      case USER_CLASS_4:
        int userClass4DSId = in.readInt();
        return "DataSerializer: with Id:" + userClass4DSId;
      case VECTOR:
        return "java.util.Vector";
      case STACK:
        return "java.util.Stack";
      case TREE_MAP:
        return "java.util.TreeMap";
      case TREE_SET:
        return "java.util.TreeSet";
      case BOOLEAN_TYPE:
        return "java.lang.Boolean.class";
      case CHARACTER_TYPE:
        return "java.lang.Character.class";
      case BYTE_TYPE:
        return "java.lang.Byte.class";
      case SHORT_TYPE:
        return "java.lang.Short.class";
      case INTEGER_TYPE:
        return "java.lang.Integer.class";
      case LONG_TYPE:
        return "java.lang.Long.class";
      case FLOAT_TYPE:
        return "java.lang.Float.class";
      case DOUBLE_TYPE:
        return "java.lang.Double.class";
      case VOID_TYPE:
        return "java.lang.Void.class";
      case USER_DATA_SERIALIZABLE:
      {
        Instantiator instantiator = InternalInstantiator.getInstantiator(in.readByte());
        return "com.gemstone.gemfire.Instantiator:" + instantiator.getInstantiatedClass().getName();
      }
      case USER_DATA_SERIALIZABLE_2:
      {
        Instantiator instantiator = InternalInstantiator.getInstantiator(in.readShort());
        return "com.gemstone.gemfire.Instantiator:" + instantiator.getInstantiatedClass().getName();
      }
      case USER_DATA_SERIALIZABLE_4:
      {
        Instantiator instantiator = InternalInstantiator.getInstantiator(in.readInt());
        return "com.gemstone.gemfire.Instantiator:" + instantiator.getInstantiatedClass().getName();
      }
      case DATA_SERIALIZABLE:
        return "com.gemstone.gemfire.DataSerializable:" + DataSerializer.readClass(in).getName();
      case SERIALIZABLE:
      {
        String name = null;
        try {
          Object obj = InternalDataSerializer.basicReadObject(getDataInput(bytes));
          name = obj.getClass().getName();
        } catch (ClassNotFoundException e) {
          name = e.getMessage();
        }
        return "java.io.Serializable:" + name;
      }
      case PDX:
      {
        int typeId = in.readInt();
        try {
          GemFireCacheImpl gfc = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.");
          PdxType pdxType = gfc.getPdxRegistry().getType(typeId);
          if (pdxType == null) { // fix 52164
            return "com.gemstone.gemfire.pdx.PdxInstance: unknown id=" + typeId; 
          }
          return "com.gemstone.gemfire.pdx.PdxInstance:" + pdxType.getClassName();
        } catch(CacheClosedException e) {
          return "com.gemstone.gemfire.pdx.PdxInstance:PdxRegistryClosed";
        }
      }
      case PDX_ENUM:
      {
        int dsId = in.readByte();
        int tmp = InternalDataSerializer.readArrayLength(in);
        int enumId = (dsId << 24) | (tmp & 0xFFFFFF);
        try {
          GemFireCacheImpl gfc = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.");
          EnumInfo enumInfo = gfc.getPdxRegistry().getEnumInfoById(enumId);
          return "PdxRegistry/java.lang.Enum:" + enumInfo.getClassName();
        } catch(CacheClosedException e) {
          return "PdxRegistry/java.lang.Enum:PdxRegistryClosed";
        }
      }
      case GEMFIRE_ENUM:
      {
        String name = DataSerializer.readString(in);
        return "java.lang.Enum:" + name;
      }
      case PDX_INLINE_ENUM:
      {
        String name = DataSerializer.readString(in);
        return "java.lang.Enum:" + name;
      }
      case BIG_INTEGER:
        return "java.math.BigInteger";
      case BIG_DECIMAL:
        return "java.math.BigDecimal";
      case UUID:
        return "java.util.UUID";
      case TIMESTAMP:
        return "java.sql.Timestamp";
      default:
      }
      return "Unknown header byte: " + header;
    } catch (IOException e) {
      //return "IOException for header byte: " + header;
      throw new Error(e);
    } catch (ClassNotFoundException e) {
      //return "IOException for header byte: " + header;
      throw new Error(e);
    }
  }
  
  public static DataInput getDataInput(byte[] bytes) {
    return new DataInputStream(new ByteArrayInputStream( bytes));
  }
}
