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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.DataSerializer;

/**
 * An interface that contains a bunch of static final values used for the
 * implementation of {@link DataSerializer}.
 * It is basically an Enum and could be changed to one once we drop 1.4.
 * The allowed range of these codes is -128..127 inclusive (i.e. byte).
 *
 * @since GemFire 5.7
 */
public interface DSCODE {
  /**
   * This byte value, -128, has never been used in any GemFire release
   * so far. It might get used in the future to introduce DataSerializer
   * versioning.
   */
  public static final byte RESERVED_FOR_FUTURE_USE = -128;

  public static final byte ILLEGAL = -127;

  // -126..0 unused

  /**
   * A header byte meaning that the next element in the stream is a
   * {@link DataSerializableFixedID} whose id is a single signed byte.
   * @since GemFire 5.7
   */
  public static final byte DS_FIXED_ID_BYTE = 1;
  /**
   * A header byte meaning that the next element in the stream is a
   * {@link DataSerializableFixedID} whose id is a single signed short.
   * @since GemFire 5.7
   */
  public static final byte DS_FIXED_ID_SHORT = 2;
  /**
   * A header byte meaning that the next element in the stream is a
   * {@link DataSerializableFixedID} whose id is a single signed int.
   * @since GemFire 5.7
   */
  public static final byte DS_FIXED_ID_INT = 3;
  /**
   * A header byte meaning that the next element in the stream is a
   * {@link DataSerializableFixedID} whose id is <code>NO_FIXED_ID</code>.
   * @since GemFire 5.7
   */
  public static final byte DS_NO_FIXED_ID = 4;

  /** A header byte meaning that the object was serialized by a user's
   * <code>DataSerializer</code> and the id is encoded with 2 bytes.
   * @since GemFire 5.7
   */
  public static final byte USER_CLASS_2 = 5;

  /** A header byte meaning that the object was serialized by a user's
   * <code>DataSerializer</code> and the id is encoded with 4 bytes.
   * @since GemFire 5.7
   */
  public static final byte USER_CLASS_4 = 6;

  // TypeIds 7 and 8 reserved for use by C# Serializable and XmlSerializable.

  // 9 unused

  /** A header byte meaning that the next element in the stream is a
   * <code>LinkedList</code>.
   */
  public static final byte LINKED_LIST = 10;

  /** A header byte meaning that the next element in the stream is a
   * <code>Properties</code>.
   * @since GemFire 5.7
   */
  public static final byte PROPERTIES = 11;

  // 12..16 unused

  /** Codes for the primitive classes, which cannot be recreated with
   * <code>Class.forName(String,boolean,ClassLoader)</code> like other classes
   */
  public static final byte BOOLEAN_TYPE = 17;
  public static final byte CHARACTER_TYPE = 18;
  public static final byte BYTE_TYPE = 19;
  public static final byte SHORT_TYPE = 20;
  public static final byte INTEGER_TYPE = 21;
  public static final byte LONG_TYPE = 22;
  public static final byte FLOAT_TYPE = 23;
  public static final byte DOUBLE_TYPE = 24;
  public static final byte VOID_TYPE = 25;

  /**
   * @since GemFire 5.7
   */
  public static final byte BOOLEAN_ARRAY = 26;
  /**
   * @since GemFire 5.7
   */
  public static final byte CHAR_ARRAY = 27;
  
  // 28..36 unused

  /** A header byte meaning that a DataSerializable that was
   * registered with the Instantiator was data serialized
   * using four bytes for its ID.
   * @since GemFire 5.7
   */
  public static final byte USER_DATA_SERIALIZABLE_4 = 37;

  /** A header byte meaning that a DataSerializable that was
   * registered with the Instantiator was data serialized
   * using two bytes for its ID.
   * @since GemFire 5.7
   */
  public static final byte USER_DATA_SERIALIZABLE_2 = 38;

  /** A header byte meaning that a DataSerializable that was
   * registered with the Instantiator was data serialized
   * using a single byte for its ID.
   */
  public static final byte USER_DATA_SERIALIZABLE = 39;

  /** A header byte meaning that the object was serialized by a user's
   * <code>DataSerializer</code>. */
  public static final byte USER_CLASS = 40;

  /** A header byte meaning that the next element in the stream is a
   * <code>null</code> */
  public static final byte NULL = 41;

  /** A header byte meaning that the next element in the stream is a
   * String */
  public static final byte STRING = 42;

  /** A header byte meaning that the next element in the stream is a
   *  (non-primitive) Class */
  public static final byte CLASS = 43;

  /** A header byte meaning that the next element in the stream is a
   * serialized object */
  public static final byte SERIALIZABLE = 44;

  /** A header byte meaning that the next element in the stream is a
   * DataSerializable object */
  public static final byte DATA_SERIALIZABLE = 45;

  /** A header byte meaning that the next element in the stream is a
   * <code>byte</code> array. */
  public static final byte BYTE_ARRAY = 46;

  /** A header byte meaning that the next element in the stream is a
   * <code>short</code> array. */
  public static final byte SHORT_ARRAY = 47;

  /** A header byte meaning that the next element in the stream is a
   * <code>int</code> array. */
  public static final byte INT_ARRAY = 48;

  /** A header byte meaning that the next element in the stream is a
   * <code>long</code> array. */
  public static final byte LONG_ARRAY = 49;

  /** A header byte meaning that the next element in the stream is a
   * <code>float</code> array. */
  public static final byte FLOAT_ARRAY = 50;

  /** A header byte meaning that the next element in the stream is a
   * <code>double</code> array. */
  public static final byte DOUBLE_ARRAY = 51;

  /** A header byte meaning that the next element in the stream is a
   * <code>Object</code> array. */
  public static final byte OBJECT_ARRAY = 52;

  /** A header boolean meaning that the next element in the stream is a
   * <code>Boolean</code>. */
  public static final byte BOOLEAN = 53;

  /** A header byte meaning that the next element in the stream is a
   * <code>Character</code>. */
  public static final byte CHARACTER = 54;

  /** A header byte meaning that the next element in the stream is a
   * <code>Byte</code>. */
  public static final byte BYTE = 55;

  /** A header byte meaning that the next element in the stream is a
   * <code>Short</code>. */
  public static final byte SHORT = 56;

  /** A header byte meaning that the next element in the stream is a
   * <code>Integer</code>. */
  public static final byte INTEGER = 57;

  /** A header byte meaning that the next element in the stream is a
   * <code>Long</code>. */
  public static final byte LONG = 58;

  /** A header byte meaning that the next element in the stream is a
   * <code>Float</code>. */
  public static final byte FLOAT = 59;

  /** A header byte meaning that the next element in the stream is a
   * <code>Double</code>. */
  public static final byte DOUBLE = 60;

  /** A header byte meaning that the next element in the stream is a
   * <code>Date</code>. */
  public static final byte DATE = 61;

  /** A header byte meaning that the next element in the stream is a
   * <code>InetAddress</code>. */
  public static final byte INET_ADDRESS = 62;

  /** A header byte meaning that the next element in the stream is a
   * <code>File</code>. */
  public static final byte FILE = 63;

  /** A header byte meaning that the next element in the stream is a
   * <code>String</code> array. */
  public static final byte STRING_ARRAY = 64;

  /** A header byte meaning that the next element in the stream is a
   * <code>ArrayList</code>. */
  public static final byte ARRAY_LIST = 65;

  /** A header byte meaning that the next element in the stream is a
   * <code>HashSet</code>. */
  public static final byte HASH_SET = 66;

  /** A header byte meaning that the next element in the stream is a
   * <code>HashMap</code>. */
  public static final byte HASH_MAP = 67;

  /** A header byte meaning that the next element in the stream is a
   * <code>TimeUnit</code>. */
  public static final byte TIME_UNIT = 68;

  /** A header byte meaning that the next element in the stream is a
   * <code>null</code> <code>String</code>. */
  public static final byte NULL_STRING = 69;

  /** A header byte meaning that the next element in the stream is a
   * <code>Hashtable</code>.
   * @since GemFire 5.7
   */
  public static final byte HASH_TABLE = 70;

  /** A header byte meaning that the next element in the stream is a
   * <code>Vector</code>.
   * @since GemFire 5.7
   */
  public static final byte VECTOR = 71;

  /** A header byte meaning that the next element in the stream is a
   * <code>IdentityHashMap</code>.
   * @since GemFire 5.7
   */
  public static final byte IDENTITY_HASH_MAP = 72;

  /** A header byte meaning that the next element in the stream is a
   * <code>LinkedHashSet</code>.
   * @since GemFire 5.7
   */
  public static final byte LINKED_HASH_SET = 73;

  /** A header byte meaning that the next element in the stream is a
   * <code>Stack</code>.
   * @since GemFire 5.7
   */
  public static final byte STACK = 74;

  /** A header byte meaning that the next element in the stream is a
   * <code>TreeMap</code>.
   * @since GemFire 5.7
   */
  public static final byte TREE_MAP = 75;

  /** A header byte meaning that the next element in the stream is a
   * <code>TreeSet</code>.
   * @since GemFire 5.7
   */
  public static final byte TREE_SET = 76;

  // 75..86 unused

  /** A header byte meaning that the next element in the stream is a
   *  buffer of 1-byte characters to turn into a String whose length
   * is <= 0xFFFF
   */
  public static final byte STRING_BYTES = 87;

  /**
   * A header byte meaning that the next element in the stream is a
   * buffer of 1-byte characters to turn into a String whose length
   * is > 0xFFFF.
   * @since GemFire 5.7
   */
  public static final byte HUGE_STRING_BYTES = 88;

  /**
   * A header byte meaning that the next element in the stream is a
   * buffer of 2-byte characters to turn into a String whose length
   * is > 0xFFFF.
   * @since GemFire 5.7
   */
  public static final byte HUGE_STRING = 89;

  // 90 unused

  /** A header byte meaning that the next element in the stream is a
   * <code>byte[][]</code>. */
  public static final byte ARRAY_OF_BYTE_ARRAYS = 91;

  // 92 unused

  /**
   * A header byte meaning that the next element in the stream is a
   * PdxSerializable object.
   * 
   * @since GemFire 6.6
   */
  public static final byte PDX = 93;
  
  /**
   * A header byte meaning that the next element in the stream is an
   * enum whose type is defined in the pdx registry.
   * 
   * @since GemFire 6.6.2
   */
  public static final byte PDX_ENUM = 94;

  /**
   * java.math.BigInteger
   * @since GemFire 6.6.2
   */
  public static final byte BIG_INTEGER = 95;
  /**
   * java.math.BigDecimal
   * @since GemFire 6.6.2
   */
  public static final byte BIG_DECIMAL = 96;

  /**
   * This code can only be used by PDX. It can't be used for normal
   * DataSerializer writeObject because it would break backward compatibility. A
   * header byte meaning that the next element in the stream is a
   * ConcurrentHashMap object.
   * 
   * @since GemFire 6.6
   */
  public static final byte CONCURRENT_HASH_MAP = 97;

  /**
   * java.util.UUID
   * @since GemFire 6.6.2
   */
  public static final byte UUID = 98;
  /**
   * java.sql.Timestamp
   * @since GemFire 6.6.2
   */
  public static final byte TIMESTAMP = 99;
  
  /**
   * Used for enums that need to always be deserialized into their enum domain class.
   * @since GemFire 6.6.2
   */
  public static final byte GEMFIRE_ENUM = 100;
  /**
   * Used for enums that need to be encoded inline because
   * the pdx registry may not be available.
   * During deserialization this type of enum may be deserialized as a PdxInstance.
   * @since GemFire 6.6.2
   */
  public static final byte PDX_INLINE_ENUM = 101;
  
  /**
   * Used for wildcard searches in soplogs with composite keys.
   * @since GemFire 8.0
   */
  public static final byte WILDCARD = 102;
  
  // 103..127 unused

  // DO NOT USE CODES > 127. They are not "byte".
}
