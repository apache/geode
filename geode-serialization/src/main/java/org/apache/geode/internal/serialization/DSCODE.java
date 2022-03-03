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
package org.apache.geode.internal.serialization;

/**
 * An enumeration that contains a bunch of pre-defined values for use in serialization
 * The allowed range of these values is -128..127 inclusive (i.e., one byte). These values
 * are used in serialization to note the type of the data that follows the DSCODE. This is
 * only useful for very advanced serialization code dealing with serialization internals.
 * Examples of its use can be found in the geode-serialization and geode-core modules.
 */
public enum DSCODE {
  /**
   * This byte value, -128, has never been used in any GemFire release so far. It might get used in
   * the future to introduce DataSerializer versioning.
   */
  RESERVED_FOR_FUTURE_USE(-128),

  ILLEGAL(-127),

  // -126..0 unused

  /**
   * A header byte meaning that the next element in the stream is a {@link DataSerializableFixedID}
   * whose id is a single signed byte.
   *
   * @since GemFire 5.7
   */
  DS_FIXED_ID_BYTE(1),
  /**
   * A header byte meaning that the next element in the stream is a {@link DataSerializableFixedID}
   * whose id is a single signed short.
   *
   * @since GemFire 5.7
   */
  DS_FIXED_ID_SHORT(2),
  /**
   * A header byte meaning that the next element in the stream is a {@link DataSerializableFixedID}
   * whose id is a single signed int.
   *
   * @since GemFire 5.7
   */
  DS_FIXED_ID_INT(3),
  /**
   * A header byte meaning that the next element in the stream is a {@link DataSerializableFixedID}
   * whose id is <code>NO_FIXED_ID</code>.
   *
   * @since GemFire 5.7
   */
  DS_NO_FIXED_ID(4),

  /**
   * A header byte meaning that the object was serialized by a user's <code>DataSerializer</code>
   * and the id is encoded with 2 bytes.
   *
   * @since GemFire 5.7
   */
  USER_CLASS_2(5),

  /**
   * A header byte meaning that the object was serialized by a user's <code>DataSerializer</code>
   * and the id is encoded with 4 bytes.
   *
   * @since GemFire 5.7
   */
  USER_CLASS_4(6),

  // TypeIds 7 and 8 reserved for use by C# Serializable and XmlSerializable.

  // 9 unused

  /**
   * A header byte meaning that the next element in the stream is a <code>LinkedList</code>.
   */
  LINKED_LIST(10),

  /**
   * A header byte meaning that the next element in the stream is a <code>Properties</code>.
   *
   * @since GemFire 5.7
   */
  PROPERTIES(11),

  // 12..16 unused

  /**
   * Codes for the primitive classes, which cannot be recreated with
   * <code>Class.forName(String,boolean,ClassLoader)</code> like other classes
   */
  BOOLEAN_TYPE(17), CHARACTER_TYPE(18), BYTE_TYPE(19), SHORT_TYPE(20), INTEGER_TYPE(21), LONG_TYPE(22), FLOAT_TYPE(23), DOUBLE_TYPE(24), VOID_TYPE(25),

  /**
   * @since GemFire 5.7
   */
  BOOLEAN_ARRAY(26),
  /**
   * @since GemFire 5.7
   */
  CHAR_ARRAY(27),

  // 28..36 unused

  /**
   * A header byte meaning that a DataSerializable that was registered with the Instantiator was
   * data serialized using four bytes for its ID.
   *
   * @since GemFire 5.7
   */
  USER_DATA_SERIALIZABLE_4(37),

  /**
   * A header byte meaning that a DataSerializable that was registered with the Instantiator was
   * data serialized using two bytes for its ID.
   *
   * @since GemFire 5.7
   */
  USER_DATA_SERIALIZABLE_2(38),

  /**
   * A header byte meaning that a DataSerializable that was registered with the Instantiator was
   * data serialized using a single byte for its ID.
   */
  USER_DATA_SERIALIZABLE(39),

  /**
   * A header byte meaning that the object was serialized by a user's <code>DataSerializer</code>.
   */
  USER_CLASS(40),

  /**
   * A header byte meaning that the next element in the stream is a <code>null</code>
   */
  NULL(41),

  /**
   * A header byte meaning that the next element in the stream is a String
   */
  STRING(42),

  /**
   * A header byte meaning that the next element in the stream is a (non-primitive) Class
   */
  CLASS(43),

  /**
   * A header byte meaning that the next element in the stream is a serialized object
   */
  SERIALIZABLE(44),

  /**
   * A header byte meaning that the next element in the stream is a DataSerializable object
   */
  DATA_SERIALIZABLE(45),

  /**
   * A header byte meaning that the next element in the stream is a <code>byte</code> array.
   */
  BYTE_ARRAY(46),

  /**
   * A header byte meaning that the next element in the stream is a <code>short</code> array.
   */
  SHORT_ARRAY(47),

  /**
   * A header byte meaning that the next element in the stream is a <code>int</code> array.
   */
  INT_ARRAY(48),

  /**
   * A header byte meaning that the next element in the stream is a <code>long</code> array.
   */
  LONG_ARRAY(49),

  /**
   * A header byte meaning that the next element in the stream is a <code>float</code> array.
   */
  FLOAT_ARRAY(50),

  /**
   * A header byte meaning that the next element in the stream is a <code>double</code> array.
   */
  DOUBLE_ARRAY(51),

  /**
   * A header byte meaning that the next element in the stream is a <code>Object</code> array.
   */
  OBJECT_ARRAY(52),

  /**
   * A header boolean meaning that the next element in the stream is a <code>Boolean</code>.
   */
  BOOLEAN(53),

  /**
   * A header byte meaning that the next element in the stream is a <code>Character</code>.
   */
  CHARACTER(54),

  /**
   * A header byte meaning that the next element in the stream is a <code>Byte</code>.
   */
  BYTE(55),

  /**
   * A header byte meaning that the next element in the stream is a <code>Short</code>.
   */
  SHORT(56),

  /**
   * A header byte meaning that the next element in the stream is a <code>Integer</code>.
   */
  INTEGER(57),

  /**
   * A header byte meaning that the next element in the stream is a <code>Long</code>.
   */
  LONG(58),

  /**
   * A header byte meaning that the next element in the stream is a <code>Float</code>.
   */
  FLOAT(59),

  /**
   * A header byte meaning that the next element in the stream is a <code>Double</code>.
   */
  DOUBLE(60),

  /**
   * A header byte meaning that the next element in the stream is a <code>Date</code>.
   */
  DATE(61),

  /**
   * A header byte meaning that the next element in the stream is a <code>InetAddress</code>.
   */
  INET_ADDRESS(62),

  /**
   * A header byte meaning that the next element in the stream is a <code>File</code>.
   */
  FILE(63),

  /**
   * A header byte meaning that the next element in the stream is a <code>String</code> array.
   */
  STRING_ARRAY(64),

  /**
   * A header byte meaning that the next element in the stream is a <code>ArrayList</code>.
   */
  ARRAY_LIST(65),

  /**
   * A header byte meaning that the next element in the stream is a <code>HashSet</code>.
   */
  HASH_SET(66),

  /**
   * A header byte meaning that the next element in the stream is a <code>HashMap</code>.
   */
  HASH_MAP(67),

  /**
   * A header byte meaning that the next element in the stream is a <code>TimeUnit</code>.
   */
  TIME_UNIT(68),

  /**
   * A header byte meaning that the next element in the stream is a <code>null</code>
   * <code>String</code>.
   */
  NULL_STRING(69),

  /**
   * A header byte meaning that the next element in the stream is a <code>Hashtable</code>.
   *
   * @since GemFire 5.7
   */
  HASH_TABLE(70),

  /**
   * A header byte meaning that the next element in the stream is a <code>Vector</code>.
   *
   * @since GemFire 5.7
   */
  VECTOR(71),

  /**
   * A header byte meaning that the next element in the stream is a <code>IdentityHashMap</code>.
   *
   * @since GemFire 5.7
   */
  IDENTITY_HASH_MAP(72),

  /**
   * A header byte meaning that the next element in the stream is a <code>LinkedHashSet</code>.
   *
   * @since GemFire 5.7
   */
  LINKED_HASH_SET(73),

  /**
   * A header byte meaning that the next element in the stream is a <code>Stack</code>.
   *
   * @since GemFire 5.7
   */
  STACK(74),

  /**
   * A header byte meaning that the next element in the stream is a <code>TreeMap</code>.
   *
   * @since GemFire 5.7
   */
  TREE_MAP(75),

  /**
   * A header byte meaning that the next element in the stream is a <code>TreeSet</code>.
   *
   * @since GemFire 5.7
   */
  TREE_SET(76),

  // 75..86 unused

  /**
   * A header byte meaning that the next element in the stream is a buffer of 1-byte characters to
   * turn into a String whose length is <= 0xFFFF
   */
  STRING_BYTES(87),

  /**
   * A header byte meaning that the next element in the stream is a buffer of 1-byte characters to
   * turn into a String whose length is > 0xFFFF.
   *
   * @since GemFire 5.7
   */
  HUGE_STRING_BYTES(88),

  /**
   * A header byte meaning that the next element in the stream is a buffer of 2-byte characters to
   * turn into a String whose length is > 0xFFFF.
   *
   * @since GemFire 5.7
   */
  HUGE_STRING(89),

  // 90 unused

  /**
   * A header byte meaning that the next element in the stream is a <code>byte[][]</code>.
   */
  ARRAY_OF_BYTE_ARRAYS(91),

  // 92 unused

  /**
   * A header byte meaning that the next element in the stream is a PdxSerializable object.
   *
   * @since GemFire 6.6
   */
  PDX(93),

  /**
   * A header byte meaning that the next element in the stream is an enum whose type is defined in
   * the pdx registry.
   *
   * @since GemFire 6.6.2
   */
  PDX_ENUM(94),

  /**
   * java.math.BigInteger
   *
   * @since GemFire 6.6.2
   */
  BIG_INTEGER(95),
  /**
   * java.math.BigDecimal
   *
   * @since GemFire 6.6.2
   */
  BIG_DECIMAL(96),

  /**
   * This code can only be used by PDX. It can't be used for normal DataSerializer writeObject
   * because it would break backward compatibility. A header byte meaning that the next element in
   * the stream is a ConcurrentHashMap object.
   *
   * @since GemFire 6.6
   */
  CONCURRENT_HASH_MAP(97),

  /**
   * java.util.UUID
   *
   * @since GemFire 6.6.2
   */
  UUID(98),
  /**
   * java.sql.Timestamp
   *
   * @since GemFire 6.6.2
   */
  TIMESTAMP(99),

  /**
   * Used for enums that need to always be deserialized into their enum domain class.
   *
   * @since GemFire 6.6.2
   */
  GEMFIRE_ENUM(100),
  /**
   * Used for enums that need to be encoded inline because the pdx registry may not be available.
   * During deserialization this type of enum may be deserialized as a PdxInstance.
   *
   * @since GemFire 6.6.2
   */
  PDX_INLINE_ENUM(101),

  /**
   * Used for wildcard searches in soplogs with composite keys.
   *
   * @since GemFire 8.0
   */
  WILDCARD(102);

  // 103..127 unused

  // DO NOT USE CODES > 127. They are not "byte".

  private final byte value;

  DSCODE(int value) {
    this.value = (byte) value;
  }

  public byte toByte() {
    return value;
  }
}
