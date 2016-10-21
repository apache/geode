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

import java.util.Date;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionService;

/**
 * PdxInstanceFactory gives you a way to create PdxInstances. Call the write methods to populate the
 * field data and then call {@link #create()} to produce an actual instance that contains the data.
 * To create a factory call {@link RegionService#createPdxInstanceFactory(String)}. A factory can
 * only create a single instance. To create multiple instances create multiple factories or use
 * {@link PdxInstance#createWriter()} to create subsequent instances. Using
 * {@link PdxInstance#createWriter()} is usually faster.
 * 
 * @since GemFire 6.6.2
 */
public interface PdxInstanceFactory {
  /**
   * Create a {@link PdxInstance}. The instance will contain any data written to this factory using
   * the write methods.
   * 
   * @return the created instance
   * @throws IllegalStateException if called more than once
   */
  public PdxInstance create();

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>char</code>.
   * <p>
   * Java char is mapped to .NET System.Char.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeChar(String fieldName, char value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>boolean</code>.
   * <p>
   * Java boolean is mapped to .NET System.Boolean.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeBoolean(String fieldName, boolean value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>byte</code>.
   * <p>
   * Java byte is mapped to .NET System.SByte.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeByte(String fieldName, byte value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>short</code>.
   * <p>
   * Java short is mapped to .NET System.Int16.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeShort(String fieldName, short value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>int</code>.
   * <p>
   * Java int is mapped to .NET System.Int32.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeInt(String fieldName, int value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>long</code>.
   * <p>
   * Java long is mapped to .NET System.Int64.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeLong(String fieldName, long value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>float</code>.
   * <p>
   * Java float is mapped to .NET System.Float.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeFloat(String fieldName, float value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>double</code>.
   * <p>
   * Java double is mapped to .NET System.Double.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeDouble(String fieldName, double value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>Date</code>.
   * <p>
   * Java Date is mapped to .NET System.DateTime.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeDate(String fieldName, Date value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>String</code>.
   * <p>
   * Java String is mapped to .NET System.String.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeString(String fieldName, String value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>Object</code>.
   * <p>
   * It is best to use one of the other writeXXX methods if your field type will always be XXX. This
   * method allows the field value to be anything that is an instance of Object. This gives you more
   * flexibility but more space is used to store the serialized field.
   * <p>
   * Note that some Java objects serialized with this method may not be compatible with non-java
   * languages. To ensure that only portable objects are serialized use
   * {@link #writeObject(String, Object, boolean)}.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeObject(String fieldName, Object value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>Object</code>.
   * <p>
   * It is best to use one of the other writeXXX methods if your field type will always be XXX. This
   * method allows the field value to be anything that is an instance of Object. This gives you more
   * flexibility but more space is used to store the serialized field.
   * <p>
   * Note that some Java objects serialized with this method may not be compatible with non-java
   * languages. To ensure that only portable objects are serialized set the
   * <code>checkPortability</code> parameter to true. The following is a list of the Java classes
   * that are portable and the .NET class they are mapped to:
   * <ul>
   * <li>instances of {@link PdxSerializable}: .NET class of same name
   * <li>instances of {@link PdxInstance}: .NET class of same name
   * <li>instances serialized by a {@link PdxSerializer}: .NET class of same name
   * <li>java.lang.Byte: System.SByte
   * <li>java.lang.Boolean: System.Boolean
   * <li>java.lang.Character: System.Char
   * <li>java.lang.Short: System.Int16
   * <li>java.lang.Integer: System.Int32
   * <li>java.lang.Long: System.Int64
   * <li>java.lang.Float: System.Float
   * <li>java.lang.Double: System.Double
   * <li>java.lang.String: System.String
   * <li>java.util.Date: System.DateTime
   * <li>byte[]: System.Byte[]
   * <li>boolean[]: System.Boolean[]
   * <li>char[]: System.Char[]
   * <li>short[]: System.Int16[]
   * <li>int[]: System.Int32[]
   * <li>long[]: System.Int64[]
   * <li>float[]: System.Float[]
   * <li>double[]: System.Double[]
   * <li>String[]: System.String[]
   * <li>byte[][]: System.Byte[][]
   * <li>Object[]: System.Collections.Generic.List<Object>
   * <li>java.util.HashMap: System.Collections.Generics.IDictionary<Object, Object>
   * <li>java.util.Hashtable: System.Collections.Hashtable
   * <li>java.util.ArrayList: System.Collections.Generic.IList<Object>
   * <li>java.util.Vector: System.Collections.ArrayList
   * <li>java.util.HashSet: CacheableHashSet
   * <li>java.util.LinkedHashSet: CacheableLinkedHashSet
   * </ul>
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @param checkPortability if true then an exception is thrown if a non-portable object is
   *        serialized
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if checkPortability is true and a non-portable object is
   *         serialized
   * @throws PdxSerializationException if serialization of the field fails.
   * @since GemFire 6.6.2
   */
  public PdxInstanceFactory writeObject(String fieldName, Object value, boolean checkPortability);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>boolean[]</code>.
   * <p>
   * Java boolean[] is mapped to .NET System.Boolean[].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeBooleanArray(String fieldName, boolean[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>char[]</code>.
   * <p>
   * Java char[] is mapped to .NET System.Char[].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeCharArray(String fieldName, char[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>byte[]</code>.
   * <p>
   * Java byte[] is mapped to .NET System.Byte[].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeByteArray(String fieldName, byte[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>short[]</code>.
   * <p>
   * Java short[] is mapped to .NET System.Int16[].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeShortArray(String fieldName, short[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>int[]</code>.
   * <p>
   * Java int[] is mapped to .NET System.Int32[].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeIntArray(String fieldName, int[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>long[]</code>.
   * <p>
   * Java long[] is mapped to .NET System.Int64[].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeLongArray(String fieldName, long[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>float[]</code>.
   * <p>
   * Java float[] is mapped to .NET System.Float[].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeFloatArray(String fieldName, float[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>double[]</code>.
   * <p>
   * Java double[] is mapped to .NET System.Double[].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeDoubleArray(String fieldName, double[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>String[]</code>.
   * <p>
   * Java String[] is mapped to .NET System.String[].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeStringArray(String fieldName, String[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>Object[]</code>.
   * <p>
   * Java Object[] is mapped to .NET System.Collections.Generic.List<Object>. For how each element
   * of the array is a mapped to .NET see {@link #writeObject(String, Object, boolean) writeObject}.
   * Note that this call may serialize elements that are not compatible with non-java languages. To
   * ensure that only portable objects are serialized use
   * {@link #writeObjectArray(String, Object[], boolean)}.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeObjectArray(String fieldName, Object[] value);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>Object[]</code>.
   * <p>
   * Java Object[] is mapped to .NET System.Collections.Generic.List<Object>. For how each element
   * of the array is a mapped to .NET see {@link #writeObject(String, Object, boolean) writeObject}.
   * Note that this call may serialize elements that are not compatible with non-java languages. To
   * ensure that only portable objects are serialized use
   * {@link #writeObjectArray(String, Object[], boolean)}.
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @param checkPortability if true then an exception is thrown if a non-portable object is
   *        serialized
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if checkPortability is true and a non-portable element is
   *         serialized
   * @throws PdxSerializationException if serialization of the field fails.
   * @since GemFire 6.6.2
   */
  public PdxInstanceFactory writeObjectArray(String fieldName, Object[] value,
      boolean checkPortability);

  /**
   * Writes the named field with the given value to the serialized form. The fields type is
   * <code>byte[][]</code>.
   * <p>
   * Java byte[][] is mapped to .NET System.Byte[][].
   * 
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public PdxInstanceFactory writeArrayOfByteArrays(String fieldName, byte[][] value);

  /**
   * Writes the named field with the given value and type to the serialized form. This method uses
   * the <code>fieldType</code> to determine which writeXXX method it should call. If it can not
   * find a specific match to a writeXXX method it will call {@link #writeObject(String, Object)
   * writeObject}. This method may serialize objects that are not portable to non-java languages. To
   * ensure that only objects that are portable to non-java languages are serialized use
   * {@link #writeField(String, Object, Class, boolean)} instead.
   * <p>
   * The fieldTypes that map to a specific method are:
   * <ul>
   * <li>boolean.class: {@link #writeBoolean}
   * <li>byte.class: {@link #writeByte}
   * <li>char.class: {@link #writeChar}
   * <li>short.class: {@link #writeShort}
   * <li>int.class: {@link #writeInt}
   * <li>long.class: {@link #writeLong}
   * <li>float.class: {@link #writeFloat}
   * <li>double.class: {@link #writeDouble}
   * <li>String.class: {@link #writeString}
   * <li>Date.class: {@link #writeDate}
   * <li>boolean[].class: {@link #writeBooleanArray}
   * <li>byte[].class: {@link #writeByteArray}
   * <li>char[].class: {@link #writeCharArray}
   * <li>short[].class: {@link #writeShortArray}
   * <li>int[].class: {@link #writeIntArray}
   * <li>long[].class: {@link #writeLongArray}
   * <li>float[].class: {@link #writeFloatArray}
   * <li>double[].class: {@link #writeDoubleArray}
   * <li>String[].class: {@link #writeStringArray}
   * <li>byte[][].class: {@link #writeArrayOfByteArrays}
   * <li>any other array class: {@link #writeObjectArray}
   * </ul>
   * Note that the object form of primitives, for example Integer.class and Long.class, map to
   * {@link #writeObject(String, Object) writeObject}.
   * 
   * @param fieldName the name of the field to write
   * @param fieldValue the value of the field to write; this parameter's class must extend the
   *        <code>fieldType</code>
   * @param fieldType the type of the field to write
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if serialization of the field fails.
   */
  public <CT, VT extends CT> PdxInstanceFactory writeField(String fieldName, VT fieldValue,
      Class<CT> fieldType);

  /**
   * Writes the named field with the given value and type to the serialized form. This method uses
   * the <code>fieldType</code> to determine which writeXXX method it should call. If it can not
   * find a specific match to a writeXXX method it will call
   * {@link #writeObject(String, Object, boolean) writeObject}. To ensure that only objects that are
   * portable to non-java languages are serialized set the <code>checkPortability</code> parameter
   * to true.
   * <p>
   * The fieldTypes that map to a specific method are:
   * <ul>
   * <li>boolean.class: {@link #writeBoolean}
   * <li>byte.class: {@link #writeByte}
   * <li>char.class: {@link #writeChar}
   * <li>short.class: {@link #writeShort}
   * <li>int.class: {@link #writeInt}
   * <li>long.class: {@link #writeLong}
   * <li>float.class: {@link #writeFloat}
   * <li>double.class: {@link #writeDouble}
   * <li>String.class: {@link #writeString}
   * <li>Date.class: {@link #writeDate}
   * <li>boolean[].class: {@link #writeBooleanArray}
   * <li>byte[].class: {@link #writeByteArray}
   * <li>char[].class: {@link #writeCharArray}
   * <li>short[].class: {@link #writeShortArray}
   * <li>int[].class: {@link #writeIntArray}
   * <li>long[].class: {@link #writeLongArray}
   * <li>float[].class: {@link #writeFloatArray}
   * <li>double[].class: {@link #writeDoubleArray}
   * <li>String[].class: {@link #writeStringArray}
   * <li>byte[][].class: {@link #writeArrayOfByteArrays}
   * <li>any other array class: {@link #writeObjectArray(String, Object[], boolean)}
   * </ul>
   * Note that the object form of primitives, for example Integer.class and Long.class, map to
   * {@link #writeObject(String, Object, boolean) writeObject}.
   * 
   * @param fieldName the name of the field to write
   * @param fieldValue the value of the field to write; this parameter's class must extend the
   *        <code>fieldType</code>
   * @param fieldType the type of the field to write
   * @param checkPortability if true then an exception is thrown if a non-portable object is
   *        serialized
   * @return this PdxInstanceFactory
   * @throws PdxFieldAlreadyExistsException if the named field has already been written
   * @throws PdxSerializationException if checkPortability is true and a non-portable object is
   *         serialized
   * @throws PdxSerializationException if serialization of the field fails.
   * @since GemFire 6.6.2
   */
  public <CT, VT extends CT> PdxInstanceFactory writeField(String fieldName, VT fieldValue,
      Class<CT> fieldType, boolean checkPortability);

  /**
   * Indicate that the named field should be included in hashCode and equals checks of this object
   * on a server that is accessing {@link PdxInstance} or when a client executes a query on a
   * server.
   * 
   * The fields that are marked as identity fields are used to generate the hashCode and equals
   * methods of {@link PdxInstance}. Because of this, the identity fields should themselves either
   * be primitives, or implement hashCode and equals.
   * 
   * If no fields are set as identity fields, then all fields will be used in hashCode and equals
   * checks.
   * 
   * The identity fields should make marked after they are written using a write* method.
   * 
   * @param fieldName the name of the field to mark as an identity field.
   * @return this PdxInstanceFactory
   * @throws PdxFieldDoesNotExistException if the named field has not already been written.
   */
  public PdxInstanceFactory markIdentityField(String fieldName);
}
