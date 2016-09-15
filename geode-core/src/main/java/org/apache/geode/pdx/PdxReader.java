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
package org.apache.geode.pdx;

import java.util.Date;

/**
 * A PdxReader will be passed to {@link PdxSerializable#fromData(PdxReader) fromData} or 
 * {@link PdxSerializer#fromData(Class, PdxReader) PdxSerializer fromData} by GemFire during deserialization of 
 * a PDX. The domain class needs to deserialize field members 
 * using this interface. This interface is implemented by GemFire.
 * Each readXXX call will return the field's value. If the serialized 
 * PDX does not contain the named field then a default value will 
 * be returned. Standard Java defaults are used. For Objects this is 
 * <code>null</code> and for primitives it is <code>0</code> or <code>0.0</code>.
 * <P> You <em>must</em> read fields in the same order they were written by {@link PdxWriter}.
 * <P> The methods on this interface are not thread safe so do not
 * call them concurrently, on the same instance, from more than one thread.
 * 
 * @since GemFire 6.6
 */
public interface PdxReader {

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>char</code> field.
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public char readChar(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>boolean</code> field.
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public boolean readBoolean(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>byte</code> field.
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public byte readByte(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>short</code> field.
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public short readShort(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>int</code> field.
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public int readInt(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>long</code> field.
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public long readLong(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>float</code> field.
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public float readFloat(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>double</code> field.
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public double readDouble(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>String</code> field.
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public String readString(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not an <code>Object</code> field.
   */
  public Object readObject(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>char[]</code> field.
   */
  public char[] readCharArray(String fieldName);
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>boolean[]</code> field.
   */
  public boolean[] readBooleanArray(String fieldName);
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>byte[]</code> field.
   */
  public byte[] readByteArray(String fieldName);
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>short[]</code> field.
   */
  public short[] readShortArray(String fieldName);
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>int[]</code> field.
   */
  public int[] readIntArray(String fieldName);
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>long[]</code> field.
   */
  public long[] readLongArray(String fieldName);
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>float[]</code> field.
   */
  public float[] readFloatArray(String fieldName);
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>double[]</code> field.
   */
  public double[] readDoubleArray(String fieldName) ;
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>String[]</code> field.
   */
  public String[] readStringArray(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>Object[]</code> field.
   */
  public Object[] readObjectArray(String fieldName);
  
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>byte[][]</code> field.
   */
  public byte[][] readArrayOfByteArrays(String fieldName);
  
  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   * @throws PdxFieldTypeMismatchException if the named field exists and is not a <code>Date</code> field.
   */
  public Date readDate(String fieldName);

  /**
   * Checks if the named field exists and returns the result.
   * <p>This can be useful when writing code that handles more than one version of
   * a PDX class.
   * @param fieldName the name of the field to check
   * @return <code>true</code> if the named field exists; otherwise <code>false</code>
   */
  public boolean hasField(String fieldName);
  
  /**
   * Checks if the named field was {@link PdxWriter#markIdentityField(String) marked} as an identity field.
   * <p>Note that if no fields have been marked then all the fields are used as identity fields even though
   * this method will return <code>false</code> since none of them have been <em>marked</em>.
   * @param fieldName the name of the field to check
   * @return <code>true</code> if the named field exists and was marked as an identify field; otherwise <code>false</code>
   */
  public boolean isIdentityField(String fieldName);

  /**
   * Reads the named field and returns its value.
   * @param fieldName the name of the field to read
   * @return the value of the field if the field exists; otherwise a default value
   * @throws PdxSerializationException if deserialization of the field fails.
   */
  public Object readField(String fieldName);
  
  /**
   * This method returns an object that represents all the unread fields which must be
   * passed to {@link PdxWriter#writeUnreadFields(PdxUnreadFields) writeUnreadFields} in the toData code.
   * <P>Note that if {@link org.apache.geode.cache.CacheFactory#setPdxIgnoreUnreadFields(boolean) setPdxIgnoreUnreadFields}
   * or {@link org.apache.geode.cache.client.ClientCacheFactory#setPdxIgnoreUnreadFields(boolean) client setPdxIgnoreUnreadFields}
   * are set to <code>true</code> then this method will always return an object that has no unread fields.
   *
   * @return an object that represents the unread fields.
   */
  public PdxUnreadFields readUnreadFields();
}
