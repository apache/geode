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

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;

/**
 * An ObjectDeserializer is held by a DSFIDSerializer serialization service. It
 * is passed to the fromData() method of a DataSerializableFixedID embedded in a
 * DeserializationContext for use in deserializing data from an input data stream.
 */
public interface ObjectDeserializer {

  /**
   * Read an object from the given data input
   *
   * @param input the input stream.
   *
   * @throws IOException A problem occurs while reading from <code>input</code>
   * @throws ClassNotFoundException The class of an object read from <code>input</code> cannot
   *         be found.
   */
  <T> T readObject(DataInput input) throws IOException, ClassNotFoundException;

  /**
   * Reads an instance of <code>String</code> from a <code>DataInput</code>. The return value may be
   * <code>null</code>.
   *
   * @param input the input stream.
   *
   * @throws IOException A problem occurs while reading from <code>input</code>
   *
   * @see StaticSerialization#writeString
   */
  String readString(DataInput input) throws IOException;

  /**
   * Reads an instance of <code>InetAddress</code> from a <code>DataInput</code>. The return value
   * may be <code>null</code>.
   *
   * @param input the input stream.
   *
   * @throws IOException A problem occurs while reading from <code>input</code> or the address read
   *         from <code>input</code> is unknown
   */
  InetAddress readInetAddress(DataInput input) throws IOException;

  /**
   * Reads a <code>HashMap</code> from a <code>DataInput</code>.
   *
   * @param input the input stream.
   *
   * @throws IOException A problem occurs while reading from <code>input</code>
   * @throws ClassNotFoundException The class of one of the <Code>HashMap</code>'s elements cannot
   *         be found.
   */
  <K, V> HashMap<K, V> readHashMap(DataInput input, DeserializationContext context)
      throws IOException, ClassNotFoundException;

  /**
   * Reads the length of an array from a <code>DataInput</code>.
   *
   * @param input the input stream.
   *
   * @throws IOException A problem occurs while reading from <code>input</code>
   */
  int readArrayLength(DataInput input) throws IOException;

  /**
   * Reads an array of <code>String</code>s from a <code>DataInput</code>.
   *
   * @param input the input stream.
   *
   * @throws IOException A problem occurs while reading from <code>input</code>
   */
  String[] readStringArray(DataInput input) throws IOException;

  /**
   * Reads an <code>int</code> array from a <code>DataInput</code>.
   *
   * @param input the input stream.
   *
   * @throws IOException A problem occurs while reading from <code>input</code>
   */
  int[] readIntArray(DataInput input) throws IOException;

  /**
   * Reads an array of <code>byte</code>s from a <code>DataInput</code>.
   *
   * @param input the input stream.
   *
   * @throws IOException A problem occurs while reading from <code>input</code>
   */
  byte[] readByteArray(DataInput input) throws IOException;

  /**
   * When deserializing you may want to invoke a fromData method on an object.
   * Use this method to ensure that the proper fromData method is invoked for
   * backward-compatibility.
   *
   * @param ds the object to to invoke a fromData method on
   * @param input the input stream.
   *
   * @throws IOException A problem occurs while reading from <code>input</code>
   * @throws ClassNotFoundException The class of an object cannot be found.
   */
  void invokeFromData(Object ds, DataInput input)
      throws IOException, ClassNotFoundException;

}
