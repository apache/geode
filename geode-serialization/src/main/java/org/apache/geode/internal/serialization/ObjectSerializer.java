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

import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

/**
 * An ObjectSerializer is held by a DSFIDSerializer serialization service. It
 * is passed to the toData() method of a DataSerializableFixedID embedded in a
 * SerializationContext for use in serializing data from an output data stream.
 */

public interface ObjectSerializer {

  /**
   * serialize an object to the given data-output
   *
   * @param obj the object to be written.
   * @param output the output stream.
   *
   * @exception IOException A problem occurs while writing to <code>output</code>.
   */
  void writeObject(Object obj, DataOutput output) throws IOException;

  /**
   * Writes an instance of <code>InetAddress</code> to a <code>DataOutput</code>.
   *
   * @param address the address to be written.
   * @param output the output stream.
   *
   * @throws IOException A problem occurs while writing to <code>output</code>.
   */
  void writeInetAddress(InetAddress address, DataOutput output) throws IOException;

  /**
   * Writes an array of <code>byte</code>s to a <code>DataOutput</code>.
   *
   * @param array the array to be written.
   * @param output the output stream.
   *
   * @throws IOException A problem occurs while writing to <code>output</code>.
   */
  void writeByteArray(byte[] array, DataOutput output) throws IOException;

  /**
   * Writes the length of an array to a <code>DataOutput</code>.
   *
   * @param len the length of an array.
   * @param output the output stream.
   *
   * @throws IOException A problem occurs while writing to <code>output</code>.
   */
  void writeArrayLength(int len, DataOutput output) throws IOException;

  /**
   * Writes an instance of <code>String</code> to a <code>DataOutput</code>.
   *
   * @param value the String value to be written.
   * @param output the output stream.
   *
   * @throws IOException A problem occurs while writing to <code>output</code>.
   */
  void writeString(String value, DataOutput output) throws IOException;

  /**
   * Writes an array of <code>String</code>s to a <code>DataOutput</code>.
   *
   * @param array the array to be written.
   * @param output the output stream.
   *
   * @throws IOException A problem occurs while writing to <code>output</code>.
   */
  void writeStringArray(String[] array, DataOutput output) throws IOException;

  /**
   * Writes an instance of <code>Integer</code> to a <code>DataOutput</code>.
   *
   * @param value the Integer value to be written.
   * @param output the output stream.
   *
   * @throws IOException A problem occurs while writing to <code>output</code>.
   */
  void writeInteger(Integer value, DataOutput output) throws IOException;

  /**
   * Writes an <code>int</code> array to a <code>DataOutput</code>.
   *
   * @param array the array to be written.
   * @param output the output stream.
   *
   * @throws IOException A problem occurs while writing to <code>output</code>.
   */
  void writeIntArray(int[] array, DataOutput output) throws IOException;

  /**
   * Writes an instance of <code>Map</code> to a <code>DataOutput</code>.
   *
   * @param map the address to be written.
   * @param output the output stream.
   * @param context the serialization context.
   *
   * @throws IOException A problem occurs while writing to <code>output</code>.
   */
  void writeHashMap(Map<?, ?> map, DataOutput output, SerializationContext context)
      throws IOException;

  /**
   * When serializing you may want to invoke a toData method on an object.
   * Use this method to ensure that the proper toData method is invoked for
   * backward-compatibility.
   *
   * @param ds the object to to invoke a toData method on.
   * @param output the output stream.
   *
   * @throws IOException A problem occurs while writing to <code>output</code>.
   */
  void invokeToData(Object ds, DataOutput output) throws IOException;
}
