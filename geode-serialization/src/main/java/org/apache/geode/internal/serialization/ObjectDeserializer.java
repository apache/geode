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
 * The class StaticSerialization also provides helper methods for deserializing
 * data.
 */
public interface ObjectDeserializer {

  /**
   * Read an object from the given data input
   */
  <T> T readObject(DataInput input) throws IOException, ClassNotFoundException;

  /**
   * Reads an instance of <code>String</code> from a <code>DataInput</code>. The return value may be
   * <code>null</code>.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   *
   * @see StaticSerialization#writeString
   */
  String readString(DataInput in) throws IOException;

  InetAddress readInetAddress(DataInput in) throws IOException;

  <K, V> HashMap<K, V> readHashMap(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException;

  int readArrayLength(DataInput in) throws IOException;

  String[] readStringArray(DataInput in) throws IOException;

  int[] readIntArray(DataInput in) throws IOException;

  byte[] readByteArray(DataInput in) throws IOException;

  /**
   * When deserializing you may want to invoke a fromData method on an object.
   * Use this method to ensure that the proper fromData method is invoked for
   * backward-compatibility.
   */
  void invokeFromData(Object ds, DataInput in)
      throws IOException, ClassNotFoundException;

}
