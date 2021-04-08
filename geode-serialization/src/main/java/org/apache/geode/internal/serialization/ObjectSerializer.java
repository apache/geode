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
 * The class StaticSerialization also provides helper methods for serializing
 * data.
 */

public interface ObjectSerializer {

  /**
   * serialize an object to the given data-output
   */
  void writeObject(Object obj, DataOutput output) throws IOException;

  void writeInetAddress(InetAddress address, DataOutput out) throws IOException;

  void writeByteArray(byte[] array, DataOutput out) throws IOException;

  void writeArrayLength(int len, DataOutput out) throws IOException;

  void writeString(String value, DataOutput out) throws IOException;

  void writeStringArray(String[] array, DataOutput out) throws IOException;

  void writeInteger(Integer value, DataOutput out) throws IOException;

  void writeIntArray(int[] array, DataOutput out) throws IOException;

  void writeHashMap(Map<?, ?> map, DataOutput out, SerializationContext context)
      throws IOException;

  /**
   * When serializing you may want to invoke a toData method on an object.
   * Use this method to ensure that the proper toData method is invoked for
   * backward-compatibility.
   */
  void invokeToData(Object ds, DataOutput out) throws IOException;
}
