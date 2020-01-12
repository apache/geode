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
package org.apache.geode.experimental.driver;

import java.io.IOException;

import com.google.protobuf.ByteString;

import org.apache.geode.annotations.Experimental;

/**
 * Interface for controlling serialization format of all values transmitted using the
 * protobuf based client-server protocol. Use this serializer by calling
 * {@link DriverFactory#setValueSerializer(ValueSerializer)}
 *
 * On the server side, there must be a registered ValueSerializer with the same id.
 */
@Experimental
public interface ValueSerializer {
  /**
   * Serialize an object into a byte array.
   *
   * @return a byte array, or null to fall back on default serialization
   */
  ByteString serialize(Object object) throws IOException;

  /**
   * Deserialize an object from a byte array.
   */
  Object deserialize(ByteString bytes) throws IOException, ClassNotFoundException;

  /**
   * Return the id of this value format. This should match the id of the ValueSerializer
   * registered on the server.
   */
  String getID();

  /**
   * True if this serializer wants to serialize all values, including primitives like
   * numbers and strings that can be sent as one of the options in EncodedValue.
   *
   */
  boolean supportsPrimitives();
}
