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
package org.apache.geode.protocol.serialization;

import java.io.IOException;

import com.google.protobuf.ByteString;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.services.module.ModuleService;

/**
 * Interface for controlling serialization format of all values transmitted using the
 * protobuf based client-server protocol.
 *
 * To use a custom format, implement this interface and register your format by
 * adding a META-INF/services/org.apache.geode.protocol.serialization.ValueSerializer file
 * that contains the full qualified name of your serializer. It will be loaded and registered
 * using {@link ModuleService} mechanism.
 *
 * Clients can than elect to use the registered format by sending a HandshakeRequest with
 * and setting the valueFormat field in the request to match the string returned by {@link #getID()}
 *
 * If clients elect to use this ValueSerializer, all values that are sent as an EncodedValue
 * can be sent as EncodedValue.customObjectResult. The server will use the
 * {@link #deserialize(ByteString)} method to deserialize them. When the server is sending values
 * to the client, it will use {@link #serialize(Object)} to serialize the value and send the
 * response to the client as a EncodedValue.customObjectResult.
 *
 * Implementations should be *threadsafe*. No guarantees are made about whether the server will
 * create one or more instances of this serializer or how they will be shared amongst server
 * threads.
 * {@link #init(Cache)} will be called before the {@link #serialize(Object)} and
 * {@link #deserialize(ByteString)}.
 */
@Experimental
public interface ValueSerializer {

  /**
   * Serialize an object into bytes that can be sent to a client.
   *
   * @return the bytes to send to the client
   *
   * @param object Object to serialize, which may be null if {@link #supportsPrimitives() is true}.
   *        If the
   *        object to be sent is serialized in PDX form, this object may be a {@link PdxInstance},
   *        even if {@link Cache#getPdxReadSerialized()} is false
   */
  public ByteString serialize(Object object) throws IOException;

  /**
   * Deserialize bytes that are sent from the client into an object. This method can
   * return a {@link PdxInstance} for PDX data to avoid having to actually have classes
   * present on the server.
   *
   * @param bytes the bytes to deserialize. Will not be null.
   * @return the deserialized version of the object
   */
  public Object deserialize(ByteString bytes) throws IOException, ClassNotFoundException;

  /**
   * Initialize this serializer. This method is called when a new connection is established that
   * requests this format.
   *
   */
  void init(Cache cache);

  /**
   * Unique identifier for this serializer. Client's must set the valueFormat field of the
   * HandshakeRequest to this value in order to use this format.
   */
  String getID();

  /**
   * True if this serializer wants to serialize all values, including primitives like
   * numbers and strings that can be sent as one of the options in EncodedValue.
   *
   */
  default boolean supportsPrimitives() {
    return false;
  }
}
