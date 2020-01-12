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
package org.apache.geode.internal.protocol.protobuf.v1.utilities;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;

/**
 * This class contains helper functions for assistance in creating protobuf objects. This class is
 * mainly focused on helper functions which can be used in building BasicTypes for use in other
 * messages or those used to create the top level Message objects.
 * <p>
 * Helper functions specific to creating ClientProtocol.Messages can be found in
 * ProtobufRequestUtilities in the test source set of this module.
 */
@Experimental
public abstract class ProtobufUtilities {
  /**
   * Creates a protobuf key,value pair from an encoded key and value
   *
   * @param key - an EncodedValue containing the key of the entry
   * @param value - an EncodedValue containing the value of the entry
   * @return a protobuf Entry object containing the passed key and value
   */
  public static BasicTypes.Entry createEntry(BasicTypes.EncodedValue key,
      BasicTypes.EncodedValue value) {
    return BasicTypes.Entry.newBuilder().setKey(key).setValue(value).build();
  }

  /**
   * Creates a protobuf (key, value) pair from unencoded data. if the value is null, it will be
   * unset in the BasicTypes.Entry.
   *
   * The key MUST NOT be null.
   *
   * @param serializationService - object which knows how to encode objects for the protobuf
   *        protocol {@link ProtobufSerializationService}
   * @param unencodedKey - the unencoded key for the entry
   * @param unencodedValue - the unencoded value for the entry
   * @return a protobuf Entry containing the encoded key and value
   * @throws EncodingException - The key or value passed doesn't have a corresponding
   *         SerializationType
   */
  public static BasicTypes.Entry createEntry(ProtobufSerializationService serializationService,
      Object unencodedKey, Object unencodedValue) throws EncodingException {
    return createEntry(serializationService.encode(unencodedKey),
        serializationService.encode(unencodedValue));
  }

  /**
   * This creates a protobuf message containing a ClientProtocol.Message
   *
   * @param getAllRequest - The request for the message
   * @return a protobuf Message containing the above parameters
   */
  public static ClientProtocol.Message createProtobufRequestWithGetAllRequest(
      RegionAPI.GetAllRequest getAllRequest) {
    return ClientProtocol.Message.newBuilder().setGetAllRequest(getAllRequest).build();
  }
}
