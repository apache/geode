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
package org.apache.geode.protocol.protobuf.utilities;

import com.google.protobuf.ByteString;
import org.apache.geode.protocol.protobuf.*;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

/**
 * This class contains helper functions for assistance in creating protobuf objects. This class is
 * mainly focused on helper functions which can be used in building BasicTypes for use in other
 * messages or those used to create the top level Message objects.
 *
 * Helper functions specific to creating ClientProtocol.Responses can be found at
 * {@link ProtobufResponseUtilities} Helper functions specific to creating ClientProtocol.Requests
 * can be found at {@link ProtobufRequestUtilities}
 */
public abstract class ProtobufUtilities {
  /**
   * Creates a object containing the type and value encoding of a piece of data
   *
   * @param serializationService - object which knows how to encode objects for the protobuf
   *        protocol {@link ProtobufSerializationService}
   * @param unencodedValue - the value object which is to be encoded
   * @return a protobuf EncodedValue object
   * @throws UnsupportedEncodingTypeException - The object passed doesn't have a corresponding
   *         SerializationType
   * @throws CodecNotRegisteredForTypeException - There isn't a protobuf codec for the
   *         SerializationType of the passed object
   */
  public static BasicTypes.EncodedValue createEncodedValue(
      SerializationService serializationService, Object unencodedValue)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    BasicTypes.EncodingType resultEncodingType =
        EncodingTypeTranslator.getEncodingTypeForObject(unencodedValue);
    byte[] encodedValue = serializationService.encode(resultEncodingType, unencodedValue);
    return BasicTypes.EncodedValue.newBuilder().setEncodingType(resultEncodingType)
        .setValue(ByteString.copyFrom(encodedValue)).build();
  }

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
   * Creates a protobuf key,value pair from unencoded data
   *
   * @param serializationService - object which knows how to encode objects for the protobuf
   *        protocol {@link ProtobufSerializationService}
   * @param unencodedKey - the unencoded key for the entry
   * @param unencodedValue - the unencoded value for the entry
   * @return a protobuf Entry containing the encoded key and value
   * @throws UnsupportedEncodingTypeException - The key or value passed doesn't have a corresponding
   *         SerializationType
   * @throws CodecNotRegisteredForTypeException - There isn't a protobuf codec for the
   *         SerializationType of the passed key or value
   */
  public static BasicTypes.Entry createEntry(SerializationService serializationService,
      Object unencodedKey, Object unencodedValue)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    return createEntry(createEncodedValue(serializationService, unencodedKey),
        createEncodedValue(serializationService, unencodedValue));
  }

  /**
   * This creates a protobuf message containing a ClientProtocol.Response
   *
   * @param messageHeader - The header for the message
   * @param response - The response for the message
   * @return a protobuf Message containing the above parameters
   */
  public static ClientProtocol.Message createProtobufResponse(
      ClientProtocol.MessageHeader messageHeader, ClientProtocol.Response response) {
    return ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setResponse(response)
        .build();
  }

  /**
   * This creates a protobuf message containing a ClientProtocol.Request
   *
   * @param messageHeader - The header for the message
   * @param request - The request for the message
   * @return a protobuf Message containing the above parameters
   */
  public static ClientProtocol.Message createProtobufRequest(
      ClientProtocol.MessageHeader messageHeader, ClientProtocol.Request request) {
    return ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setRequest(request)
        .build();
  }

  /**
   * This builds the MessageHeader for a response which matches an incoming request
   *
   * @param request - The request message that we're responding to.
   * @return the MessageHeader the response to the passed request
   */
  public static ClientProtocol.MessageHeader createMessageHeaderForRequest(
      ClientProtocol.Message request) {
    return createMessageHeader(request.getMessageHeader().getCorrelationId());
  }

  /**
   * This creates a MessageHeader
   *
   * @param correlationId - An identifier used to correlate requests and responses
   * @return a MessageHeader containing the above parameters
   */
  public static ClientProtocol.MessageHeader createMessageHeader(int correlationId) {
    return ClientProtocol.MessageHeader.newBuilder().setCorrelationId(correlationId).build();
  }

  /**
   * This will return the object encoded in a protobuf EncodedValue
   *
   * @param serializationService - object which knows how to encode objects for the protobuf
   *        protocol {@link ProtobufSerializationService}
   * @param encodedValue - The value to be decoded
   * @return the object encoded in the passed encodedValue
   * @throws UnsupportedEncodingTypeException - There isn't a SerializationType matching the
   *         encodedValues type
   * @throws CodecNotRegisteredForTypeException - There isn't a protobuf codec for the
   *         SerializationType matching the encodedValues type
   */
  public static Object decodeValue(SerializationService serializationService,
      BasicTypes.EncodedValue encodedValue)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    BasicTypes.EncodingType encoding = encodedValue.getEncodingType();
    byte[] bytes = encodedValue.getValue().toByteArray();
    return serializationService.decode(encoding, bytes);
  }
}
