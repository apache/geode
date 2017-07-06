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
package org.apache.geode.protocol;

import org.apache.geode.protocol.protobuf.*;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

public class MessageUtil {
  public static ClientProtocol.Message createGetRequestMessage() {
    ClientProtocol.Message.Builder messageBuilder = ClientProtocol.Message.newBuilder();
    messageBuilder.setMessageHeader(getMessageHeaderBuilder());
    ClientProtocol.Request.Builder requestBuilder = getRequestBuilder();
    requestBuilder.setGetRequest(getGetRequestBuilder());
    messageBuilder.setRequest(requestBuilder);
    return messageBuilder.build();
  }

  public static ClientProtocol.Message makePutRequestMessage(
      SerializationService serializationService, String requestKey, String requestValue,
      String requestRegion, ClientProtocol.MessageHeader header)
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecAlreadyRegisteredForTypeException {
    BasicTypes.Entry entry = ProtobufUtilities.createEntry(
        ProtobufUtilities.createEncodedValue(serializationService, requestKey),
        ProtobufUtilities.createEncodedValue(serializationService, requestValue));

    ClientProtocol.Request request =
        ProtobufRequestUtilities.createPutRequest(requestRegion, entry);
    return ProtobufUtilities.createProtobufRequest(header, request);
  }

  public static ClientProtocol.Message makeGetRequestMessage(
      SerializationService serializationService, Object requestKey, String requestRegion,
      ClientProtocol.MessageHeader header) throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    ClientProtocol.Request request = ProtobufRequestUtilities.createGetRequest(requestRegion,
        ProtobufUtilities.createEncodedValue(serializationService, requestKey));
    return ProtobufUtilities.createProtobufRequest(header, request);
  }

  private static ClientProtocol.Request.Builder getRequestBuilder() {
    return ClientProtocol.Request.newBuilder();
  }

  private static RegionAPI.GetRequest.Builder getGetRequestBuilder() {
    return RegionAPI.GetRequest.newBuilder();
  }

  private static ClientProtocol.MessageHeader.Builder getMessageHeaderBuilder() {
    return ClientProtocol.MessageHeader.newBuilder();
  }
}
