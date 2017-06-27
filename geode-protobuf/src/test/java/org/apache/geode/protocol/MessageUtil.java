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

import com.google.protobuf.ByteString;

import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.EncodingTypeTranslator;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.serialization.codec.StringCodec;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.SerializationCodecRegistry;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

import java.nio.ByteBuffer;

public class MessageUtil {
  public static ClientProtocol.Message createGetRequestMessage() {
    ClientProtocol.Message.Builder messageBuilder = ClientProtocol.Message.newBuilder();
    messageBuilder.setMessageHeader(getMessageHeaderBuilder());
    ClientProtocol.Request.Builder requestBuilder = getRequestBuilder();
    requestBuilder.setGetRequest(getGetRequestBuilder());
    messageBuilder.setRequest(requestBuilder);
    return messageBuilder.build();
  }

  public static RegionAPI.PutRequest makePutRequest(String requestKey, String requestValue,
      String requestRegion) throws CodecNotRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecAlreadyRegisteredForTypeException {
    StringCodec stringCodec = getStringCodec();
    BasicTypes.EncodedValue.Builder key =
        BasicTypes.EncodedValue.newBuilder().setEncodingType(BasicTypes.EncodingType.STRING)
            .setValue(ByteString.copyFrom(stringCodec.encode(requestKey)));
    BasicTypes.EncodedValue.Builder value =
        BasicTypes.EncodedValue.newBuilder().setEncodingType(BasicTypes.EncodingType.STRING)
            .setValue(ByteString.copyFrom(stringCodec.encode(requestValue)));
    BasicTypes.Entry.Builder entry = BasicTypes.Entry.newBuilder().setKey(key).setValue(value);
    RegionAPI.PutRequest.Builder putRequestBuilder = RegionAPI.PutRequest.newBuilder();
    putRequestBuilder.setRegionName(requestRegion).setEntry(entry);

    return putRequestBuilder.build();
  }

  public static ClientProtocol.Message makePutRequestMessage(String requestKey, String requestValue,
      String requestRegion, ClientProtocol.MessageHeader header)
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecAlreadyRegisteredForTypeException {
    ClientProtocol.Request request = ClientProtocol.Request.newBuilder()
        .setPutRequest(makePutRequest(requestKey, requestValue, requestRegion)).build();
    return ClientProtocol.Message.newBuilder().setMessageHeader(header).setRequest(request).build();
  }

  public static RegionAPI.GetRequest makeGetRequest(String requestKey, String requestRegion)
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecAlreadyRegisteredForTypeException {
    StringCodec stringCodec = getStringCodec();
    RegionAPI.GetRequest.Builder getRequestBuilder = RegionAPI.GetRequest.newBuilder();
    getRequestBuilder.setRegionName(requestRegion)
        .setKey(BasicTypes.EncodedValue.newBuilder().setEncodingType(BasicTypes.EncodingType.STRING)
            .setValue(ByteString.copyFrom(stringCodec.encode(requestKey))));

    return getRequestBuilder.build();
  }

  public static ClientProtocol.Message makeGetRequestMessage(String requestKey,
      String requestRegion, ClientProtocol.MessageHeader header)
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    ClientProtocol.Request request = ClientProtocol.Request.newBuilder()
        .setGetRequest(makeGetRequest(requestKey, requestRegion)).build();
    return ClientProtocol.Message.newBuilder().setMessageHeader(header).setRequest(request).build();
  }

  public static StringCodec getStringCodec() throws CodecAlreadyRegisteredForTypeException,
      CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException {
    SerializationCodecRegistry serializationCodecRegistry = new SerializationCodecRegistry();
    return (StringCodec) serializationCodecRegistry.getCodecForType(
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.STRING));
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
