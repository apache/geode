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
package org.apache.geode.internal.protocol.protobuf.v1;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.google.protobuf.MessageLite;

import org.apache.geode.internal.protocol.protobuf.Handshake;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;

public class MessageUtil {

  public static void performAndVerifyHandshake(Socket socket) throws IOException {
    sendHandshake(socket);
    verifyHandshakeSuccess(socket);
  }

  public static void verifyHandshakeSuccess(Socket socket) throws IOException {
    Handshake.HandshakeAcknowledgement handshakeResponse =
        Handshake.HandshakeAcknowledgement.parseDelimitedFrom(socket.getInputStream());
    assertTrue(handshakeResponse.getHandshakePassed());
  }

  public static void sendHandshake(Socket socket) throws IOException {
    Handshake.NewConnectionHandshake.newBuilder()
        .setMajorVersion(Handshake.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
        .setMinorVersion(Handshake.MinorVersions.CURRENT_MINOR_VERSION_VALUE).build()
        .writeDelimitedTo(socket.getOutputStream());
  }

  public static RegionAPI.GetRegionRequest makeGetRegionRequest(String requestRegion) {
    return RegionAPI.GetRegionRequest.newBuilder().setRegionName(requestRegion).build();
  }

  public static ClientProtocol.Message makeGetRegionRequestMessage(String requestRegion) {
    ClientProtocol.Request request = ClientProtocol.Request.newBuilder()
        .setGetRegionRequest(makeGetRegionRequest(requestRegion)).build();
    return ClientProtocol.Message.newBuilder().setRequest(request).build();
  }

  public static ClientProtocol.Message createGetRequestMessage() {
    ClientProtocol.Message.Builder messageBuilder = ClientProtocol.Message.newBuilder();
    ClientProtocol.Request.Builder requestBuilder = getRequestBuilder();
    requestBuilder.setGetRequest(getGetRequestBuilder());
    messageBuilder.setRequest(requestBuilder);
    return messageBuilder.build();
  }

  public static ClientProtocol.Message makePutRequestMessage(
      SerializationService serializationService, String requestKey, String requestValue,
      String requestRegion)
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException {
    BasicTypes.Entry entry = ProtobufUtilities.createEntry(
        ProtobufUtilities.createEncodedValue(serializationService, requestKey),
        ProtobufUtilities.createEncodedValue(serializationService, requestValue));

    ClientProtocol.Request request =
        ProtobufRequestUtilities.createPutRequest(requestRegion, entry);
    return ProtobufUtilities.createProtobufMessage(request);
  }

  public static ClientProtocol.Message makeGetRequestMessage(
      SerializationService serializationService, Object requestKey, String requestRegion)
      throws Exception {
    ClientProtocol.Request request = ProtobufRequestUtilities.createGetRequest(requestRegion,
        ProtobufUtilities.createEncodedValue(serializationService, requestKey));
    return ProtobufUtilities.createProtobufMessage(request);
  }

  private static ClientProtocol.Request.Builder getRequestBuilder() {
    return ClientProtocol.Request.newBuilder();
  }

  private static RegionAPI.GetRequest.Builder getGetRequestBuilder() {
    return RegionAPI.GetRequest.newBuilder();
  }

  public static ByteArrayInputStream writeMessageDelimitedToInputStream(MessageLite message) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      message.writeDelimitedTo(output);
      return new ByteArrayInputStream(output.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e); // never happens.
    }
  }
}
