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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.google.protobuf.MessageLite;

import org.apache.geode.internal.protocol.protobuf.ProtocolVersion;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.exception.InvalidProtocolMessageException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;

public class MessageUtil {
  public static void performAndVerifyHandshake(Socket socket) throws IOException {
    sendHandshake(socket);
    verifyHandshakeSuccess(socket);
  }

  public static void verifyHandshakeSuccess(Socket socket) throws IOException {
    ProtocolVersion.VersionAcknowledgement handshakeResponse =
        ProtocolVersion.VersionAcknowledgement.parseDelimitedFrom(socket.getInputStream());
    assertTrue(handshakeResponse.getVersionAccepted());
  }

  public static void sendHandshake(Socket socket) throws IOException {
    ProtocolVersion.NewConnectionClientVersion.newBuilder()
        .setMajorVersion(ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
        .setMinorVersion(ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE).build()
        .writeDelimitedTo(socket.getOutputStream());
  }

  public static RegionAPI.GetSizeRequest makeGetSizeRequest(String requestRegion) {
    return RegionAPI.GetSizeRequest.newBuilder().setRegionName(requestRegion).build();
  }

  public static ClientProtocol.Message makeGetSizeRequestMessage(String requestRegion) {
    return ClientProtocol.Message.newBuilder().setGetSizeRequest(makeGetSizeRequest(requestRegion))
        .build();
  }

  public static ClientProtocol.Message createGetRequestMessage() {
    ClientProtocol.Message.Builder messageBuilder = ClientProtocol.Message.newBuilder();
    messageBuilder.setGetRequest(getGetRequestBuilder());
    return messageBuilder.build();
  }

  public static ClientProtocol.Message makePutRequestMessage(
      ProtobufSerializationService serializationService, String requestKey, String requestValue,
      String requestRegion) throws EncodingException {
    BasicTypes.Entry entry = ProtobufUtilities.createEntry(serializationService.encode(requestKey),
        serializationService.encode(requestValue));
    return ProtobufRequestUtilities.createPutRequest(requestRegion, entry);
  }

  public static ClientProtocol.Message makeGetRequestMessage(
      ProtobufSerializationService serializationService, Object requestKey, String requestRegion)
      throws Exception {
    return ProtobufRequestUtilities.createGetRequest(requestRegion,
        serializationService.encode(requestKey));
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

  public static void validateGetResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer, Object expectedValue)
      throws InvalidProtocolMessageException, IOException {

    ClientProtocol.Message response =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(ClientProtocol.Message.MessageTypeCase.GETRESPONSE, response.getMessageTypeCase());
    RegionAPI.GetResponse getResponse = response.getGetResponse();
    BasicTypes.EncodedValue result = getResponse.getResult();
    assertEquals(BasicTypes.EncodedValue.ValueCase.STRINGRESULT, result.getValueCase());
    assertEquals(expectedValue, result.getStringResult());
  }
}
