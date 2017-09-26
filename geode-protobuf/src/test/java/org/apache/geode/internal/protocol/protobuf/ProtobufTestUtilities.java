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
package org.apache.geode.internal.protocol.protobuf;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.protobuf.GeneratedMessageV3;

public class ProtobufTestUtilities {
  public static ByteArrayInputStream messageToByteArrayInputStream(GeneratedMessageV3 message)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    message.writeDelimitedTo(byteArrayOutputStream);
    return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
  }


  public static ClientProtocol.Request createProtobufRequestWithGetRegionNamesRequest(
      RegionAPI.GetRegionNamesRequest getRegionNamesRequest) {
    return ClientProtocol.Request.newBuilder().setGetRegionNamesRequest(getRegionNamesRequest)
        .build();
  }

  public static void verifyHandshake(InputStream inputStream, OutputStream outputStream,
      HandshakeAPI.AuthenticationMode authenticationMode) throws IOException {
    buildHandshakeRequest(authenticationMode).writeDelimitedTo(outputStream);

    HandshakeAPI.HandshakeResponse handshakeResponse =
        HandshakeAPI.HandshakeResponse.parseDelimitedFrom(inputStream);

    assertTrue(handshakeResponse.getOk());
    assertFalse(handshakeResponse.hasError());
  }

  public static HandshakeAPI.HandshakeRequest buildHandshakeRequest(
      HandshakeAPI.AuthenticationMode authenticationMode) {
    return HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(1).setMinor(0))
        .setAuthenticationMode(authenticationMode).build();
  }
}
