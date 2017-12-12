package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.shiro.subject.Subject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.protobuf.Handshake;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.MessageUtil;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.state.ConnectionShiroAuthenticatingStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionHandshakeStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.state.ConnectionShiroAuthorizingStateProcessor;
import org.apache.geode.internal.protocol.state.NoSecurityConnectionStateProcessor;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.UnitTest;

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

@Category(UnitTest.class)
public class HandshakeHandlerJUnitTest {
  private static final int INVALID_MAJOR_VERSION = 67;
  private static final int INVALID_MINOR_VERSION = 92347;

  private HandshakeHandler handshakeHandler = new HandshakeHandler();

  @Test
  public void testCurrentVersionHandshakeSucceeds() throws Exception {
    Handshake.NewConnectionHandshake handshakeRequest =
        generateHandshakeRequest(Handshake.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
            Handshake.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    ByteArrayInputStream inputStream =
        MessageUtil.writeMessageDelimitedToInputStream(handshakeRequest);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    assertTrue(handshakeHandler.handleHandshake(inputStream, outputStream,
        mock(ProtocolClientStatistics.class)));

    Handshake.HandshakeAcknowledgement handshakeResponse = Handshake.HandshakeAcknowledgement
        .parseDelimitedFrom(new ByteArrayInputStream(outputStream.toByteArray()));
    assertTrue(handshakeResponse.getHandshakePassed());
    assertEquals(Handshake.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
        handshakeResponse.getServerMajorVersion());
    assertEquals(Handshake.MinorVersions.CURRENT_MINOR_VERSION_VALUE,
        handshakeResponse.getServerMinorVersion());
  }

  @Test
  public void testInvalidMajorVersionFails() throws Exception {
    assertNotEquals(INVALID_MAJOR_VERSION, Handshake.MajorVersions.CURRENT_MAJOR_VERSION_VALUE);

    Handshake.NewConnectionHandshake handshakeRequest = generateHandshakeRequest(
        INVALID_MAJOR_VERSION, Handshake.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    verifyHandshakeFails(handshakeRequest);

    // Also validate the protobuf INVALID_MAJOR_VERSION_VALUE constant fails
    handshakeRequest = generateHandshakeRequest(Handshake.MajorVersions.INVALID_MAJOR_VERSION_VALUE,
        Handshake.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
    verifyHandshakeFails(handshakeRequest);
  }

  private void verifyHandshakeFails(Handshake.NewConnectionHandshake handshakeRequest)
      throws Exception {
    ByteArrayInputStream inputStream =
        MessageUtil.writeMessageDelimitedToInputStream(handshakeRequest);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    try {
      handshakeHandler.handleHandshake(inputStream, outputStream,
          mock(ProtocolClientStatistics.class));
      fail("Invalid handshake should throw IOException");
    } catch (IOException e) {
      // expected if handshake verification fails
    }

    Handshake.HandshakeAcknowledgement handshakeResponse = Handshake.HandshakeAcknowledgement
        .parseDelimitedFrom(new ByteArrayInputStream(outputStream.toByteArray()));

    assertFalse(handshakeResponse.getHandshakePassed());
  }

  @Test
  public void testInvalidMinorVersionFails() throws Exception {
    assertNotEquals(INVALID_MINOR_VERSION, Handshake.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    Handshake.NewConnectionHandshake handshakeRequest = generateHandshakeRequest(
        Handshake.MajorVersions.CURRENT_MAJOR_VERSION_VALUE, INVALID_MINOR_VERSION);

    verifyHandshakeFails(handshakeRequest);

    // Also validate the protobuf INVALID_MINOR_VERSION_VALUE constant fails
    handshakeRequest = generateHandshakeRequest(Handshake.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
        Handshake.MinorVersions.INVALID_MINOR_VERSION_VALUE);
    verifyHandshakeFails(handshakeRequest);
  }

  private Handshake.NewConnectionHandshake generateHandshakeRequest(int majorVersion,
      int minorVersion) {
    return Handshake.NewConnectionHandshake.newBuilder().setMajorVersion(majorVersion)
        .setMinorVersion(minorVersion).build();
  }
}
