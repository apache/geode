package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.state.ConnectionShiroAuthenticatingStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionHandshakeStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.state.ConnectionShiroAuthorizingStateProcessor;
import org.apache.geode.internal.protocol.state.NoSecurityConnectionStateProcessor;
import org.apache.geode.internal.protocol.state.exception.ConnectionStateException;
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
public class HandshakeRequestOperationHandlerJUnitTest {
  private static final int INVALID_MAJOR_VERSION = 67;
  private static final int INVALID_MINOR_VERSION = 92347;

  private HandshakeRequestOperationHandler handshakeHandler =
      new HandshakeRequestOperationHandler();
  private SerializationService serializationService = new ProtobufSerializationService();
  private ProtobufConnectionHandshakeStateProcessor handshakeStateProcessor;

  @Before
  public void Setup() {
    handshakeStateProcessor =
        new ProtobufConnectionHandshakeStateProcessor(mock(SecurityService.class));
  }

  @Test
  public void testCurrentVersionHandshakeSucceeds() throws Exception {
    ConnectionAPI.HandshakeRequest handshakeRequest =
        generateHandshakeRequest(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
            ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
    MessageExecutionContext messageExecutionContext =
        new MessageExecutionContext(mock(InternalCache.class), null, handshakeStateProcessor);
    Result<ConnectionAPI.HandshakeResponse, ClientProtocol.ErrorResponse> result =
        handshakeHandler.process(serializationService, handshakeRequest, messageExecutionContext);
    ConnectionAPI.HandshakeResponse handshakeResponse = result.getMessage();
    assertTrue(handshakeResponse.getHandshakePassed());
    assertEquals(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
        handshakeResponse.getServerMajorVersion());
    assertEquals(ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE,
        handshakeResponse.getServerMinorVersion());
  }

  @Test
  public void testInvalidMajorVersionFails() throws Exception {
    assertNotEquals(INVALID_MAJOR_VERSION, ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE);

    ConnectionAPI.HandshakeRequest handshakeRequest = generateHandshakeRequest(
        INVALID_MAJOR_VERSION, ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
    MessageExecutionContext messageExecutionContext =
        new MessageExecutionContext(mock(InternalCache.class), null, handshakeStateProcessor);

    verifyHandshakeFails(handshakeRequest, messageExecutionContext);
  }

  @Test
  public void testInvalidMajorVersionProtocolConstantFails() throws Exception {
    ConnectionAPI.HandshakeRequest handshakeRequest =
        generateHandshakeRequest(ConnectionAPI.MajorVersions.INVALID_MAJOR_VERSION_VALUE,
            ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
    MessageExecutionContext messageExecutionContext =
        new MessageExecutionContext(mock(InternalCache.class), null, handshakeStateProcessor);
    verifyHandshakeFails(handshakeRequest, messageExecutionContext);
  }

  private void verifyHandshakeFails(ConnectionAPI.HandshakeRequest handshakeRequest,
      MessageExecutionContext messageExecutionContext) throws Exception {
    Result<ConnectionAPI.HandshakeResponse, ClientProtocol.ErrorResponse> result =
        handshakeHandler.process(serializationService, handshakeRequest, messageExecutionContext);
    ConnectionAPI.HandshakeResponse handshakeResponse = result.getMessage();
    assertFalse(handshakeResponse.getHandshakePassed());
  }

  @Test
  public void testInvalidMinorVersionFails() throws Exception {
    assertNotEquals(INVALID_MINOR_VERSION, ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    ConnectionAPI.HandshakeRequest handshakeRequest = generateHandshakeRequest(
        ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE, INVALID_MINOR_VERSION);
    MessageExecutionContext messageExecutionContext =
        new MessageExecutionContext(mock(InternalCache.class), null, handshakeStateProcessor);

    verifyHandshakeFails(handshakeRequest, messageExecutionContext);
  }

  @Test
  public void testInvalidMinorVersionProtocolConstantFails() throws Exception {
    ConnectionAPI.HandshakeRequest handshakeRequest =
        generateHandshakeRequest(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
            ConnectionAPI.MinorVersions.INVALID_MINOR_VERSION_VALUE);
    MessageExecutionContext messageExecutionContext =
        new MessageExecutionContext(mock(InternalCache.class), null, handshakeStateProcessor);

    verifyHandshakeFails(handshakeRequest, messageExecutionContext);
  }

  @Test
  public void testNoSecurityStateFailsHandshake() throws Exception {
    ConnectionAPI.HandshakeRequest handshakeRequest =
        generateHandshakeRequest(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
            ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
    MessageExecutionContext messageExecutionContext = new MessageExecutionContext(
        mock(InternalCache.class), null, new NoSecurityConnectionStateProcessor());

    try {
      handshakeHandler.process(serializationService, handshakeRequest, messageExecutionContext);
      fail("Handshake in non-handshake state should throw exception");
    } catch (ConnectionStateException ex) {
      assertEquals(ProtocolErrorCode.UNSUPPORTED_OPERATION, ex.getErrorCode());
    }
  }

  @Test
  public void testAuthenticatingStateFailsHandshake() throws Exception {
    ConnectionAPI.HandshakeRequest handshakeRequest =
        generateHandshakeRequest(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
            ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
    MessageExecutionContext messageExecutionContext =
        new MessageExecutionContext(mock(InternalCache.class), null,
            new ConnectionShiroAuthenticatingStateProcessor(mock(SecurityService.class)));

    try {
      handshakeHandler.process(serializationService, handshakeRequest, messageExecutionContext);
      fail("Handshake in non-handshake state should throw exception");
    } catch (ConnectionStateException ex) {
      assertEquals(ProtocolErrorCode.UNSUPPORTED_OPERATION, ex.getErrorCode());
    }
  }

  @Test
  public void testAuthorizingStateFailsHandshake() throws Exception {
    ConnectionAPI.HandshakeRequest handshakeRequest =
        generateHandshakeRequest(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
            ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
    MessageExecutionContext messageExecutionContext =
        new MessageExecutionContext(mock(InternalCache.class), null,
            new ConnectionShiroAuthorizingStateProcessor(mock(SecurityService.class),
                mock(Subject.class)));

    try {
      handshakeHandler.process(serializationService, handshakeRequest, messageExecutionContext);
      fail("Handshake in non-handshake state should throw exception");
    } catch (ConnectionStateException ex) {
      assertEquals(ProtocolErrorCode.UNSUPPORTED_OPERATION, ex.getErrorCode());
    }
  }

  private ConnectionAPI.HandshakeRequest generateHandshakeRequest(int majorVersion,
      int minorVersion) {
    return ConnectionAPI.HandshakeRequest.newBuilder().setMajorVersion(majorVersion)
        .setMinorVersion(minorVersion).build();
  }
}
