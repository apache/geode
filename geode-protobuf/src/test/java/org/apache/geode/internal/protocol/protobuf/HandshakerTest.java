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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.security.server.Authenticator;
import org.apache.geode.security.server.Authorizer;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class HandshakerTest {

  private Map<String, Class<? extends Authenticator>> authenticatorMap;
  private Handshaker handshaker;

  public static class AuthenticatorMock implements Authenticator {

    @Override
    public void authenticate(InputStream inputStream, OutputStream outputStream,
        SecurityManager securityManager) throws IOException {

    }

    @Override
    public boolean isAuthenticated() {
      return false;
    }

    @Override
    public Authorizer getAuthorizer() throws AuthenticationRequiredException {
      return null;
    }

    @Override
    public String implementationID() {
      return null;
    }
  }

  public static class SimpleMock extends AuthenticatorMock {
  }

  public static class NoopMock extends AuthenticatorMock {
  }

  @Before
  public void setUp() {
    authenticatorMap = new HashMap<>();

    authenticatorMap.put("NOOP", NoopMock.class);
    authenticatorMap.put("SIMPLE", SimpleMock.class);

    handshaker = new Handshaker(authenticatorMap);
    assertFalse(handshaker.shaken());
  }

  @Test
  public void version1_0IsSupported() throws Exception {
    HandshakeAPI.HandshakeRequest handshakeRequest = HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(1).setMinor(0))
        .setAuthenticationMode(HandshakeAPI.AuthenticationMode.NONE).build();

    ByteArrayInputStream byteArrayInputStream =
        ProtobufTestUtilities.messageToByteArrayInputStream(handshakeRequest);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    Authenticator actualAuthenticator =
        handshaker.handshake(byteArrayInputStream, byteArrayOutputStream);
    assertTrue(actualAuthenticator instanceof NoopMock);

    assertTrue(handshaker.shaken());
  }

  @Test(expected = IncompatibleVersionException.class)
  public void version2NotSupported() throws Exception {
    HandshakeAPI.HandshakeRequest handshakeRequest = HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(2).setMinor(0))
        .setAuthenticationMode(HandshakeAPI.AuthenticationMode.NONE).build();

    ByteArrayInputStream byteArrayInputStream =
        ProtobufTestUtilities.messageToByteArrayInputStream(handshakeRequest);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    handshaker.handshake(byteArrayInputStream, byteArrayOutputStream);
  }

  @Test(expected = IncompatibleVersionException.class)
  public void bogusAuthenticationMode() throws Exception {
    HandshakeAPI.HandshakeRequest handshakeRequest = HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(1).setMinor(0))
        .setAuthenticationModeValue(-1).build();

    ByteArrayInputStream byteArrayInputStream =
        ProtobufTestUtilities.messageToByteArrayInputStream(handshakeRequest);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    handshaker.handshake(byteArrayInputStream, byteArrayOutputStream);
  }

  @Test
  public void simpleIsSupported() throws Exception {
    HandshakeAPI.HandshakeRequest handshakeRequest = HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(1).setMinor(0))
        .setAuthenticationMode(HandshakeAPI.AuthenticationMode.SIMPLE).build();

    ByteArrayInputStream byteArrayInputStream =
        ProtobufTestUtilities.messageToByteArrayInputStream(handshakeRequest);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    Authenticator actualAuthenticator =
        handshaker.handshake(byteArrayInputStream, byteArrayOutputStream);
    assertTrue(actualAuthenticator instanceof SimpleMock);

    assertTrue(handshaker.shaken());
  }

  @Test(expected = EOFException.class)
  public void eofAtHandshake() throws Exception {
    handshaker.handshake(new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream());
  }
}
