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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolHandshaker;
import org.apache.geode.internal.cache.tier.sockets.ServiceLoadingFailureException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.server.Authenticator;

public class Handshaker implements ClientProtocolHandshaker {
  private static final int MAJOR_VERSION = 1;
  private static final int MINOR_VERSION = 0;
  private static final Logger logger = LogService.getLogger();

  private final Map<String, Class<? extends Authenticator>> authenticators;
  private boolean shaken = false;

  public Handshaker(Map<String, Class<? extends Authenticator>> availableAuthenticators) {
    this.authenticators = availableAuthenticators;
  }

  @Override
  public Authenticator handshake(InputStream inputStream, OutputStream outputStream)
      throws IOException, IncompatibleVersionException {
    HandshakeAPI.HandshakeRequest handshakeRequest =
        HandshakeAPI.HandshakeRequest.parseDelimitedFrom(inputStream);

    if (handshakeRequest == null) {
      throw new EOFException("No handshake received from client");
    }

    HandshakeAPI.Semver version = handshakeRequest.getVersion();
    if (version.getMajor() != MAJOR_VERSION) {
      writeFailureTo(outputStream, ProtocolErrorCode.UNSUPPORTED_VERSION.codeValue,
          "Version mismatch: incompatible major version");
      throw new IncompatibleVersionException(
          "Client major version does not match server major version");
    }
    if (version.getMinor() > MINOR_VERSION) {
      writeFailureTo(outputStream, ProtocolErrorCode.UNSUPPORTED_VERSION.codeValue,
          "Version mismatch: client newer than server");
      throw new IncompatibleVersionException(
          "Client minor version is greater than server minor version");
    }

    try {
      Class<? extends Authenticator> authenticatorClass =
          selectAuthenticator(authenticators, handshakeRequest.getAuthenticationMode());

      if (authenticatorClass == null) {
        writeFailureTo(outputStream, ProtocolErrorCode.UNSUPPORTED_AUTHENTICATION_MODE.codeValue,
            "Invalid authentication mode");
        throw new IncompatibleVersionException("Invalid authentication mode");
      }

      HandshakeAPI.HandshakeResponse.newBuilder().setOk(true).build()
          .writeDelimitedTo(outputStream);
      shaken = true;
      return authenticatorClass.newInstance();

    } catch (IllegalAccessException | InstantiationException e) {
      logger.error("Could not instantiate authenticator from handshake: ", e);
      throw new ServiceLoadingFailureException(e);
    }
  }

  private Class<? extends Authenticator> selectAuthenticator(
      Map<String, Class<? extends Authenticator>> authenticators,
      HandshakeAPI.AuthenticationMode authenticationMode)
      throws IllegalAccessException, InstantiationException {
    switch (authenticationMode) {
      case SIMPLE:
        return authenticators.get("SIMPLE");
      case NONE:
        return authenticators.get("NOOP");
      case UNRECOGNIZED:
        // fallthrough!
      default:
        return null;
    }
  }

  private void writeFailureTo(OutputStream outputStream, int errorCode, String errorMessage)
      throws IOException {
    HandshakeAPI.HandshakeResponse.newBuilder().setOk(false)
        .setError(BasicTypes.Error.newBuilder().setErrorCode(errorCode).setMessage(errorMessage))
        .build().writeDelimitedTo(outputStream);
  }

  @Override
  public boolean shaken() {
    return shaken;
  }
}
