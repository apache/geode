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
package org.apache.geode.protocol.protobuf;

import org.apache.geode.internal.cache.tier.sockets.StreamAuthenticator;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.SecurityManager;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class ProtobufSimpleAuthenticator implements StreamAuthenticator {
  private boolean authenticated;

  @Override
  public void receiveMessage(InputStream inputStream, OutputStream outputStream,
      SecurityManager securityManager) throws IOException {
    AuthenticationAPI.SimpleAuthenticationRequest authenticationRequest =
        AuthenticationAPI.SimpleAuthenticationRequest.parseDelimitedFrom(inputStream);
    if (authenticationRequest == null) {
      throw new EOFException();
    }

    Properties properties = new Properties();
    properties.setProperty("username", authenticationRequest.getUsername());
    properties.setProperty("password", authenticationRequest.getPassword());

    try {
      Object principal = securityManager.authenticate(properties);
      authenticated = principal != null;
    } catch (AuthenticationFailedException e) {
      authenticated = false;
    }

    AuthenticationAPI.SimpleAuthenticationResponse.newBuilder().setAuthenticated(authenticated)
        .build().writeDelimitedTo(outputStream);
  }

  @Override
  public boolean isAuthenticated() {
    return authenticated;
  }

  @Override
  public String implementationID() {
    return "SIMPLE";
  }
}
