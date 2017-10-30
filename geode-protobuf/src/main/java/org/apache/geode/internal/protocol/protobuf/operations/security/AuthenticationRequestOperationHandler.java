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
package org.apache.geode.internal.protocol.protobuf.operations.security;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.protobuf.AuthenticationAPI;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.security.exception.IncompatibleAuthenticationMechanismsException;
import org.apache.geode.internal.protocol.security.processors.AuthorizationSecurityProcessor;
import org.apache.geode.internal.protocol.security.Authenticator;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.security.AuthenticationFailedException;

public class AuthenticationRequestOperationHandler implements
    OperationHandler<AuthenticationAPI.AuthenticationRequest, AuthenticationAPI.AuthenticationResponse, ClientProtocol.ErrorResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<AuthenticationAPI.AuthenticationResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, AuthenticationAPI.AuthenticationRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {

    if (messageExecutionContext.getAuthenticationToken() != null) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(buildAndLogError(ProtocolErrorCode.ALREADY_AUTHENTICATED,
              "The user has already been authenticated for this connection. Re-authentication is not supported at this time.",
              null))
          .build());
    }

    Authenticator authenticator = messageExecutionContext.getAuthenticator();
    Properties properties = new Properties();
    properties.putAll(request.getCredentialsMap());

    try {
      Object authenticationToken = authenticator.authenticate(properties);
      messageExecutionContext.setSecurityProcessor(new AuthorizationSecurityProcessor());
      messageExecutionContext.setAuthenticationToken(authenticationToken);
      return Success
          .of(AuthenticationAPI.AuthenticationResponse.newBuilder().setAuthenticated(true).build());
    } catch (IncompatibleAuthenticationMechanismsException e) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder().setError(
          buildAndLogError(ProtocolErrorCode.UNSUPPORTED_AUTHENTICATION_MODE, e.getMessage(), e))
          .build());
    } catch (AuthenticationFailedException e) {
      return Success.of(
          AuthenticationAPI.AuthenticationResponse.newBuilder().setAuthenticated(false).build());
    }
  }

  private BasicTypes.Error buildAndLogError(ProtocolErrorCode errorCode, String message,
      Exception ex) {
    if (ex == null) {
      logger.warn(message);
    } else {
      logger.warn(message, ex);
    }

    return BasicTypes.Error.newBuilder().setErrorCode(errorCode.codeValue).setMessage(message)
        .build();
  }
}
