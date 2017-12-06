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
package org.apache.geode.internal.protocol.protobuf.v1.operations.security;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.state.ConnectionAuthenticatingStateProcessor;
import org.apache.geode.internal.protocol.state.exception.ConnectionStateException;
import org.apache.geode.security.AuthenticationFailedException;

public class AuthenticationRequestOperationHandler implements
    OperationHandler<ConnectionAPI.AuthenticationRequest, ConnectionAPI.AuthenticationResponse, ClientProtocol.ErrorResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<ConnectionAPI.AuthenticationResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, ConnectionAPI.AuthenticationRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {
    ConnectionAuthenticatingStateProcessor stateProcessor;

    try {
      stateProcessor = messageExecutionContext.getConnectionStateProcessor().allowAuthentication();
    } catch (ConnectionStateException e) {
      return Failure.of(ProtobufResponseUtilities.makeErrorResponse(e));
    }

    Properties properties = new Properties();
    properties.putAll(request.getCredentialsMap());

    try {
      messageExecutionContext.setConnectionStateProcessor(stateProcessor.authenticate(properties));
      return Success
          .of(ConnectionAPI.AuthenticationResponse.newBuilder().setAuthenticated(true).build());
    } catch (AuthenticationFailedException e) {
      return Success
          .of(ConnectionAPI.AuthenticationResponse.newBuilder().setAuthenticated(false).build());
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
