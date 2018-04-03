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

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionAuthenticatingStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionTerminatingStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.security.AuthenticationFailedException;

public class AuthenticationRequestOperationHandler implements
    ProtobufOperationHandler<ConnectionAPI.AuthenticationRequest, ConnectionAPI.AuthenticationResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<ConnectionAPI.AuthenticationResponse> process(
      ProtobufSerializationService serializationService,
      ConnectionAPI.AuthenticationRequest request, MessageExecutionContext messageExecutionContext)
      throws ConnectionStateException {
    ProtobufConnectionAuthenticatingStateProcessor stateProcessor;

    // If authentication not allowed by this state this will throw a ConnectionStateException
    stateProcessor = messageExecutionContext.getConnectionStateProcessor().allowAuthentication();

    Properties properties = new Properties();
    properties.putAll(request.getCredentialsMap());

    try {
      ProtobufConnectionStateProcessor nextState =
          stateProcessor.authenticate(messageExecutionContext, properties);
      messageExecutionContext.setConnectionStateProcessor(nextState);
      return Success
          .of(ConnectionAPI.AuthenticationResponse.newBuilder().setAuthenticated(true).build());
    } catch (AuthenticationFailedException e) {
      messageExecutionContext.getStatistics().incAuthenticationFailures();
      logger.debug("Authentication failed", e);
      messageExecutionContext
          .setConnectionStateProcessor(new ProtobufConnectionTerminatingStateProcessor());
      return Success
          .of(ConnectionAPI.AuthenticationResponse.newBuilder().setAuthenticated(false).build());
    }
  }
}
