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
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.state.AcceptMessages;
import org.apache.geode.internal.protocol.protobuf.v1.state.RequireAuthentication;
import org.apache.geode.internal.protocol.protobuf.v1.state.TerminateConnection;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.protocol.serialization.ValueSerializer;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ServiceResult;

public class HandshakeRequestOperationHandler implements
    ProtobufOperationHandler<ConnectionAPI.HandshakeRequest, ConnectionAPI.HandshakeResponse> {
  private static final Logger logger = LogService.getLogger();
  private final ModuleService moduleService;

  public HandshakeRequestOperationHandler(ModuleService moduleService) {
    this.moduleService = moduleService;
  }

  @Override
  public Result<ConnectionAPI.HandshakeResponse> process(
      ProtobufSerializationService serializationService, ConnectionAPI.HandshakeRequest request,
      MessageExecutionContext messageExecutionContext) throws ConnectionStateException {

    boolean authenticated = false;

    if (request.getCredentialsCount() > 0
        || messageExecutionContext.getConnectionState() instanceof RequireAuthentication) {
      Properties properties = new Properties();
      properties.putAll(request.getCredentialsMap());

      try {
        messageExecutionContext.authenticate(properties);
        messageExecutionContext.setState(new AcceptMessages());
        authenticated = true;
      } catch (AuthenticationFailedException e) {
        messageExecutionContext.getStatistics().incAuthenticationFailures();
        logger.debug("Authentication failed", e);
        messageExecutionContext.setState(new TerminateConnection());
      }
    }

    String valueFormat = request.getValueFormat();
    if (valueFormat != null && !valueFormat.isEmpty()) {
      ValueSerializer newSerializer = loadSerializer(valueFormat);
      messageExecutionContext.setValueSerializer(newSerializer);
    }

    return Success
        .of(ConnectionAPI.HandshakeResponse.newBuilder().setAuthenticated(authenticated).build());
  }

  private ValueSerializer loadSerializer(String valueFormat)
      throws ConnectionStateException {
    ServiceResult<Set<ValueSerializer>> valueSerializerServiceResult =
        moduleService.loadService(ValueSerializer.class);
    if (valueSerializerServiceResult.isSuccessful()) {
      for (ValueSerializer serializer : valueSerializerServiceResult.getMessage()) {
        if (serializer.getID().equals(valueFormat)) {
          return serializer;
        }
      }
    }

    throw new ConnectionStateException(BasicTypes.ErrorCode.INVALID_REQUEST,
        "Unable to find a ValueSerializer for format " + valueFormat);
  }
}
