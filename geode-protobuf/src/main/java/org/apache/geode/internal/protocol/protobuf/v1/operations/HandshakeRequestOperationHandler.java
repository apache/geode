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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.state.ConnectionHandshakingStateProcessor;
import org.apache.geode.internal.protocol.state.ConnectionStateProcessor;
import org.apache.geode.internal.protocol.state.ConnectionTerminatingStateProcessor;
import org.apache.geode.internal.protocol.state.exception.ConnectionStateException;

public class HandshakeRequestOperationHandler implements
    OperationHandler<ConnectionAPI.HandshakeRequest, ConnectionAPI.HandshakeResponse, ClientProtocol.ErrorResponse> {
  private static final Logger logger = LogManager.getLogger();
  private final VersionValidator validator = new VersionValidator();

  @Override
  public Result<ConnectionAPI.HandshakeResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, ConnectionAPI.HandshakeRequest request,
      MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, ConnectionStateException {
    ConnectionHandshakingStateProcessor stateProcessor;

    // If handshake not allowed by this state this will throw a ConnectionStateException
    stateProcessor = messageExecutionContext.getConnectionStateProcessor().allowHandshake();

    final boolean handshakeSucceeded =
        validator.isValid(request.getMajorVersion(), request.getMinorVersion());
    if (handshakeSucceeded) {
      ConnectionStateProcessor nextStateProcessor = stateProcessor.handshakeSucceeded();
      messageExecutionContext.setConnectionStateProcessor(nextStateProcessor);
    } else {
      messageExecutionContext
          .setConnectionStateProcessor(new ConnectionTerminatingStateProcessor());
    }

    return Success.of(ConnectionAPI.HandshakeResponse.newBuilder()
        .setServerMajorVersion(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
        .setServerMinorVersion(ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE)
        .setHandshakePassed(handshakeSucceeded).build());
  }
}
