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

import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.state.ConnectionHandshakingStateProcessor;
import org.apache.geode.internal.protocol.state.ConnectionStateProcessor;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.state.exception.ConnectionStateException;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HandshakeRequestOperationHandler implements
    OperationHandler<ConnectionAPI.HandshakeRequest, ConnectionAPI.HandshakeResponse, ClientProtocol.ErrorResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<ConnectionAPI.HandshakeResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, ConnectionAPI.HandshakeRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {
    ConnectionHandshakingStateProcessor stateProcessor;

    try {
      stateProcessor = messageExecutionContext.getConnectionStateProcessor().allowHandshake();
    } catch (ConnectionStateException e) {
      return Failure.of(ProtobufResponseUtilities.makeErrorResponse(e));
    }

    boolean handshakeSucceeded = false;
    // Require an exact match with our version of the protobuf code for this implementation
    if (request.getMajorVersion() == ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE
        && request.getMinorVersion() == ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE) {
      handshakeSucceeded = true;
      ConnectionStateProcessor nextStateProcessor = stateProcessor.handshakeSucceeded();
      messageExecutionContext.setConnectionStateProcessor(nextStateProcessor);
    }

    return Success.of(ConnectionAPI.HandshakeResponse.newBuilder()
        .setServerMajorVersion(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
        .setServerMinorVersion(ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE)
        .setHandshakePassed(handshakeSucceeded).build());
  }
}
