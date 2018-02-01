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
package org.apache.geode.internal.protocol.protobuf.v1.state;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;

import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.OperationContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.protobuf.v1.operations.ProtocolVersionHandler;
import org.apache.geode.internal.protocol.state.ConnectionStateProcessor;
import org.apache.geode.internal.protocol.state.LegacySecurityConnectionStateProcessor;
import org.apache.geode.internal.protocol.state.NoSecurityConnectionStateProcessor;
import org.apache.geode.internal.protocol.state.exception.ConnectionStateException;
import org.apache.geode.internal.security.SecurityService;

public class ProtobufConnectionHandshakeStateProcessor implements ConnectionStateProcessor {
  private final SecurityService securityService;

  public ProtobufConnectionHandshakeStateProcessor(SecurityService securityService) {
    this.securityService = securityService;
  }

  @Override
  public void validateOperation(MessageExecutionContext messageContext,
      OperationContext operationContext) throws ConnectionStateException {
    throw new ConnectionStateException(ProtocolErrorCode.INVALID_REQUEST,
        "Connection processing should never be asked to validate an operation");
  }

  private ConnectionStateProcessor nextConnectionState() {
    if (securityService.isIntegratedSecurity()) {
      return new ConnectionShiroAuthenticatingStateProcessor(securityService);
    } else if (securityService.isPeerSecurityRequired()
        || securityService.isClientSecurityRequired()) {
      return new LegacySecurityConnectionStateProcessor();
    } else {
      // Noop authenticator...no security
      return new NoSecurityConnectionStateProcessor();
    }
  }

  @Override
  public boolean handleMessageIndependently(InputStream inputStream, OutputStream outputStream,
      MessageExecutionContext executionContext) throws IOException {
    // inputStream will have had the first byte stripped off to determine communication mode, add
    // that byte back before processing message
    PushbackInputStream messageStream = new PushbackInputStream(inputStream);
    messageStream.unread(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());

    if (ProtocolVersionHandler.handleVersionMessage(messageStream, outputStream,
        executionContext.getStatistics())) {
      executionContext.setConnectionStateProcessor(nextConnectionState());
    }
    return true;
  }
}
