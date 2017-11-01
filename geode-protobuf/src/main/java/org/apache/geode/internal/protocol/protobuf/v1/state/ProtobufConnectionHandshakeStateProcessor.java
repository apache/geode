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

import org.apache.geode.internal.protocol.state.ConnectionHandshakingStateProcessor;
import org.apache.geode.internal.protocol.state.ConnectionStateProcessor;
import org.apache.geode.internal.protocol.state.LegacySecurityConnectionStateProcessor;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.state.NoSecurityConnectionStateProcessor;
import org.apache.geode.internal.protocol.OperationContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.state.exception.ConnectionStateException;
import org.apache.geode.internal.protocol.protobuf.v1.operations.HandshakeRequestOperationHandler;
import org.apache.geode.internal.security.SecurityService;

public class ProtobufConnectionHandshakeStateProcessor
    implements ConnectionHandshakingStateProcessor {
  private final SecurityService securityService;

  public ProtobufConnectionHandshakeStateProcessor(SecurityService securityService) {
    this.securityService = securityService;
  }

  @Override
  public void validateOperation(MessageExecutionContext messageContext,
      OperationContext operationContext) throws ConnectionStateException {
    if (!(operationContext.getOperationHandler() instanceof HandshakeRequestOperationHandler)) {
      throw new ConnectionStateException(ProtocolErrorCode.HANDSHAKE_REQUIRED,
          "Protobuf handshake must be completed before any other operation.");
    }
  }

  @Override
  public ConnectionStateProcessor handshakeSucceeded() {
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
}
