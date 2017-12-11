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
package org.apache.geode.internal.protocol.state;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.OperationContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.state.exception.ConnectionStateException;
import org.apache.geode.internal.protocol.state.exception.OperationNotAuthorizedException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;

public class ConnectionShiroAuthorizingStateProcessor implements ConnectionStateProcessor {
  private final SecurityService securityService;
  private final Subject subject;

  public ConnectionShiroAuthorizingStateProcessor(SecurityService securityService,
      Subject subject) {
    this.securityService = securityService;
    this.subject = subject;
  }

  @Override
  public void validateOperation(MessageExecutionContext messageContext,
      OperationContext operationContext) throws ConnectionStateException {
    ThreadState threadState = securityService.bindSubject(subject);
    try {
      securityService.authorize(operationContext.getAccessPermissionRequired());
    } catch (NotAuthorizedException e) {
      messageContext.getStatistics().incAuthorizationViolations();
      throw new OperationNotAuthorizedException(ProtocolErrorCode.AUTHORIZATION_FAILED,
          "The user is not authorized to complete this operation");
    } finally {
      threadState.restore();
    }
  }

  @Override
  public ConnectionAuthenticatingStateProcessor allowAuthentication()
      throws ConnectionStateException {
    throw new ConnectionStateException(ProtocolErrorCode.ALREADY_AUTHENTICATED,
        "The user has already been authenticated for this connection. Re-authentication is not supported at this time.");
  }
}
