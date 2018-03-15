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

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufOperationContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.OperationNotAuthorizedException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;

public class ProtobufConnectionAuthorizingStateProcessor
    implements ProtobufConnectionStateProcessor {
  private final SecurityService securityService;
  private final Subject subject;

  public ProtobufConnectionAuthorizingStateProcessor(SecurityService securityService,
      Subject subject) {
    this.securityService = securityService;
    this.subject = subject;
  }

  @Override
  public void validateOperation(Object message, ProtobufSerializationService serializer,
      MessageExecutionContext messageContext, ProtobufOperationContext operationContext)
      throws ConnectionStateException, DecodingException {
    ThreadState threadState = securityService.bindSubject(subject);
    try {
      securityService.authorize(operationContext.getAccessPermissionRequired(
          operationContext.getFromRequest().apply(message), serializer));
    } catch (NotAuthorizedException e) {
      messageContext.getStatistics().incAuthorizationViolations();
      throw new OperationNotAuthorizedException(
          "The user is not authorized to complete this operation: "
              + ((ClientProtocol.Message) message).getMessageTypeCase());
    } finally {
      threadState.restore();
    }
  }

  @Override
  public ProtobufConnectionAuthenticatingStateProcessor allowAuthentication()
      throws ConnectionStateException {
    throw new ConnectionStateException(BasicTypes.ErrorCode.ALREADY_AUTHENTICATED,
        "The user has already been authenticated for this connection. Re-authentication is not supported at this time.");
  }

  public ThreadState prepareThreadForAuthorization() {
    return securityService.bindSubject(subject);
  }

  public void restoreThreadState(ThreadState state) {
    state.restore();
  }
}
