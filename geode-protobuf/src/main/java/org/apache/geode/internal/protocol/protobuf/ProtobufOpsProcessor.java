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
package org.apache.geode.internal.protocol.protobuf;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.OperationContext;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.protobuf.registry.ProtobufOperationContextRegistry;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.security.SecurityProcessor;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.NotAuthorizedException;

import static org.apache.geode.internal.protocol.ProtocolErrorCode.*;

/**
 * This handles protobuf requests by determining the operation type of the request and dispatching
 * it to the appropriate handler.
 */
@Experimental
public class ProtobufOpsProcessor {

  private final ProtobufOperationContextRegistry protobufOperationContextRegistry;
  private final SerializationService serializationService;
  private static final Logger logger = LogService.getLogger(ProtobufOpsProcessor.class);

  public ProtobufOpsProcessor(SerializationService serializationService,
      ProtobufOperationContextRegistry protobufOperationContextRegistry) {
    this.serializationService = serializationService;
    this.protobufOperationContextRegistry = protobufOperationContextRegistry;
  }

  public ClientProtocol.Response process(ClientProtocol.Request request,
      MessageExecutionContext messageExecutionContext) {
    ClientProtocol.Request.RequestAPICase requestType = request.getRequestAPICase();
    logger.debug("Processing request of type {}", requestType);
    OperationContext operationContext =
        protobufOperationContextRegistry.getOperationContext(requestType);
    Result result;

    SecurityProcessor securityProcessor = messageExecutionContext.getSecurityProcessor();
    try {
      securityProcessor.validateOperation(request, messageExecutionContext, operationContext);
      result = processOperation(request, messageExecutionContext, requestType, operationContext);
    } catch (AuthenticationRequiredException e) {
      logger.warn(e);
      result = Failure
          .of(ProtobufResponseUtilities.makeErrorResponse(AUTHENTICATION_FAILED, e.getMessage()));
    } catch (NotAuthorizedException e) {
      logger.warn(e);
      messageExecutionContext.getStatistics().incAuthorizationViolations();
      result = Failure.of(ProtobufResponseUtilities.makeErrorResponse(AUTHORIZATION_FAILED,
          "The user is not authorized to complete this operation"));
    }

    return ((ClientProtocol.Response.Builder) result.map(operationContext.getToResponse(),
        operationContext.getToErrorResponse())).build();
  }

  private Result processOperation(ClientProtocol.Request request, MessageExecutionContext context,
      ClientProtocol.Request.RequestAPICase requestType, OperationContext operationContext) {
    try {
      return operationContext.getOperationHandler().process(serializationService,
          operationContext.getFromRequest().apply(request), context);
    } catch (InvalidExecutionContextException exception) {
      logger.error("Invalid execution context found for operation {}", requestType);
      return Failure.of(ProtobufResponseUtilities.makeErrorResponse(UNSUPPORTED_OPERATION,
          "Invalid execution context found for operation."));
    }
  }
}
