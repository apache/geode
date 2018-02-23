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
package org.apache.geode.internal.protocol.protobuf.v1;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.protobuf.v1.registry.ProtobufOperationContextRegistry;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionTerminatingStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.OperationNotAuthorizedException;

/**
 * This handles protobuf requests by determining the operation type of the request and dispatching
 * it to the appropriate handler.
 */
@Experimental
public class ProtobufOpsProcessor {
  private final ProtobufOperationContextRegistry protobufOperationContextRegistry;
  private final ProtobufSerializationService serializationService;
  private static final Logger logger = LogService.getLogger(ProtobufOpsProcessor.class);

  public ProtobufOpsProcessor(ProtobufSerializationService serializationService,
      ProtobufOperationContextRegistry protobufOperationContextRegistry) {
    this.serializationService = serializationService;
    this.protobufOperationContextRegistry = protobufOperationContextRegistry;
  }

  public ClientProtocol.Message process(ClientProtocol.Message request,
      MessageExecutionContext messageExecutionContext) {
    ClientProtocol.Message.MessageTypeCase requestType = request.getMessageTypeCase();
    logger.debug("Processing request of type {}", requestType);
    ProtobufOperationContext operationContext =
        protobufOperationContextRegistry.getOperationContext(requestType);
    Result result;

    try {
      messageExecutionContext.getConnectionStateProcessor()
          .validateOperation(messageExecutionContext, operationContext);
      result = processOperation(request, messageExecutionContext, requestType, operationContext);
    } catch (OperationNotAuthorizedException e) {
      // Don't move to a terminating state for authorization state failures
      logger.warn(e.getMessage());
      result = Failure.of(e);
    } catch (EncodingException | DecodingException e) {
      logger.warn(e.getMessage());
      result = Failure.of(e);
    } catch (ConnectionStateException e) {
      logger.warn(e.getMessage());
      messageExecutionContext
          .setConnectionStateProcessor(new ProtobufConnectionTerminatingStateProcessor());
      result = Failure.of(e);
    }

    return ((ClientProtocol.Message.Builder) result.map(operationContext.getToResponse(),
        operationContext.getToErrorResponse())).build();
  }

  private Result processOperation(ClientProtocol.Message request, MessageExecutionContext context,
      ClientProtocol.Message.MessageTypeCase requestType, ProtobufOperationContext operationContext)
      throws ConnectionStateException, EncodingException, DecodingException {

    long startTime = context.getStatistics().startOperation();
    try {
      return operationContext.getOperationHandler().process(serializationService,
          operationContext.getFromRequest().apply(request), context);
    } catch (InvalidExecutionContextException exception) {
      logger.error("Invalid execution context found for operation {}", requestType);
      logger.error(exception);
      return Failure.of(BasicTypes.ErrorCode.INVALID_REQUEST,
          "Invalid execution context found for operation.");
    } finally {
      context.getStatistics().endOperation(startTime);
    }
  }
}
