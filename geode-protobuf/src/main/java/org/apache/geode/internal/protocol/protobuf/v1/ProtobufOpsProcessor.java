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
import org.apache.geode.internal.protocol.protobuf.v1.registry.ProtobufOperationContextRegistry;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.TerminateConnection;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.NotAuthorizedException;

/**
 * This handles protobuf requests by determining the operation type of the request and dispatching
 * it to the appropriate handler.
 */
@Experimental
public class ProtobufOpsProcessor {
  private final ProtobufOperationContextRegistry protobufOperationContextRegistry;
  private static final Logger logger = LogService.getLogger();

  public ProtobufOpsProcessor(ProtobufOperationContextRegistry protobufOperationContextRegistry) {
    this.protobufOperationContextRegistry = protobufOperationContextRegistry;
  }

  @SuppressWarnings("unchecked")
  public ClientProtocol.Message process(ClientProtocol.Message request,
      MessageExecutionContext messageExecutionContext) {
    ClientProtocol.Message.MessageTypeCase requestType = request.getMessageTypeCase();
    logger.debug("Processing request of type {}", requestType);
    @SuppressWarnings("rawtypes")
    ProtobufOperationContext operationContext =
        protobufOperationContextRegistry.getOperationContext(requestType);

    @SuppressWarnings("rawtypes")
    Result result;

    try {
      messageExecutionContext.getConnectionState().validateOperation(operationContext);
      result = processOperation(request, messageExecutionContext, requestType, operationContext);
    } catch (VirtualMachineError error) {
      initiateFailure(error);
      throw error;
    } catch (Throwable t) {
      logger.warn("Failure for request " + request, t);
      checkFailure();
      result = Failure.of(t);

      if (t instanceof ConnectionStateException) {
        messageExecutionContext.setState(new TerminateConnection());
      }
    }

    return ((ClientProtocol.Message.Builder) result.map(operationContext.getToResponse(),
        operationContext.getToErrorResponse())).build();
  }

  @SuppressWarnings("deprecation")
  private static void checkFailure() {
    org.apache.geode.SystemFailure.checkFailure();
  }

  @SuppressWarnings("deprecation")
  private static void initiateFailure(VirtualMachineError error) {
    org.apache.geode.SystemFailure.initiateFailure(error);
  }

  @SuppressWarnings("unchecked")
  private Result<?> processOperation(ClientProtocol.Message request,
      MessageExecutionContext context,
      ClientProtocol.Message.MessageTypeCase requestType,
      @SuppressWarnings("rawtypes") ProtobufOperationContext operationContext)
      throws ConnectionStateException, EncodingException, DecodingException {

    long startTime = context.getStatistics().startOperation();
    try {
      return operationContext.getOperationHandler().process(context.getSerializationService(),
          operationContext.getFromRequest().apply(request), context);
    } catch (InvalidExecutionContextException exception) {
      logger.error("Invalid execution context found for operation {}", requestType, exception);
      return Failure.of(BasicTypes.ErrorCode.INVALID_REQUEST,
          "Invalid execution context found for operation.");
    } catch (UnsupportedOperationException exception) {
      logger.error("Unsupported operation exception for request {}", requestType, exception);
      return Failure.of(BasicTypes.ErrorCode.UNSUPPORTED_OPERATION,
          "Unsupported operation:" + exception.getMessage());
    } catch (NotAuthorizedException e) {
      return Failure.of(BasicTypes.ErrorCode.AUTHORIZATION_FAILED, "Not authorized: " + e);
    } finally {
      context.getStatistics().endOperation(startTime);
    }
  }
}
