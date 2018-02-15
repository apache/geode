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

import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;

public abstract class AbstractFunctionRequestOperationHandler<Req, Resp>
    implements ProtobufOperationHandler<Req, Resp> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<Resp, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService, Req request,
      MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException {

    final String functionID = getFunctionID(request);

    final Function<?> function = FunctionService.getFunction(functionID);
    if (function == null) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder().setErrorCode(BasicTypes.ErrorCode.INVALID_REQUEST)
              .setMessage(LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
                  .toLocalizedString(functionID))
              .build())
          .build());
    }

    final SecurityService securityService = messageExecutionContext.getCache().getSecurityService();
    final String regionName = getRegionName(request);

    try {
      // check security for function.
      function.getRequiredPermissions(regionName).forEach(securityService::authorize);
    } catch (NotAuthorizedException ex) {
      final String message = "Authorization failed for function \"" + functionID + "\"";
      logger.warn(message, ex);
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder().setError(BasicTypes.Error
          .newBuilder().setMessage(message).setErrorCode(BasicTypes.ErrorCode.AUTHORIZATION_FAILED))
          .build());
    }

    Object executionTarget = getExecutionTarget(request, regionName, messageExecutionContext);
    if (executionTarget instanceof Failure) {
      return (Failure) executionTarget;
    }

    try {
      Execution execution = getFunctionExecutionObject(executionTarget);

      Object arguments = getFunctionArguments(request, serializationService);

      if (arguments != null) {
        execution = execution.setArguments(arguments);
      }

      Set<?> parseFilter = parseFilter(serializationService, request);
      if (parseFilter != null) {
        execution = execution.withFilter(parseFilter);
      }

      final ResultCollector<Object, List<Object>> resultCollector = execution.execute(functionID);

      if (function.hasResult()) {
        List<Object> results = resultCollector.getResult();

        return buildResultMessage(serializationService, results);
      } else {
        // This is fire and forget.
        return buildResultMessage(serializationService);
      }
    } catch (FunctionException ex) {
      final String message = "Function execution failed: " + ex.toString();
      logger.info(message, ex);
      return Failure
          .of(ClientProtocol.ErrorResponse.newBuilder().setError(BasicTypes.Error.newBuilder()
              .setErrorCode(BasicTypes.ErrorCode.SERVER_ERROR).setMessage(message)).build());
    } catch (EncodingException ex) {
      final String message = "Encoding failed: " + ex.toString();
      logger.info(message, ex);
      return Failure
          .of(ClientProtocol.ErrorResponse.newBuilder().setError(BasicTypes.Error.newBuilder()
              .setErrorCode(BasicTypes.ErrorCode.SERVER_ERROR).setMessage(message)).build());
    }
  }

  protected abstract Set<?> parseFilter(ProtobufSerializationService serializationService,
      Req request) throws EncodingException, DecodingException;

  protected abstract String getFunctionID(Req request);

  /** the result of this may be null, which is used by the security service to mean "no region" */
  protected abstract String getRegionName(Req request);

  /** region, list of members, etc */
  protected abstract Object getExecutionTarget(Req request, String regionName,
      MessageExecutionContext executionContext) throws InvalidExecutionContextException;

  /** arguments for the function */
  protected abstract Object getFunctionArguments(Req request,
      ProtobufSerializationService serializationService)
      throws EncodingException, DecodingException;

  protected abstract Execution getFunctionExecutionObject(Object executionTarget)
      throws InvalidExecutionContextException;

  protected abstract Result buildResultMessage(ProtobufSerializationService serializationService)
      throws EncodingException;

  protected abstract Result buildResultMessage(ProtobufSerializationService serializationService,
      List<Object> results) throws EncodingException;
}
