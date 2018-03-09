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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.EncodedValue;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.OQLQueryRequest;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.OQLQueryResponse;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.OQLQueryResponse.Builder;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionAuthorizingStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.OperationNotAuthorizedException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public class OqlQueryRequestOperationHandler
    implements ProtobufOperationHandler<OQLQueryRequest, OQLQueryResponse> {
  Logger logger = LogService.getLogger();

  @Override
  public Result<OQLQueryResponse> process(final ProtobufSerializationService serializationService,
      final OQLQueryRequest request, final MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, ConnectionStateException, EncodingException,
      DecodingException {
    String queryString = request.getQuery();
    List<EncodedValue> encodedParameters = request.getBindParameterList();

    InternalQueryService queryService = messageExecutionContext.getCache().getQueryService();

    Query query = queryService.newQuery(queryString);
    Object[] bindParameters = decodeBindParameters(serializationService, encodedParameters);

    if (messageExecutionContext
        .getConnectionStateProcessor() instanceof ProtobufConnectionAuthorizingStateProcessor) {
      final SecurityService securityService =
          messageExecutionContext.getCache().getSecurityService();
      ThreadState threadState =
          ((ProtobufConnectionAuthorizingStateProcessor) messageExecutionContext
              .getConnectionStateProcessor()).prepareThreadForAuthorization();
      try {
        for (String regionName : ((DefaultQuery) query).getRegionsInQuery(bindParameters)) {
          securityService.authorize(ResourcePermission.Resource.DATA,
              ResourcePermission.Operation.READ, regionName);
        }
      } catch (NotAuthorizedException ex) {
        messageExecutionContext.getStatistics().incAuthorizationViolations();
        throw new OperationNotAuthorizedException(
            "The user is not authorized to complete this operation");
      } finally {
        ((ProtobufConnectionAuthorizingStateProcessor) messageExecutionContext
            .getConnectionStateProcessor()).restoreThreadState(threadState);
      }
    }

    try {
      Object results = query.execute(bindParameters);
      return Success.of(encodeResults(serializationService, results));
    } catch (QueryException e) {
      logger.info("Query failed: " + query, e);
      return Failure.of(e);
    }

  }

  private Object[] decodeBindParameters(final ProtobufSerializationService serializationService,
      final List<EncodedValue> encodedParameters) {
    Object[] bindParameters = new Object[encodedParameters.size()];
    for (int i = 0; i < encodedParameters.size(); i++) {
      bindParameters[i] = serializationService.decode(encodedParameters.get(i));
    }
    return bindParameters;
  }

  private OQLQueryResponse encodeResults(final ProtobufSerializationService serializationService,
      final Object value) throws EncodingException {
    final Builder builder = OQLQueryResponse.newBuilder();

    // The result is a single value
    if (!(value instanceof SelectResults)) {
      builder.setSingleResult(serializationService.encode(value));
      return builder.build();
    }

    SelectResults<?> selectResults = (SelectResults<?>) value;

    // The result is a list of objects
    if (!selectResults.getCollectionType().getElementType().isStructType()) {
      BasicTypes.EncodedValueList.Builder listResult = BasicTypes.EncodedValueList.newBuilder();
      selectResults.stream().map(serializationService::encode).forEach(listResult::addElement);
      builder.setListResult(listResult);
      return builder.build();
    }

    // The result is a list of structs
    SelectResults<Struct> structResults = (SelectResults<Struct>) selectResults;

    StructType elementType = (StructType) structResults.getCollectionType().getElementType();
    BasicTypes.Table.Builder tableResult = BasicTypes.Table.newBuilder();
    tableResult.addAllFieldName(Arrays.asList(elementType.getFieldNames()));

    for (Struct row : structResults) {
      tableResult.addRow(encodeStruct(serializationService, row));
    }
    builder.setTableResult(tableResult);

    return builder.build();
  }

  private BasicTypes.EncodedValueList.Builder encodeStruct(
      final ProtobufSerializationService serializationService, final Struct row)
      throws EncodingException {
    BasicTypes.EncodedValueList.Builder structBuilder = BasicTypes.EncodedValueList.newBuilder();
    for (Object element : row.getFieldValues()) {
      structBuilder.addElement(serializationService.encode(element));
    }
    return structBuilder;
  }

}
