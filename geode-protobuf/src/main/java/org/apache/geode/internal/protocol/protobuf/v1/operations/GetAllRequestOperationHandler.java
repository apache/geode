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

import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.INVALID_REQUEST;
import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.SERVER_ERROR;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionAuthorizingStateProcessor;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class GetAllRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.GetAllRequest, RegionAPI.GetAllResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<RegionAPI.GetAllResponse> process(ProtobufSerializationService serializationService,
      RegionAPI.GetAllRequest request, MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException {
    String regionName = request.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);
    if (region == null) {
      logger.error("Received GetAll request for non-existing region {}", regionName);
      return Failure.of(BasicTypes.ErrorCode.SERVER_ERROR, "Region not found");
    }

    ThreadState threadState = null;
    SecurityService securityService = messageExecutionContext.getCache().getSecurityService();
    boolean perKeyAuthorization = false;
    if (messageExecutionContext
        .getConnectionStateProcessor() instanceof ProtobufConnectionAuthorizingStateProcessor) {
      threadState = ((ProtobufConnectionAuthorizingStateProcessor) messageExecutionContext
          .getConnectionStateProcessor()).prepareThreadForAuthorization();
      // Check if authorized for entire region
      try {
        securityService.authorize(new ResourcePermission(ResourcePermission.Resource.DATA,
            ResourcePermission.Operation.READ, regionName));
        ((ProtobufConnectionAuthorizingStateProcessor) messageExecutionContext
            .getConnectionStateProcessor()).restoreThreadState(threadState);
        threadState = null;
      } catch (NotAuthorizedException ex) {
        // Not authorized for the region, have to check keys individually
        perKeyAuthorization = true;
      }
    }
    final boolean authorizeKeys = perKeyAuthorization; // Required for use in lambda

    long startTime = messageExecutionContext.getStatistics().startOperation();
    RegionAPI.GetAllResponse.Builder responseBuilder = RegionAPI.GetAllResponse.newBuilder();
    try {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(true);
      request.getKeyList().stream().forEach((key) -> processSingleKey(responseBuilder,
          serializationService, region, key, securityService, authorizeKeys));
    } finally {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(false);
      messageExecutionContext.getStatistics().endOperation(startTime);
      if (threadState != null) {
        ((ProtobufConnectionAuthorizingStateProcessor) messageExecutionContext
            .getConnectionStateProcessor()).restoreThreadState(threadState);
      }
    }

    return Success.of(responseBuilder.build());
  }

  private void processSingleKey(RegionAPI.GetAllResponse.Builder responseBuilder,
      ProtobufSerializationService serializationService, Region region, BasicTypes.EncodedValue key,
      SecurityService securityService, boolean authorizeKeys) {
    try {

      Object decodedKey = serializationService.decode(key);
      if (authorizeKeys) {
        securityService.authorize(new ResourcePermission(ResourcePermission.Resource.DATA,
            ResourcePermission.Operation.READ, region.getName(), decodedKey.toString()));
      }
      Object value = region.get(decodedKey);
      BasicTypes.Entry entry =
          ProtobufUtilities.createEntry(serializationService, decodedKey, value);
      responseBuilder.addEntries(entry);

    } catch (NotAuthorizedException ex) {
      responseBuilder.addFailures(
          buildKeyedError(key, "Unauthorized access", BasicTypes.ErrorCode.AUTHORIZATION_FAILED));
    } catch (DecodingException ex) {
      logger.info("Key encoding not supported: {}", ex);
      responseBuilder
          .addFailures(buildKeyedError(key, "Key encoding not supported.", INVALID_REQUEST));
    } catch (EncodingException ex) {
      logger.info("Value encoding not supported: {}", ex);
      responseBuilder
          .addFailures(buildKeyedError(key, "Value encoding not supported.", INVALID_REQUEST));
    } catch (Exception ex) {
      logger.warn("Failure in protobuf getAll operation for key: " + key, ex);
      responseBuilder.addFailures(buildKeyedError(key, ex.toString(), SERVER_ERROR));
    }
  }

  private BasicTypes.KeyedError buildKeyedError(BasicTypes.EncodedValue key, String errorMessage,
      BasicTypes.ErrorCode errorCode) {
    return BasicTypes.KeyedError.newBuilder().setKey(key)
        .setError(BasicTypes.Error.newBuilder().setErrorCode(errorCode).setMessage(errorMessage))
        .build();
  }

}
