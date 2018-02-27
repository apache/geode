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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.SerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionAuthorizingStateProcessor;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class PutAllRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.PutAllRequest, RegionAPI.PutAllResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.PutAllResponse> process(ProtobufSerializationService serializationService,
      RegionAPI.PutAllRequest putAllRequest, MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException {
    String regionName = putAllRequest.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);

    if (region == null) {
      logger.error("Received PutAll request for non-existing region {}", regionName);
      return Failure.of(BasicTypes.ErrorCode.SERVER_ERROR,
          "Region passed does not exist: " + regionName);
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
            ResourcePermission.Operation.WRITE, regionName));
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
    RegionAPI.PutAllResponse.Builder builder = RegionAPI.PutAllResponse.newBuilder();
    try {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(true);

      putAllRequest.getEntryList().stream().forEach((entry) -> processSinglePut(builder,
          serializationService, region, entry, securityService, authorizeKeys));

    } finally {
      messageExecutionContext.getStatistics().endOperation(startTime);
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(false);
      if (threadState != null) {
        ((ProtobufConnectionAuthorizingStateProcessor) messageExecutionContext
            .getConnectionStateProcessor()).restoreThreadState(threadState);
      }
    }
    return Success.of(builder.build());
  }

  private void processSinglePut(RegionAPI.PutAllResponse.Builder builder,
      SerializationService serializationService, Region region, BasicTypes.Entry entry,
      SecurityService securityService, boolean authorizeKeys) {
    try {

      Object decodedKey = serializationService.decode(entry.getKey());

      if (authorizeKeys) {
        securityService.authorize(new ResourcePermission(ResourcePermission.Resource.DATA,
            ResourcePermission.Operation.WRITE, region.getName(), decodedKey.toString()));
      }

      Object decodedValue = serializationService.decode(entry.getValue());
      region.put(decodedKey, decodedValue);

    } catch (NotAuthorizedException ex) {
      builder.addFailedKeys(
          buildKeyedError(entry, BasicTypes.ErrorCode.AUTHORIZATION_FAILED, "Unauthorized access"));
    } catch (DecodingException ex) {
      logger.info("Encoding not supported: " + ex);
      builder.addFailedKeys(this.buildKeyedError(entry, INVALID_REQUEST, "Encoding not supported"));
    } catch (ClassCastException ex) {
      builder.addFailedKeys(buildKeyedError(entry, SERVER_ERROR, ex.toString()));
    } catch (Exception ex) {
      logger.warn("Error processing putAll entry", ex);
      builder.addFailedKeys(buildKeyedError(entry, SERVER_ERROR, ex.toString()));
    }
  }

  private BasicTypes.KeyedError buildKeyedError(BasicTypes.Entry entry,
      BasicTypes.ErrorCode errorCode, String message) {
    return BasicTypes.KeyedError.newBuilder().setKey(entry.getKey())
        .setError(BasicTypes.Error.newBuilder().setErrorCode(errorCode).setMessage(message))
        .build();
  }
}
