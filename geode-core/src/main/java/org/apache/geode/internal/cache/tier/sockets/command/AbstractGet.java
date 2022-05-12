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

package org.apache.geode.internal.cache.tier.sockets.command;

import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_INVALID;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_NOT_FOUND;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_TOMBSTONE;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.cache.operations.internal.GetOperationContextImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public abstract class AbstractGet extends Get70 {

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, final long startTime) throws IOException {

    final CacheServerStats stats = serverConnection.getCacheServerStats();

    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    long stepStartTime = updateReadGetRequestTime(startTime, stats);

    try {
      processRequest(clientMessage, serverConnection, securityService, stats, startTime,
          stepStartTime);
    } catch (ResponseException e) {
      writeException(clientMessage, e.getCause(), false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    }
  }

  protected Object getObjectOrThrowResponseException(final @NotNull Part part)
      throws ResponseException {
    try {
      return part.getObject();
    } catch (Exception e) {
      throw new ResponseException(e);
    }
  }

  protected abstract void processRequest(@NotNull Message request,
      @NotNull ServerConnection serverConnection,
      @NotNull SecurityService securityService, @NotNull CacheServerStats stats, long startTime,
      long stepStartTime) throws IOException, ResponseException;

  protected void processGetRequest(final @NotNull Message request,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, final @NotNull CacheServerStats stats,
      final long startTime, long stepStartTime, final @Nullable String regionName,
      final @Nullable Object key, @Nullable Object callbackArg) throws IOException {

    final boolean debugEnabled = logger.isDebugEnabled();
    if (debugEnabled) {
      logger.debug(
          "{}: Received get request ({} bytes) from {} for region {} key {} txId {}",
          serverConnection.getName(), request.getPayloadLength(),
          serverConnection.getSocketString(), regionName, key, request.getTransactionId());
    }

    if (key == null || regionName == null) {
      writeErrorResponseForRegionOrKeyNull(request, serverConnection, regionName, key);
      return;
    }

    final Region<?, ?> region = serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      writeErrorResponseForRegionNotFound(request, serverConnection, regionName);
      return;
    }

    GetOperationContext getContext = null;
    try {
      // for integrated security
      securityService.authorize(ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ,
          regionName, key);

      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        getContext = authzRequest.getAuthorize(regionName, key, callbackArg);
        callbackArg = getContext.getCallbackArg();
      }
    } catch (NotAuthorizedException ex) {
      writeException(request, ex, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // Get the value and update the statistics. Do not deserialize
    // the value if it is a byte[].
    final Entry entry;
    try {
      entry = getEntry(region, key, callbackArg, serverConnection);
    } catch (Exception e) {
      writeException(request, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    @Retained
    final Object originalData = entry.value;
    Object data = originalData;
    try {
      boolean isObject = entry.isObject;
      final VersionTag<?> versionTag = entry.versionTag;
      final boolean keyNotPresent = entry.keyNotPresent;

      try {
        AuthorizeRequestPP postAuthzRequest = serverConnection.getPostAuthzRequest();
        if (postAuthzRequest != null) {
          try {
            getContext = postAuthzRequest.getAuthorize(regionName, key, data, isObject, getContext);
            GetOperationContextImpl gci = (GetOperationContextImpl) getContext;
            Object newData = gci.getRawValue();
            if (newData != data) {
              // user changed the value
              isObject = getContext.isObject();
              data = newData;
            }
          } finally {
            if (getContext != null) {
              ((GetOperationContextImpl) getContext).release();
            }
          }
        }
      } catch (NotAuthorizedException ex) {
        writeException(request, ex, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }

      // post process
      data = securityService.postProcess(regionName, key, data, entry.isObject);

      stepStartTime = updateProcessGetTime(stepStartTime, stats);

      writeResponse(request, serverConnection, region, data, isObject, versionTag,
          keyNotPresent);
    } finally {
      OffHeapHelper.release(originalData);
    }

    serverConnection.setAsTrue(RESPONDED);

    if (debugEnabled) {
      logger.debug("{}: Wrote get response back to {} for region {} {}", serverConnection.getName(),
          serverConnection.getSocketString(), regionName, entry);
    }

    stats.incWriteGetResponseTime(DistributionStats.getStatTime() - stepStartTime);

    final CachePerfStats regionPerfStats = ((InternalRegion) region).getRegionPerfStats();
    if (regionPerfStats != null) {
      regionPerfStats.endGetForClient(startTime, entry.keyNotPresent);
    }
  }

  private void writeErrorResponseForRegionNotFound(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection, final @NotNull String regionName)
      throws IOException {
    String reason = " was not found during get request";
    writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
  }

  private void writeErrorResponseForRegionOrKeyNull(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection, final @Nullable String regionName,
      final @Nullable Object key) throws IOException {
    final String errMessage;
    if ((key == null) && (regionName == null)) {
      errMessage =
          "The input region name and key for the get request are null.";
    } else if (key == null) {
      errMessage = "The input key for the get request is null.";
    } else {
      errMessage = "The input region name for the get request is null.";
    }
    logger.warn("{}: {}", serverConnection.getName(), errMessage);
    writeErrorResponse(clientMessage, MessageType.REQUESTDATAERROR, errMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
  }

  protected void writeResponse(final @NotNull Message request,
      final @NotNull ServerConnection serverConnection,
      final @NotNull Region<?, ?> region, final @Nullable Object data, final boolean isObject,
      final @Nullable VersionTag<?> versionTag, final boolean keyNotPresent) throws IOException {

    final Message response = serverConnection.getResponseMessage();
    response.setTransactionId(request.getTransactionId());

    // TODO jbarrett - PR metadata
    processResponse(response, keyNotPresent, data, isObject, versionTag);
    logger.info("XXX: Using the new GET_RESPONSE: {}", response.getMessageType());

    serverConnection.getCache().getCancelCriterion().checkCancelInProgress(null);
    response.send(serverConnection);
    request.clearParts();
  }

  private long updateProcessGetTime(long start, final CacheServerStats stats) {
    long oldStart = start;
    start = DistributionStats.getStatTime();
    stats.incProcessGetTime(start - oldStart);
    return start;
  }

  private long updateReadGetRequestTime(long start, final CacheServerStats stats) {
    long oldStart = start;
    start = DistributionStats.getStatTime();
    stats.incReadGetRequestTime(start - oldStart);
    return start;
  }

  protected void processResponse(final @NotNull Message response, final boolean keyNotPresent,
      final @Nullable Object data, final boolean isObject,
      final @Nullable VersionTag<?> versionTag) {

    if (keyNotPresent) {
      if (null == versionTag) {
        response.setMessageType(GET_RESPONSE_NOT_FOUND);
        response.setNumberOfParts(0);
      } else {
        response.setMessageType(GET_RESPONSE_TOMBSTONE);
        response.setNumberOfParts(1);
        response.addObjPart(versionTag);
      }
    } else if (data == null && isObject) {
      response.setMessageType(GET_RESPONSE_INVALID);
      response.setNumberOfParts(1);
      response.addObjPart(versionTag);
    } else {
      response.setMessageType(GET_RESPONSE);
      response.setNumberOfParts(2);
      response.addPartInAnyForm(data, isObject);
      // TODO jbarrett - when is version tag options, original had flag.
      response.addObjPart(versionTag);
    }
  }

  protected static class ResponseException extends Exception {
    public ResponseException(final Throwable cause) {
      super(cause);
    }
  }
}
