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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.ResourceException;
import org.apache.geode.cache.operations.PutOperationContext;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * @since GemFire 6.1
 */
public class Put61 extends BaseCommand {

  private static final Put61 singleton = new Put61();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long p_start)
      throws IOException, InterruptedException {
    long start = p_start;
    Part regionNamePart = null, keyPart = null, valuePart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    Part eventPart = null;
    StringBuilder errMessage = new StringBuilder();
    boolean isDelta = false;
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    CacheServerStats stats = serverConnection.getCacheServerStats();

    // requiresResponse = true;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadPutRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = clientMessage.getPart(0);
    keyPart = clientMessage.getPart(1);
    try {
      isDelta = (Boolean) clientMessage.getPart(2).getObject();
    } catch (Exception e) {
      writeException(clientMessage, MessageType.PUT_DELTA_ERROR, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      // CachePerfStats not available here.
      return;
    }
    valuePart = clientMessage.getPart(3);
    eventPart = clientMessage.getPart(4);
    if (clientMessage.getNumberOfParts() > 5) {
      callbackArgPart = clientMessage.getPart(5);
      try {
        callbackArg = callbackArgPart.getObject();
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getString();

    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: Received 6.1{}put request ({} bytes) from {} for region {} key {}",
          serverConnection.getName(), (isDelta ? " delta " : " "), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), regionName, key);
    }

    // Process the put request
    if (key == null || regionName == null) {
      if (key == null) {
        String putMsg = " The input key for the 6.1 put request is null";
        if (isDebugEnabled) {
          logger.debug("{}:{}", serverConnection.getName(), putMsg);
        }
        errMessage.append(putMsg);
      }
      if (regionName == null) {
        String putMsg = " The input region name for the 6.1 put request is null";
        if (isDebugEnabled) {
          logger.debug("{}:{}", serverConnection.getName(), putMsg);
        }
        errMessage.append(putMsg);
      }
      writeErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR, errMessage.toString(),
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      String reason = " was not found during 6.1 put request";
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    if (valuePart.isNull() && region.containsKey(key)) {
      // Invalid to 'put' a null value in an existing key
      String putMsg = " Attempted to 6.1 put a null value for existing key " + key;
      if (isDebugEnabled) {
        logger.debug("{}:{}", serverConnection.getName(), putMsg);
      }
      errMessage.append(putMsg);
      writeErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR, errMessage.toString(),
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // try {
    // this.eventId = (EventID)eventPart.getObject();
    ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
    long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    EventID eventId =
        new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId);

    try {
      Object value = null;
      if (!isDelta) {
        value = valuePart.getSerializedForm();
      }
      boolean isObject = valuePart.isObject();
      boolean isMetaRegion = region.isUsedForMetaRegion();
      clientMessage.setMetaRegion(isMetaRegion);

      securityService.authorize(Resource.DATA, Operation.WRITE, regionName, key.toString());

      AuthorizeRequest authzRequest = null;
      if (!isMetaRegion) {
        authzRequest = serverConnection.getAuthzRequest();
      }
      if (authzRequest != null) {
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          authzRequest.createRegionAuthorize((String) key);
        }
        // Allow PUT operations on meta regions (bug #38961)
        else {
          PutOperationContext putContext =
              authzRequest.putAuthorize(regionName, key, value, isObject, callbackArg);
          value = putContext.getValue();
          isObject = putContext.isObject();
          callbackArg = putContext.getCallbackArg();
        }
      }
      // If the value is 1 byte and the byte represents null,
      // attempt to create the entry. This test needs to be
      // moved to DataSerializer or DataSerializer.NULL needs
      // to be publicly accessible.
      boolean result = false;
      if (value == null && !isDelta) {
        // Create the null entry. Since the value is null, the value of the
        // isObject
        // the true after null doesn't matter and is not used.
        result = region.basicBridgeCreate(key, null, true, callbackArg,
            serverConnection.getProxyID(), true, new EventIDHolder(eventId), false);
      } else {
        // Put the entry
        byte[] delta = null;
        if (isDelta) {
          delta = valuePart.getSerializedForm();
        }
        result = region.basicBridgePut(key, value, delta, isObject, callbackArg,
            serverConnection.getProxyID(), true, new EventIDHolder(eventId));
      }
      if (result) {
        serverConnection.setModificationInfo(true, regionName, key);
      } else {
        String message = serverConnection.getName() + ": Failed to 6.1 put entry for region "
            + regionName + " key " + key + " value " + valuePart;
        if (isDebugEnabled) {
          logger.debug(message);
        }
        throw new Exception(message);
      }
    } catch (RegionDestroyedException rde) {
      writeException(clientMessage, rde, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (ResourceException re) {
      writeException(clientMessage, re, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (InvalidDeltaException ide) {
      logger.info("Error applying delta for key {} of region {}: {}",
          key, regionName, ide.getMessage());
      writeException(clientMessage, MessageType.PUT_DELTA_ERROR, ide, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      region.getCachePerfStats().incDeltaFullValuesRequested();
      return;

    } catch (Exception ce) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, ce);

      // If an exception occurs during the put, preserve the connection
      writeException(clientMessage, ce, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      if (ce instanceof GemFireSecurityException) {
        // Fine logging for security exceptions since these are already
        // logged by the security logger
        if (isDebugEnabled) {
          logger.debug("{}: Unexpected Security exception", serverConnection.getName(), ce);
        }
      } else if (isDebugEnabled) {
        logger.debug("{}: Unexpected Exception", serverConnection.getName(), ce);
      }
      return;
    } finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessPutTime(start - oldStart);
    }

    // Increment statistics and write the reply
    if (region instanceof PartitionedRegion) {
      PartitionedRegion pr = (PartitionedRegion) region;
      if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
        writeReplyWithRefreshMetadata(clientMessage, serverConnection, pr, pr.getNetworkHopType());
        pr.clearNetworkHopData();
      } else {
        writeReply(clientMessage, serverConnection);
      }
    } else {
      writeReply(clientMessage, serverConnection);
    }
    serverConnection.setAsTrue(RESPONDED);
    if (isDebugEnabled) {
      logger.debug("{}: Sent 6.1 put response back to {} for region {} key {} value {}",
          serverConnection.getName(), serverConnection.getSocketString(), regionName, key,
          valuePart);
    }
    stats.incWritePutResponseTime(DistributionStats.getStatTime() - start);
  }

}
