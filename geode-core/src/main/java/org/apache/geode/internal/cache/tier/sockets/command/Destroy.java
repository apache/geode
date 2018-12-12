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

import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.operations.DestroyOperationContext;
import org.apache.geode.cache.operations.RegionDestroyOperationContext;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
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

public class Destroy extends BaseCommand {

  private static final Destroy singleton = new Destroy();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long startparam)
      throws IOException, InterruptedException {
    long start = startparam;

    Part regionNamePart = null, keyPart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    Part eventPart = null;
    StringBuffer errMessage = new StringBuffer();
    CacheServerStats stats = serverConnection.getCacheServerStats();
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadDestroyRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = clientMessage.getPart(0);
    keyPart = clientMessage.getPart(1);
    eventPart = clientMessage.getPart(2);
    // callbackArgPart = null; (redundant assignment)
    if (clientMessage.getNumberOfParts() > 3) {
      callbackArgPart = clientMessage.getPart(3);
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
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received destroy request ({} bytes) from {} for region {} key {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), regionName, key);
    }

    // Process the destroy request
    if (key == null || regionName == null) {
      if (key == null) {
        logger.warn("{}: The input key for the destroy request is null",
            serverConnection.getName());
        errMessage.append("The input key for the destroy request is null");
      }
      if (regionName == null) {
        logger.warn("{}: The input region name for the destroy request is null",
            serverConnection.getName());
        errMessage
            .append("The input region name for the destroy request is null");
      }
      writeErrorResponse(clientMessage, MessageType.DESTROY_DATA_ERROR, errMessage.toString(),
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      String reason = String.format("%s was not found during destroy request",
          regionName);
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // Destroy the entry
    ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
    long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    EventID eventId =
        new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId);

    try {
      // for integrated security
      securityService.authorize(Resource.DATA, Operation.WRITE, regionName, key.toString());

      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          RegionDestroyOperationContext destroyContext =
              authzRequest.destroyRegionAuthorize((String) key, callbackArg);
          callbackArg = destroyContext.getCallbackArg();
        } else {
          DestroyOperationContext destroyContext =
              authzRequest.destroyAuthorize(regionName, key, callbackArg);
          callbackArg = destroyContext.getCallbackArg();
        }
      }
      region.basicBridgeDestroy(key, callbackArg, serverConnection.getProxyID(), true,
          new EventIDHolder(eventId));
      serverConnection.setModificationInfo(true, regionName, key);
    } catch (EntryNotFoundException e) {
      // Don't send an exception back to the client if this
      // exception happens. Just log it and continue.
      if (logger.isDebugEnabled()) {
        logger.debug("{}: during entry destroy no entry was found for key {}",
            serverConnection.getName(), key);
      }
    } catch (RegionDestroyedException rde) {
      writeException(clientMessage, rde, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, e);

      // If an exception occurs during the destroy, preserve the connection
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      if (e instanceof GemFireSecurityException) {
        // Fine logging for security exceptions since these are already
        // logged by the security logger
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Unexpected Security exception", serverConnection.getName(), e);
        }
      } else {
        logger.warn(String.format("%s: Unexpected Exception",
            serverConnection.getName()), e);
      }
      return;
    }

    // Update the statistics and write the reply
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessDestroyTime(start - oldStart);
    }
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
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent destroy response for region {} key {}", serverConnection.getName(),
          regionName, key);
    }
    stats.incWriteDestroyResponseTime(DistributionStats.getStatTime() - start);
  }
}
