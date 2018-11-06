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

import org.apache.geode.cache.operations.RegionDestroyOperationContext;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class DestroyRegion extends BaseCommand {

  private static final DestroyRegion singleton = new DestroyRegion();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Part regionNamePart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null;
    Part eventPart = null;
    StringBuilder errMessage = new StringBuilder();
    CacheServerStats stats = serverConnection.getCacheServerStats();
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadDestroyRegionRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = clientMessage.getPart(0);
    eventPart = clientMessage.getPart(1);
    // callbackArgPart = null; (redundant assignment)
    if (clientMessage.getNumberOfParts() > 2) {
      callbackArgPart = clientMessage.getPart(2);
      try {
        callbackArg = callbackArgPart.getObject();
      } catch (DistributedSystemDisconnectedException se) {
        // FIXME this can't happen
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{} ignoring message of type {} from client {} because shutdown occurred during message processing.",
              serverConnection.getName(), MessageType.getString(clientMessage.getMessageType()),
              serverConnection.getProxyID());
        }

        serverConnection.setFlagProcessMessagesAsFalse();
        serverConnection.setClientDisconnectedException(se);
        return;
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getString();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received destroy region request ({} bytes) from {} for region {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), regionName);
    }

    // Process the destroy region request
    if (regionName == null) {
      logger.warn("{}: The input region name for the destroy region request is null",
          serverConnection.getName());
      errMessage.append(
          "The input region name for the destroy region request is null.");

      writeErrorResponse(clientMessage, MessageType.DESTROY_REGION_DATA_ERROR,
          errMessage.toString(), serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      String reason =
          "Region was not found during destroy region request";
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // Destroy the region
    ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
    long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    EventID eventId =
        new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId);

    try {
      // user needs to have data:manage on all regions in order to destory a particular region
      securityService.authorize(Resource.DATA, Operation.MANAGE);

      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        RegionDestroyOperationContext destroyContext =
            authzRequest.destroyRegionAuthorize(regionName, callbackArg);
        callbackArg = destroyContext.getCallbackArg();
      }
      // region.destroyRegion(callbackArg);
      region.basicBridgeDestroyRegion(callbackArg, serverConnection.getProxyID(),
          true /* boolean from cache Client */, eventId);
    } catch (DistributedSystemDisconnectedException e) {
      // FIXME better exception hierarchy would avoid this check
      if (serverConnection.getCachedRegionHelper().getCache().getCancelCriterion()
          .cancelInProgress() != null) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{} ignoring message of type {} from client {} because shutdown occurred during message processing.",
              serverConnection.getName(), MessageType.getString(clientMessage.getMessageType()),
              serverConnection.getProxyID());
        }
        serverConnection.setFlagProcessMessagesAsFalse();
        serverConnection.setClientDisconnectedException(e);
      } else {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
      }
      return;
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, e);

      // Otherwise, write an exception message and continue
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // Update the statistics and write the reply
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessDestroyRegionTime(start - oldStart);
    }
    writeReply(clientMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent destroy region response for region {}", serverConnection.getName(),
          regionName);
    }
    stats.incWriteDestroyRegionResponseTime(DistributionStats.getStatTime() - start);
  }

}
