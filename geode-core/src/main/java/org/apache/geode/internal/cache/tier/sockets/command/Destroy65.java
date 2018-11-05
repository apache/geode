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
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.operations.DestroyOperationContext;
import org.apache.geode.cache.operations.RegionDestroyOperationContext;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.OpType;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Resource;

public class Destroy65 extends BaseCommand {

  private static final Destroy65 singleton = new Destroy65();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  protected void writeReplyWithRefreshMetadata(Message origMsg, ServerConnection serverConnection,
      PartitionedRegion pr, byte nwHop) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected void writeReplyWithRefreshMetadata(Message origMsg, ServerConnection servConn,
      PartitionedRegion pr, boolean entryNotFoundForRemove, byte nwHop, VersionTag tag)
      throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(2);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(new byte[] {pr.getMetadataVersion(), nwHop});
    pr.getPrStats().incPRMetaDataSentCount();
    replyMsg.addIntPart(entryNotFoundForRemove ? 1 : 0);
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADATA tx: {}", servConn.getName(),
          origMsg.getTransactionId());
    }
  }

  protected void writeReply(Message origMsg, ServerConnection servConn, boolean entryNotFound,
      VersionTag tag) throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(2);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(okBytes());
    replyMsg.addIntPart(entryNotFound ? 1 : 0);
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {} parts={}", servConn.getName(), origMsg.getTransactionId(),
          replyMsg.getNumberOfParts());
    }
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Part regionNamePart;
    Part keyPart;
    Part callbackArgPart;
    Part eventPart;
    Part expectedOldValuePart;

    Object operation = null;
    Object expectedOldValue = null;

    String regionName = null;
    Object callbackArg = null, key = null;
    StringBuilder errMessage = new StringBuilder();
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    CacheServerStats stats = serverConnection.getCacheServerStats();
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    long now = DistributionStats.getStatTime();
    stats.incReadDestroyRequestTime(now - start);

    // Retrieve the data from the message parts
    regionNamePart = clientMessage.getPart(0);
    keyPart = clientMessage.getPart(1);
    expectedOldValuePart = clientMessage.getPart(2);
    try {

      operation = clientMessage.getPart(3).getObject();

      if (((operation instanceof Operation) && ((Operation) operation == Operation.REMOVE))
          || ((operation instanceof Byte) && (Byte) operation == OpType.DESTROY))

      {
        expectedOldValue = expectedOldValuePart.getObject();
      }
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

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
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Received destroy65 request ({} bytes; op={}) from {} for region {} key {}{} txId {}",
          serverConnection.getName(), clientMessage.getPayloadLength(), operation,
          serverConnection.getSocketString(), regionName, key,
          (operation == Operation.REMOVE ? " value=" + expectedOldValue : ""),
          clientMessage.getTransactionId());
    }
    boolean entryNotFoundForRemove = false;

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
    EventIDHolder clientEvent = new EventIDHolder(eventId);

    Breadcrumbs.setEventId(eventId);

    // msg.isRetry might be set by v7.0 and later clients
    if (clientMessage.isRetry()) {
      // if (logger.isDebugEnabled()) {
      // logger.debug("DEBUG: encountered isRetry in Destroy65");
      // }
      clientEvent.setPossibleDuplicate(true);
      if (region.getAttributes().getConcurrencyChecksEnabled()) {
        // recover the version tag from other servers
        clientEvent.setRegion(region);
        if (!recoverVersionTagForRetriedOperation(clientEvent)) {
          clientEvent.setPossibleDuplicate(false); // no-one has seen this event
        }
      }
    }

    try {
      // for integrated security
      securityService.authorize(Resource.DATA, ResourcePermission.Operation.WRITE, regionName,
          key.toString());

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
      if (operation == null || operation == Operation.DESTROY) {
        region.basicBridgeDestroy(key, callbackArg, serverConnection.getProxyID(), true,
            clientEvent);
      } else {
        // this throws exceptions if expectedOldValue checks fail
        try {
          if (expectedOldValue == null) {
            expectedOldValue = Token.INVALID;
          }
          if (operation == Operation.REMOVE && clientMessage.isRetry()
              && clientEvent.getVersionTag() != null) {
            // the operation was successful last time it was tried, so there's
            // no need to perform it again. Just return the version tag and
            // success status
            if (logger.isDebugEnabled()) {
              logger.debug("remove(k,v) operation was successful last time with version {}",
                  clientEvent.getVersionTag());
            }
            // try the operation anyway to ensure that it's been distributed to all servers
            try {
              region.basicBridgeRemove(key, expectedOldValue, callbackArg,
                  serverConnection.getProxyID(), true, clientEvent);
            } catch (EntryNotFoundException e) {
              // ignore, and don't set entryNotFoundForRemove because this was a successful
              // operation - bug #51664
            }
          } else {
            region.basicBridgeRemove(key, expectedOldValue, callbackArg,
                serverConnection.getProxyID(), true, clientEvent);
            if (logger.isDebugEnabled()) {
              logger.debug("region.remove succeeded");
            }
          }
        } catch (EntryNotFoundException e) {
          serverConnection.setModificationInfo(true, regionName, key);
          if (logger.isDebugEnabled()) {
            logger.debug("writing entryNotFound response");
          }
          entryNotFoundForRemove = true;
        }
      }
      serverConnection.setModificationInfo(true, regionName, key);
    } catch (EntryNotFoundException e) {
      // Don't send an exception back to the client if this
      // exception happens. Just log it and continue.
      if (logger.isDebugEnabled()) {
        logger.debug("{}: during entry destroy no entry was found for key {}",
            serverConnection.getName(), key);
      }
      entryNotFoundForRemove = true;
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
    now = DistributionStats.getStatTime();
    stats.incProcessDestroyTime(now - start);

    if (region instanceof PartitionedRegion) {
      PartitionedRegion pr = (PartitionedRegion) region;
      if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
        writeReplyWithRefreshMetadata(clientMessage, serverConnection, pr, entryNotFoundForRemove,
            pr.getNetworkHopType(), clientEvent.getVersionTag());
        pr.clearNetworkHopData();
      } else {
        writeReply(clientMessage, serverConnection,
            entryNotFoundForRemove | clientEvent.getIsRedestroyedEntry(),
            clientEvent.getVersionTag());
      }
    } else {
      writeReply(clientMessage, serverConnection,
          entryNotFoundForRemove | clientEvent.getIsRedestroyedEntry(),
          clientEvent.getVersionTag());
    }
    serverConnection.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent destroy response for region {} key {}", serverConnection.getName(),
          regionName, key);
    }
    stats.incWriteDestroyResponseTime(DistributionStats.getStatTime() - start);


  }
}
