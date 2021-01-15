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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.internal.DestroyOp;
import org.apache.geode.cache.operations.DestroyOperationContext;
import org.apache.geode.cache.operations.RegionDestroyOperationContext;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.OpType;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.Token;
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

public class Destroy70 extends BaseCommand {
  @Immutable
  private static final Destroy70 singleton = new Destroy70();

  public static Command getCommand() {
    return singleton;
  }

  private Destroy70() {}

  protected void writeReplyWithRefreshMetadata(Message origMsg, ServerConnection servConn,
      PartitionedRegion pr, boolean entryNotFoundForRemove, byte nwHop, VersionTag versionTag)
      throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int numParts = 3;
    if (versionTag != null) {
      flags |= DestroyOp.HAS_VERSION_TAG;
      numParts++;
    }
    flags |= DestroyOp.HAS_ENTRY_NOT_FOUND_PART;
    // if (logger.isDebugEnabled()) {
    // logger.fine("writing response with metadata and " + numParts + " parts");
    // }
    replyMsg.setNumberOfParts(numParts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addIntPart(flags);
    if (versionTag != null) {
      replyMsg.addObjPart(versionTag);
    }
    replyMsg.addBytesPart(new byte[] {pr.getMetadataVersion(), nwHop});
    replyMsg.addIntPart(entryNotFoundForRemove ? 1 : 0);
    pr.getPrStats().incPRMetaDataSentCount();
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADATA tx: {}", servConn.getName(),
          origMsg.getTransactionId());
    }
  }

  protected void writeReply(Message origMsg, ServerConnection servConn, boolean entryNotFound,
      VersionTag versionTag) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("Destroy70.writeReply(entryNotFound={}, tag={})", entryNotFound, versionTag);
    }
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int numParts = 3;
    if (versionTag != null) {
      flags |= DestroyOp.HAS_VERSION_TAG;
      numParts++;
    }
    flags |= DestroyOp.HAS_ENTRY_NOT_FOUND_PART;
    // if (logger.isDebugEnabled()) {
    // logger.fine("writing response with 1-byte metadata and " + numParts + " parts");
    // }
    replyMsg.setNumberOfParts(numParts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addIntPart(flags);
    if (versionTag != null) {
      // if (logger.isDebugEnabled()) {
      // logger.fine("wrote version tag in response: " + versionTag);
      // }
      replyMsg.addObjPart(versionTag);
      // } else {
      // if (logger.isDebugEnabled()) {
      // logger.fine("response has no version tag");
      // }
    }
    replyMsg.addBytesPart(okBytes()); // make old single-hop code happy by puting byte[]{0} here
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
    StringBuilder errMessage = new StringBuilder();
    CacheServerStats stats = serverConnection.getCacheServerStats();
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    long now = DistributionStats.getStatTime();
    stats.incReadDestroyRequestTime(now - start);

    // Retrieve the data from the message parts
    final Part regionNamePart = clientMessage.getPart(0);
    final Part keyPart = clientMessage.getPart(1);
    final Part expectedOldValuePart = clientMessage.getPart(2);

    final Operation operation;
    try {
      final Part operationPart = clientMessage.getPart(3);

      if (operationPart.isBytes()) {
        final byte[] bytes = operationPart.getSerializedForm();
        if (null == bytes || 0 == bytes.length) {
          // older clients can send empty bytes for default operation.
          operation = Operation.DESTROY;
        } else {
          operation = Operation.fromOrdinal(bytes[0]);
        }
      } else {
        // Fallback for older clients.
        final Object operationObject = operationPart.getObject();
        if (operationObject == null) {
          // native clients may send a null since the op is java-serialized.
          operation = Operation.DESTROY;
        } else if (operationObject instanceof Byte
            && (Byte) operationObject == OpType.DESTROY) {
          // older native clients may send Byte object OpType.DESTROY value treated as
          // Operation.REMOVE.
          operation = Operation.REMOVE;
        } else {
          operation = (Operation) operationObject;
        }
      }
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    Object expectedOldValue = null;
    if (operation == Operation.REMOVE) {
      try {
        expectedOldValue = expectedOldValuePart.getObject();
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }

    final Part eventPart = clientMessage.getPart(4);

    Object callbackArg = null;
    if (clientMessage.getNumberOfParts() > 5) {
      final Part callbackArgPart = clientMessage.getPart(5);
      try {
        callbackArg = callbackArgPart.getObject();
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }

    final Object key;
    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final String regionName = regionNamePart.getCachedString();

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

    final LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      String reason = " was not found during destroy request";
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // Destroy the entry
    final ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
    final long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    final long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    final EventID eventId =
        new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId);
    final EventIDHolder clientEvent = new EventIDHolder(eventId);

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
      securityService.authorize(ResourcePermission.Resource.DATA,
          ResourcePermission.Operation.WRITE, regionName,
          key);

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
