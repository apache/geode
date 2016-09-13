/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.operations.DestroyOperationContext;
import com.gemstone.gemfire.cache.operations.RegionDestroyOperationContext;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.EventIDHolder;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.OpType;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.util.Breadcrumbs;
import com.gemstone.gemfire.security.GemFireSecurityException;

public class Destroy65 extends BaseCommand {

  private final static Destroy65 singleton = new Destroy65();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  protected void writeReplyWithRefreshMetadata(Message origMsg,
                                               ServerConnection servConn,
                                               PartitionedRegion pr,
                                               byte nwHop) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected void writeReplyWithRefreshMetadata(Message origMsg,
                                               ServerConnection servConn,
                                               PartitionedRegion pr,
                                               boolean entryNotFoundForRemove,
                                               byte nwHop,
                                               VersionTag tag) throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(2);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(new byte[] { pr.getMetadataVersion(), nwHop });
    pr.getPrStats().incPRMetaDataSentCount();
    replyMsg.addIntPart(entryNotFoundForRemove ? 1 : 0);
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADAT tx: {}", servConn.getName(), origMsg.getTransactionId());
    }
  }

  protected void writeReply(Message origMsg, ServerConnection servConn, boolean entryNotFound, VersionTag tag)
    throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(2);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(OK_BYTES);
    replyMsg.addIntPart(entryNotFound ? 1 : 0);
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {} parts={}", servConn.getName(), origMsg.getTransactionId(), replyMsg.getNumberOfParts());
    }
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start) throws IOException, InterruptedException {
    Part regionNamePart;
    Part keyPart;
    Part callbackArgPart;
    Part eventPart;
    Part expectedOldValuePart;

    Object operation = null;
    Object expectedOldValue = null;

    String regionName = null;
    Object callbackArg = null, key = null;
    StringBuffer errMessage = new StringBuffer();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    servConn.setAsTrue(REQUIRES_RESPONSE);

    long now = DistributionStats.getStatTime();
    stats.incReadDestroyRequestTime(now - start);

    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    keyPart = msg.getPart(1);
    expectedOldValuePart = msg.getPart(2);
    try {

      operation = msg.getPart(3).getObject();

      if (((operation instanceof Operation) && ((Operation) operation == Operation.REMOVE)) || ((operation instanceof Byte) && (Byte) operation == OpType.DESTROY))

      {
        expectedOldValue = expectedOldValuePart.getObject();
      }
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    eventPart = msg.getPart(4);

    if (msg.getNumberOfParts() > 5) {
      callbackArgPart = msg.getPart(5);
      try {
        callbackArg = callbackArgPart.getObject();
      } catch (Exception e) {
        writeException(msg, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getString();
    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received destroy65 request ({} bytes; op={}) from {} for region {} key {}{} txId {}", servConn.getName(), msg
        .getPayloadLength(), operation, servConn.getSocketString(), regionName, key, (operation == Operation.REMOVE ? " value=" + expectedOldValue : ""), msg
        .getTransactionId());
    }
    boolean entryNotFoundForRemove = false;

    // Process the destroy request
    if (key == null || regionName == null) {
      if (key == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.Destroy_0_THE_INPUT_KEY_FOR_THE_DESTROY_REQUEST_IS_NULL, servConn
          .getName()));
        errMessage.append(LocalizedStrings.Destroy__THE_INPUT_KEY_FOR_THE_DESTROY_REQUEST_IS_NULL.toLocalizedString());
      }
      if (regionName == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.Destroy_0_THE_INPUT_REGION_NAME_FOR_THE_DESTROY_REQUEST_IS_NULL, servConn
          .getName()));
        errMessage.append(LocalizedStrings.Destroy__THE_INPUT_REGION_NAME_FOR_THE_DESTROY_REQUEST_IS_NULL.toLocalizedString());
      }
      writeErrorResponse(msg, MessageType.DESTROY_DATA_ERROR, errMessage.toString(), servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) servConn.getCache().getRegion(regionName);
    if (region == null) {
      String reason = LocalizedStrings.Destroy__0_WAS_NOT_FOUND_DURING_DESTROY_REQUEST.toLocalizedString(regionName);
      writeRegionDestroyedEx(msg, regionName, reason, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    // Destroy the entry
    ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
    long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    EventID eventId = new EventID(servConn.getEventMemberIDByteArray(), threadId, sequenceId);
    EventIDHolder clientEvent = new EventIDHolder(eventId);

    Breadcrumbs.setEventId(eventId);

    // msg.isRetry might be set by v7.0 and later clients
    if (msg.isRetry()) {
      //          if (logger.isDebugEnabled()) {
      //            logger.debug("DEBUG: encountered isRetry in Destroy65");
      //          }
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
      this.securityService.authorizeRegionWrite(regionName, key.toString());

      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          RegionDestroyOperationContext destroyContext = authzRequest.destroyRegionAuthorize((String) key, callbackArg);
          callbackArg = destroyContext.getCallbackArg();
        } else {
          DestroyOperationContext destroyContext = authzRequest.destroyAuthorize(regionName, key, callbackArg);
          callbackArg = destroyContext.getCallbackArg();
        }
      }
      if (operation == null || operation == Operation.DESTROY) {
        region.basicBridgeDestroy(key, callbackArg, servConn.getProxyID(), true, clientEvent);
      } else {
        // this throws exceptions if expectedOldValue checks fail
        try {
          if (expectedOldValue == null) {
            expectedOldValue = Token.INVALID;
          }
          if (operation == Operation.REMOVE && msg.isRetry() && clientEvent.getVersionTag() != null) {
            // the operation was successful last time it was tried, so there's
            // no need to perform it again.  Just return the version tag and
            // success status
            if (logger.isDebugEnabled()) {
              logger.debug("remove(k,v) operation was successful last time with version {}", clientEvent.getVersionTag());
            }
            // try the operation anyway to ensure that it's been distributed to all servers
            try {
              region.basicBridgeRemove(key, expectedOldValue, callbackArg, servConn.getProxyID(), true, clientEvent);
            } catch (EntryNotFoundException e) {
              // ignore, and don't set entryNotFoundForRemove because this was a successful
              // operation - bug #51664
            }
          } else {
            region.basicBridgeRemove(key, expectedOldValue, callbackArg, servConn.getProxyID(), true, clientEvent);
            if (logger.isDebugEnabled()) {
              logger.debug("region.remove succeeded");
            }
          }
        } catch (EntryNotFoundException e) {
          servConn.setModificationInfo(true, regionName, key);
          if (logger.isDebugEnabled()) {
            logger.debug("writing entryNotFound response");
          }
          entryNotFoundForRemove = true;
        }
      }
      servConn.setModificationInfo(true, regionName, key);
    } catch (EntryNotFoundException e) {
      // Don't send an exception back to the client if this
      // exception happens. Just log it and continue.
      logger.info(LocalizedMessage.create(LocalizedStrings.Destroy_0_DURING_ENTRY_DESTROY_NO_ENTRY_WAS_FOUND_FOR_KEY_1, new Object[] {
        servConn.getName(), key
      }));
      entryNotFoundForRemove = true;
    } catch (RegionDestroyedException rde) {
      writeException(msg, rde, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(servConn, e);

      // If an exception occurs during the destroy, preserve the connection
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      if (e instanceof GemFireSecurityException) {
        // Fine logging for security exceptions since these are already
        // logged by the security logger
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Unexpected Security exception", servConn.getName(), e);
        }
      } else {
        logger.warn(LocalizedMessage.create(LocalizedStrings.Destroy_0_UNEXPECTED_EXCEPTION, servConn.getName()), e);
      }
      return;
    }

    // Update the statistics and write the reply
    now = DistributionStats.getStatTime();
    stats.incProcessDestroyTime(now - start);

    if (region instanceof PartitionedRegion) {
      PartitionedRegion pr = (PartitionedRegion) region;
      if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
        writeReplyWithRefreshMetadata(msg, servConn, pr, entryNotFoundForRemove, pr.getNetworkHopType(), clientEvent.getVersionTag());
        pr.clearNetworkHopData();
      } else {
        writeReply(msg, servConn, entryNotFoundForRemove | clientEvent.getIsRedestroyedEntry(), clientEvent.getVersionTag());
      }
    } else {
      writeReply(msg, servConn, entryNotFoundForRemove | clientEvent.getIsRedestroyedEntry(), clientEvent.getVersionTag());
    }
    servConn.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent destroy response for region {} key {}", servConn.getName(), regionName, key);
    }
    stats.incWriteDestroyResponseTime(DistributionStats.getStatTime() - start);


  }
}
