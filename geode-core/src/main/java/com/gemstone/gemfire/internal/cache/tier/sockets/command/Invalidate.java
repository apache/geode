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

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.operations.InvalidateOperationContext;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.EventIDHolder;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
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

public class Invalidate extends BaseCommand {

  private final static Invalidate singleton = new Invalidate();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start) throws IOException, InterruptedException {
    Part regionNamePart = null, keyPart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    Part eventPart = null;
    StringBuffer errMessage = new StringBuffer();
    CacheServerStats stats = servConn.getCacheServerStats();
    servConn.setAsTrue(REQUIRES_RESPONSE);

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadInvalidateRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    keyPart = msg.getPart(1);
    eventPart = msg.getPart(2);
    //    callbackArgPart = null; (redundant assignment)
    if (msg.getNumberOfParts() > 3) {
      callbackArgPart = msg.getPart(3);
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
      logger.debug(servConn.getName() + ": Received invalidate request (" + msg.getPayloadLength() + " bytes) from " + servConn
        .getSocketString() + " for region " + regionName + " key " + key);
    }

    // Process the invalidate request
    if (key == null || regionName == null) {
      if (key == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.BaseCommand__THE_INPUT_KEY_FOR_THE_0_REQUEST_IS_NULL, "invalidate"));
        errMessage.append(LocalizedStrings.BaseCommand__THE_INPUT_KEY_FOR_THE_0_REQUEST_IS_NULL.toLocalizedString("invalidate"));
      }
      if (regionName == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.BaseCommand__THE_INPUT_REGION_NAME_FOR_THE_0_REQUEST_IS_NULL, "invalidate"));
        errMessage.append(LocalizedStrings.BaseCommand__THE_INPUT_REGION_NAME_FOR_THE_0_REQUEST_IS_NULL.toLocalizedString("invalidate"));
      }
      writeErrorResponse(msg, MessageType.DESTROY_DATA_ERROR, errMessage.toString(), servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    LocalRegion region = (LocalRegion) servConn.getCache().getRegion(regionName);
    if (region == null) {
      String reason = LocalizedStrings.BaseCommand__0_WAS_NOT_FOUND_DURING_1_REQUEST.toLocalizedString(regionName, "invalidate");
      writeRegionDestroyedEx(msg, regionName, reason, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    // Invalidate the entry
    ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
    long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    EventID eventId = new EventID(servConn.getEventMemberIDByteArray(), threadId, sequenceId);

    Breadcrumbs.setEventId(eventId);

    VersionTag tag = null;

    try {
      // for integrated security
      this.securityService.authorizeRegionWrite(regionName, key.toString());

      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        InvalidateOperationContext invalidateContext = authzRequest.invalidateAuthorize(regionName, key, callbackArg);
        callbackArg = invalidateContext.getCallbackArg();
      }
      EventIDHolder clientEvent = new EventIDHolder(eventId);

      // msg.isRetry might be set by v7.0 and later clients
      if (msg.isRetry()) {
        //            if (logger.isDebugEnabled()) {
        //              logger.debug("DEBUG: encountered isRetry in Invalidate");
        //            }
        clientEvent.setPossibleDuplicate(true);
        if (region.getAttributes().getConcurrencyChecksEnabled()) {
          // recover the version tag from other servers
          clientEvent.setRegion(region);
          if (!recoverVersionTagForRetriedOperation(clientEvent)) {
            clientEvent.setPossibleDuplicate(false); // no-one has seen this event
          }
        }
      }

      region.basicBridgeInvalidate(key, callbackArg, servConn.getProxyID(), true, clientEvent);
      tag = clientEvent.getVersionTag();
      servConn.setModificationInfo(true, regionName, key);
    } catch (EntryNotFoundException e) {
      // Don't send an exception back to the client if this
      // exception happens. Just log it and continue.
      logger.info(LocalizedMessage.create(LocalizedStrings.BaseCommand_DURING_0_NO_ENTRY_WAS_FOUND_FOR_KEY_1, new Object[] {
        "invalidate", key
      }));
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
        logger.warn(LocalizedMessage.create(LocalizedStrings.BaseCommand_0_UNEXPECTED_EXCEPTION, servConn.getName()), e);
      }
      return;
    }

    // Update the statistics and write the reply
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessInvalidateTime(start - oldStart);
    }
    if (region instanceof PartitionedRegion) {
      PartitionedRegion pr = (PartitionedRegion) region;
      if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
        writeReplyWithRefreshMetadata(msg, servConn, pr, pr.getNetworkHopType(), tag);
        pr.clearNetworkHopData();
      } else {
        writeReply(msg, servConn, tag);
      }
    } else {
      writeReply(msg, servConn, tag);
    }
    servConn.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent invalidate response for region {} key {}", servConn.getName(), regionName, key);
    }
    stats.incWriteInvalidateResponseTime(DistributionStats.getStatTime() - start);
  }

  protected void writeReply(Message origMsg, ServerConnection servConn, VersionTag tag) throws IOException {
    writeReply(origMsg, servConn);
  }

  protected void writeReplyWithRefreshMetadata(Message origMsg,
                                               ServerConnection servConn,
                                               PartitionedRegion pr,
                                               byte nwHop,
                                               VersionTag tag) throws IOException {
    writeReplyWithRefreshMetadata(origMsg, servConn, pr, nwHop);
  }
}
