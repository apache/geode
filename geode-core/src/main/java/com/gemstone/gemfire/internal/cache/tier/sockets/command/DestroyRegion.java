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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.cache.operations.RegionDestroyOperationContext;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DistributionStats;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DestroyRegion extends BaseCommand {

  private final static DestroyRegion singleton = new DestroyRegion();

  public static Command getCommand() {
    return singleton;
  }

  private DestroyRegion() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {
    Part regionNamePart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null;
    Part eventPart = null;
    StringBuffer errMessage = new StringBuffer();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    servConn.setAsTrue(REQUIRES_RESPONSE);

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadDestroyRegionRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    eventPart = msg.getPart(1);
//    callbackArgPart = null; (redundant assignment)
    if (msg.getNumberOfParts() > 2) {
      callbackArgPart = msg.getPart(2);
      try {
        callbackArg = callbackArgPart.getObject();
      }
      catch (DistributedSystemDisconnectedException se) {
        // FIXME this can't happen
        if (logger.isDebugEnabled()) {
          logger.debug("{} ignoring message of type {} from client {} because shutdown occurred during message processing.", servConn.getName(), MessageType.getString(msg.getMessageType()), servConn.getProxyID());
        }

        servConn.setFlagProcessMessagesAsFalse();
        return;
      }
      catch (Exception e) {
        writeException(msg, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getString();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received destroy region request ({} bytes) from {} for region {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString(), regionName);
    }

    // Process the destroy region request
    if (regionName == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.DestroyRegion_0_THE_INPUT_REGION_NAME_FOR_THE_DESTROY_REGION_REQUEST_IS_NULL, servConn.getName()));
      errMessage
          .append(LocalizedStrings.DestroyRegion__THE_INPUT_REGION_NAME_FOR_THE_DESTROY_REGION_REQUEST_IS_NULL.toLocalizedString());

      writeErrorResponse(msg, MessageType.DESTROY_REGION_DATA_ERROR, errMessage
          .toString(), servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
      if (region == null) {
        String reason = LocalizedStrings.DestroyRegion_REGION_WAS_NOT_FOUND_DURING_DESTROY_REGION_REQUEST.toLocalizedString();
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        // Destroy the region
        ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart
            .getSerializedForm());
        long threadId = EventID
            .readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
        long sequenceId = EventID
            .readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
        EventID eventId = new EventID(servConn.getEventMemberIDByteArray(),
            threadId, sequenceId);

        try {
          AuthorizeRequest authzRequest = servConn.getAuthzRequest();
          if (authzRequest != null) {
            RegionDestroyOperationContext destroyContext = authzRequest
                .destroyRegionAuthorize(regionName, callbackArg);
            callbackArg = destroyContext.getCallbackArg();
          }
          // region.destroyRegion(callbackArg);
          region.basicBridgeDestroyRegion(callbackArg, servConn.getProxyID(),
              true /* boolean from cache Client */, eventId);
        }
        catch (DistributedSystemDisconnectedException e) {
          // FIXME better exception hierarchy would avoid this check
          if (servConn.getCachedRegionHelper().getCache().getCancelCriterion().cancelInProgress() != null) {
            if (logger.isDebugEnabled()) {
              logger.debug("{} ignoring message of type {} from client {} because shutdown occurred during message processing.", servConn.getName(), MessageType.getString(msg.getMessageType()), servConn.getProxyID());
            }
            servConn.setFlagProcessMessagesAsFalse();
          }
          else {
            writeException(msg, e, false, servConn);
            servConn.setAsTrue(RESPONDED);
          }
          return;
        }
        catch (Exception e) {
          // If an interrupted exception is thrown , rethrow it
          checkForInterrupt(servConn, e);

          // Otherwise, write an exception message and continue
          writeException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }

        // Update the statistics and write the reply
        {
          long oldStart = start;
          start = DistributionStats.getStatTime();
          stats.incProcessDestroyRegionTime(start - oldStart);
        }
        writeReply(msg, servConn);
        servConn.setAsTrue(RESPONDED);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Sent destroy region response for region {}", servConn.getName(), regionName);
        }
        stats.incWriteDestroyRegionResponseTime(DistributionStats.getStatTime()
            - start);
      }
    }
  }

}
