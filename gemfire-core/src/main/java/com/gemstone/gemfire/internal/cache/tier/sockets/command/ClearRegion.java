/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
import com.gemstone.gemfire.cache.operations.RegionClearOperationContext;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import java.io.IOException;
import java.nio.ByteBuffer;


public class ClearRegion extends BaseCommand {

  private final static ClearRegion singleton = new ClearRegion();

  public static Command getCommand() {
    return singleton;
  }

  private ClearRegion() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {
    Part regionNamePart = null,  callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null;
    Part eventPart = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    servConn.setAsTrue(REQUIRES_RESPONSE);

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadClearRegionRequestTime(start - oldStart);
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
      catch (Exception e) {
        writeException(msg, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getString();
    if (logger.isDebugEnabled()) {
      logger.debug(servConn.getName() + ": Received clear region request (" + msg.getPayloadLength() + " bytes) from " + servConn.getSocketString() + " for region " + regionName);
    }

    // Process the clear region request
    if (regionName == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.ClearRegion_0_THE_INPUT_REGION_NAME_FOR_THE_CLEAR_REGION_REQUEST_IS_NULL, servConn.getName()));
      String errMessage = LocalizedStrings.ClearRegion_THE_INPUT_REGION_NAME_FOR_THE_CLEAR_REGION_REQUEST_IS_NULL.toLocalizedString();

      writeErrorResponse(msg, MessageType.CLEAR_REGION_DATA_ERROR, errMessage,
          servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
      if (region == null) {
        String reason = LocalizedStrings.ClearRegion_WAS_NOT_FOUND_DURING_CLEAR_REGION_REGUEST.toLocalizedString();
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        // Clear the region

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
            RegionClearOperationContext clearContext = authzRequest
                .clearAuthorize(regionName, callbackArg);
            callbackArg = clearContext.getCallbackArg();
          }
          region.basicBridgeClear(callbackArg, servConn.getProxyID(),
              true /* boolean from cache Client */, eventId);
        }
        catch (Exception e) {
          // If an interrupted exception is thrown , rethrow it
          checkForInterrupt(servConn, e);

          // If an exception occurs during the clear, preserve the connection
          writeException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }

        // Update the statistics and write the reply
        {
          long oldStart = start;
          start = DistributionStats.getStatTime();
          stats.incProcessClearRegionTime(start - oldStart);
        }
        writeReply(msg, servConn);
        servConn.setAsTrue(RESPONDED);
        if (logger.isDebugEnabled()) {
          logger.debug(servConn.getName() + ": Sent clear region response for region " + regionName);
        }
        stats.incWriteClearRegionResponseTime(DistributionStats.getStatTime()
            - start);
      }
    }

  }

}
