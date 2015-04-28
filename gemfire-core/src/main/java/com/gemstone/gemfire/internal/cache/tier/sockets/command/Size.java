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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.security.GemFireSecurityException;


public class Size extends BaseCommand {

  private final static Size singleton = new Size();

  public static Command getCommand() {
    return singleton;
  }

  private Size() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {
    Part regionNamePart = null, keyPart = null, callbackArgPart = null;
    String regionName = null;
    Part eventPart = null;
    StringBuffer errMessage = new StringBuffer();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    servConn.setAsTrue(REQUIRES_RESPONSE);

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadSizeRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    regionName = regionNamePart.getString();
    
    if (regionName == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.BaseCommand__THE_INPUT_REGION_NAME_FOR_THE_0_REQUEST_IS_NULL, "size"));
      errMessage
            .append(LocalizedStrings.BaseCommand__THE_INPUT_REGION_NAME_FOR_THE_0_REQUEST_IS_NULL.toLocalizedString("size"));
      writeErrorResponse(msg, MessageType.SIZE_ERROR, errMessage
          .toString(), servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
      if (region == null) {
        String reason = LocalizedStrings.BaseCommand__0_WAS_NOT_FOUND_DURING_1_REQUEST.toLocalizedString(regionName,"size");
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        // Size the entry

        try {
          /*
           * 
           * txtodo: doesn't seem like there is any notion of authzSize
           * 
          AuthorizeRequest authzRequest = servConn.getAuthzRequest();
          if (authzRequest != null) {
            // TODO SW: This is to handle DynamicRegionFactory destroy
            // calls. Rework this when the semantics of DynamicRegionFactory are
            // cleaned up.
            if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
              RegionSizeOperationContext destroyContext = authzRequest
                  .invalidateRegionAuthorize((String)key, callbackArg);
              callbackArg = destroyContext.getCallbackArg();
            }
            else {
              SizeOperationContext destroyContext = authzRequest
                  .destroyAuthorize(regionName, key, callbackArg);
              callbackArg = destroyContext.getCallbackArg();
            }
          }
          */
          writeSizeResponse(region.size(), msg, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
        catch (RegionDestroyedException rde) {
          writeException(msg, rde, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
        catch (Exception e) {
          // If an interrupted exception is thrown , rethrow it
          checkForInterrupt(servConn, e);

          // If an exception occurs during the destroy, preserve the connection
          writeException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
          if (e instanceof GemFireSecurityException) {
            // Fine logging for security exceptions since these are already
            // logged by the security logger
            if (logger.isDebugEnabled())
              logger.debug("{}: Unexpected Security exception", servConn.getName(), e);
          }
          else {
            logger.warn(LocalizedMessage.create(LocalizedStrings.BaseCommand_0_UNEXPECTED_EXCEPTION, servConn.getName()), e); 
          }
          servConn.setAsTrue(RESPONDED);
          return;
        } finally {
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Sent size response for region {}", servConn.getName(), regionName);
          }
          stats.incWriteSizeResponseTime(DistributionStats.getStatTime()
            - start);
        }
      }
    }
    
    

  }
  
  private static void writeSizeResponse(Integer sizeCount, Message origMsg,
      ServerConnection servConn) throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.addObjPart(sizeCount);
    responseMsg.send(servConn, origMsg.getTransactionId());
  }

}
