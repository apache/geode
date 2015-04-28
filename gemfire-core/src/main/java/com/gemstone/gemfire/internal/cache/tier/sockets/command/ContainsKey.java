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

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import java.io.IOException;


public class ContainsKey extends BaseCommand {

  private final static ContainsKey singleton = new ContainsKey();

  public static Command getCommand() {
    return singleton;
  }

  private ContainsKey() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    Part regionNamePart = null, keyPart = null;
    String regionName = null;
    Object key = null;
    //StringBuffer errMessage = new StringBuffer();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();

    servConn.setAsTrue(REQUIRES_RESPONSE);
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadContainsKeyRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    keyPart = msg.getPart(1);
    regionName = regionNamePart.getString();
    try {
      key = keyPart.getStringOrObject();
    }
    catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received containsKey request ({} bytes) from {} for region {} key {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString(), regionName, key);
    }

    // Process the containsKey request
    if (key == null || regionName == null) {
      String errMessage = "";
      if (key == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.ContainsKey_0_THE_INPUT_KEY_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL, servConn.getName()));
        errMessage = LocalizedStrings.ContainsKey_THE_INPUT_KEY_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL.toLocalizedString();
      }
      if (regionName == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.ContainsKey_0_THE_INPUT_REGION_NAME_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL, servConn.getName()));
        errMessage = LocalizedStrings.ContainsKey_THE_INPUT_REGION_NAME_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL.toLocalizedString();
      }
      writeErrorResponse(msg, MessageType.CONTAINS_KEY_DATA_ERROR, errMessage,
          servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
      if (region == null) {
        String reason = LocalizedStrings.ContainsKey_WAS_NOT_FOUND_DURING_CONTAINSKEY_REQUEST.toLocalizedString();
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        AuthorizeRequest authzRequest = servConn.getAuthzRequest();
        if (authzRequest != null) {
          try {
            authzRequest.containsKeyAuthorize(regionName, key);
          }
          catch (NotAuthorizedException ex) {
            writeException(msg, ex, false, servConn);
            servConn.setAsTrue(RESPONDED);
            return;
          }
        }
        // Execute the containsKey
        boolean containsKey = region.containsKey(key);

        // Update the statistics and write the reply
        {
          long oldStart = start;
          start = DistributionStats.getStatTime();
          stats.incProcessContainsKeyTime(start - oldStart);
        }
        writeContainsKeyResponse(containsKey, msg, servConn);
        servConn.setAsTrue(RESPONDED);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Sent containsKey response for region {} key {}", servConn.getName(), regionName, key);
        }
        stats.incWriteContainsKeyResponseTime(DistributionStats.getStatTime()
            - start);
      }
    }

  }

  private static void writeContainsKeyResponse(boolean containsKey, Message origMsg,
      ServerConnection servConn) throws IOException {
    LogWriterI18n logger = servConn.getLogWriter();
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.addObjPart(containsKey ? Boolean.TRUE : Boolean.FALSE);
    responseMsg.send(servConn, origMsg.getTransactionId());
  }

}
