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
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.KeySetOperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class KeySet extends BaseCommand {

  private final static KeySet singleton = new KeySet();

  public static Command getCommand() {
    return singleton;
  }

  private KeySet() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {
    Part regionNamePart = null;
    String regionName = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    // Retrieve the region name from the message parts
    regionNamePart = msg.getPart(0);
    regionName = regionNamePart.getString();
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: Received key set request ({} bytes) from {} for region {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString(), regionName);
    }

    // Process the key set request
    if (regionName == null) {
      String message = null;
//      if (regionName == null) (can only be null) 
      {
        message = LocalizedStrings.KeySet_0_THE_INPUT_REGION_NAME_FOR_THE_KEY_SET_REQUEST_IS_NULL.toLocalizedString(servConn.getName());
        logger.warn(LocalizedMessage.create(LocalizedStrings.KeySet_0_THE_INPUT_REGION_NAME_FOR_THE_KEY_SET_REQUEST_IS_NULL, servConn.getName()));
      }
      writeKeySetErrorResponse(msg, MessageType.KEY_SET_DATA_ERROR, message,
          servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
      if (region == null) {
        String reason = LocalizedStrings.KeySet__0_WAS_NOT_FOUND_DURING_KEY_SET_REQUEST.toLocalizedString(regionName);
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        KeySetOperationContext keySetContext = null;
        AuthorizeRequest authzRequest = servConn.getAuthzRequest();
        if (authzRequest != null) {
          try {
            keySetContext = authzRequest.keySetAuthorize(regionName);
          }
          catch (NotAuthorizedException ex) {
            writeChunkedException(msg, ex, false, servConn);
            servConn.setAsTrue(RESPONDED);
            return;
          }
        }
        // Update the statistics and write the reply
        // bserverStats.incLong(processDestroyTimeId,
        // DistributionStats.getStatTime() - start);
        // start = DistributionStats.getStatTime();

        // Send header
        chunkedResponseMsg.setMessageType(MessageType.RESPONSE);
        chunkedResponseMsg.setTransactionId(msg.getTransactionId());
        chunkedResponseMsg.sendHeader();

        // Send chunk response
        try {
          fillAndSendKeySetResponseChunks(region, regionName, keySetContext,
              servConn);
          servConn.setAsTrue(RESPONDED);
        }
        catch (Exception e) {
          // If an interrupted exception is thrown , rethrow it
          checkForInterrupt(servConn, e);

          // Otherwise, write an exception message and continue
          writeChunkedException(msg, e, false, servConn, servConn
              .getChunkedResponseMessage());
          servConn.setAsTrue(RESPONDED);
          return;
        }

        if (isDebugEnabled) {
          // logger.fine(getName() + ": Sent chunk (1 of 1) of register interest
          // response (" + chunkedResponseMsg.getBufferLength() + " bytes) for
          // region " + regionName + " key " + key);
          logger.debug("{}: Sent key set response for the region {}", servConn.getName(), regionName);
        }
        // bserverStats.incLong(writeDestroyResponseTimeId,
        // DistributionStats.getStatTime() - start);
        // bserverStats.incInt(destroyResponsesId, 1);
      }
    }
  }

  private void fillAndSendKeySetResponseChunks(LocalRegion region,
      String regionName, KeySetOperationContext context,
      ServerConnection servConn) throws IOException {

    // Get the key set
    Set keySet = region.keys();
    KeySetOperationContext keySetContext = context;

    // Post-operation filtering
    AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
    if (postAuthzRequest != null) {
      keySetContext = postAuthzRequest.keySetAuthorize(regionName, keySet,
          keySetContext);
      keySet = keySetContext.getKeySet();
    }

    List keyList = new ArrayList(maximumChunkSize);
    final boolean isTraceEnabled = logger.isTraceEnabled();
    for (Iterator it = keySet.iterator(); it.hasNext();) {
      Object entryKey = it.next();
      keyList.add(entryKey);
      if (isTraceEnabled) {
        logger.trace("{}: fillAndSendKeySetResponseKey <{}>; list size was {}; region: {}", servConn.getName(), entryKey, keyList.size(), region.getFullPath());
      }
      if (keyList.size() == maximumChunkSize) {
        // Send the chunk and clear the list
        sendKeySetResponseChunk(region, keyList, false, servConn);
        keyList.clear();
      }
    }
    // Send the last chunk even if the list is of zero size.
    sendKeySetResponseChunk(region, keyList, true, servConn);
  }

  private static void sendKeySetResponseChunk(Region region, List list,
      boolean lastChunk, ServerConnection servConn) throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();

    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, zipValues);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} key set response chunk for region={}{}", servConn.getName(), (lastChunk ? " last " : " "), region.getFullPath(), (logger.isTraceEnabled() ? " keys=" + list + " chunk=<" + chunkedResponseMsg + ">" : ""));
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

}
