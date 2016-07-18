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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.KeySetOperationContext;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.security.NotAuthorizedException;

public class KeySet extends BaseCommand {

  private final static KeySet singleton = new KeySet();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start) throws IOException, InterruptedException {
    Part regionNamePart = null;
    String regionName = null;
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    // Retrieve the region name from the message parts
    regionNamePart = msg.getPart(0);
    regionName = regionNamePart.getString();
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: Received key set request ({} bytes) from {} for region {}", servConn.getName(), msg.getPayloadLength(), servConn
        .getSocketString(), regionName);
    }

    // Process the key set request
    if (regionName == null) {
      String message = null;
      //      if (regionName == null) (can only be null)
      {
        message = LocalizedStrings.KeySet_0_THE_INPUT_REGION_NAME_FOR_THE_KEY_SET_REQUEST_IS_NULL.toLocalizedString(servConn
          .getName());
        logger.warn(LocalizedMessage.create(LocalizedStrings.KeySet_0_THE_INPUT_REGION_NAME_FOR_THE_KEY_SET_REQUEST_IS_NULL, servConn
          .getName()));
      }
      writeKeySetErrorResponse(msg, MessageType.KEY_SET_DATA_ERROR, message, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) servConn.getCache().getRegion(regionName);
    if (region == null) {
      String reason = LocalizedStrings.KeySet__0_WAS_NOT_FOUND_DURING_KEY_SET_REQUEST.toLocalizedString(regionName);
      writeRegionDestroyedEx(msg, regionName, reason, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    try {
      this.securityService.authorizeRegionRead(regionName);
    } catch (NotAuthorizedException ex) {
      writeChunkedException(msg, ex, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    KeySetOperationContext keySetContext = null;
    AuthorizeRequest authzRequest = servConn.getAuthzRequest();
    if (authzRequest != null) {
      try {
        keySetContext = authzRequest.keySetAuthorize(regionName);
      } catch (NotAuthorizedException ex) {
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
      fillAndSendKeySetResponseChunks(region, regionName, keySetContext, servConn);
      servConn.setAsTrue(RESPONDED);
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(servConn, e);

      // Otherwise, write an exception message and continue
      writeChunkedException(msg, e, false, servConn, servConn.getChunkedResponseMessage());
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

  private void fillAndSendKeySetResponseChunks(LocalRegion region,
                                               String regionName,
                                               KeySetOperationContext context,
                                               ServerConnection servConn) throws IOException {

    // Get the key set
    Set keySet = region.keys();
    KeySetOperationContext keySetContext = context;

    // Post-operation filtering
    AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
    if (postAuthzRequest != null) {
      keySetContext = postAuthzRequest.keySetAuthorize(regionName, keySet, keySetContext);
      keySet = keySetContext.getKeySet();
    }

    List keyList = new ArrayList(maximumChunkSize);
    final boolean isTraceEnabled = logger.isTraceEnabled();
    for (Iterator it = keySet.iterator(); it.hasNext(); ) {
      Object entryKey = it.next();
      keyList.add(entryKey);
      if (isTraceEnabled) {
        logger.trace("{}: fillAndSendKeySetResponseKey <{}>; list size was {}; region: {}", servConn.getName(), entryKey, keyList
          .size(), region.getFullPath());
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

  private static void sendKeySetResponseChunk(Region region, List list, boolean lastChunk, ServerConnection servConn)
    throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();

    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, zipValues);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} key set response chunk for region={}{}", servConn.getName(), (lastChunk ? " last " : " "), region
        .getFullPath(), (logger.isTraceEnabled() ? " keys=" + list + " chunk=<" + chunkedResponseMsg + ">" : ""));
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

}
