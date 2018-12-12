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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.operations.KeySetOperationContext;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class KeySet extends BaseCommand {

  private static final KeySet singleton = new KeySet();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Part regionNamePart = null;
    String regionName = null;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    // Retrieve the region name from the message parts
    regionNamePart = clientMessage.getPart(0);
    regionName = regionNamePart.getString();
    ChunkedMessage chunkedResponseMsg = serverConnection.getChunkedResponseMessage();
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: Received key set request ({} bytes) from {} for region {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), regionName);
    }

    // Process the key set request
    if (regionName == null) {
      String message = null;
      // if (regionName == null) (can only be null)
      {
        message = String.format("%s: The input region name for the key set request is null",
            serverConnection.getName());
        logger.warn("{}: The input region name for the key set request is null",
            serverConnection.getName());
      }
      writeKeySetErrorResponse(clientMessage, MessageType.KEY_SET_DATA_ERROR, message,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      String reason = String.format("%s was not found during key set request",
          regionName);
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    if (isInTransaction() && region.getPartitionAttributes() != null) {
      // GEODE-5186: fail the the transaction if it is a retry after failover for keySet on
      // partitioned region
      if (clientMessage.isRetry()) {
        keySetWriteChunkedException(clientMessage,
            new TransactionException(
                "Failover on a set operation of a partitioned region is not allowed in a transaction."),
            serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }

    try {
      securityService.authorize(Resource.DATA, Operation.READ, regionName);
    } catch (NotAuthorizedException ex) {
      keySetWriteChunkedException(clientMessage, ex, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    KeySetOperationContext keySetContext = null;
    AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
    if (authzRequest != null) {
      try {
        keySetContext = authzRequest.keySetAuthorize(regionName);
      } catch (NotAuthorizedException ex) {
        writeChunkedException(clientMessage, ex, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    // Update the statistics and write the reply
    // bserverStats.incLong(processDestroyTimeId,
    // DistributionStats.getStatTime() - start);
    // start = DistributionStats.getStatTime();

    // Send header
    chunkedResponseMsg.setMessageType(MessageType.RESPONSE);
    chunkedResponseMsg.setTransactionId(clientMessage.getTransactionId());
    chunkedResponseMsg.sendHeader();

    // Send chunk response
    try {
      fillAndSendKeySetResponseChunks(region, regionName, keySetContext, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, e);

      // Otherwise, write an exception message and continue
      writeChunkedException(clientMessage, e, serverConnection,
          serverConnection.getChunkedResponseMessage());
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    if (isDebugEnabled) {
      // logger.fine(getName() + ": Sent chunk (1 of 1) of register interest
      // response (" + chunkedResponseMsg.getBufferLength() + " bytes) for
      // region " + regionName + " key " + key);
      logger.debug("{}: Sent key set response for the region {}", serverConnection.getName(),
          regionName);
    }
    // bserverStats.incLong(writeDestroyResponseTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyResponsesId, 1);

  }

  protected void keySetWriteChunkedException(Message clientMessage, Throwable ex,
      ServerConnection serverConnection) throws IOException {
    writeChunkedException(clientMessage, ex, serverConnection);
  }

  private void fillAndSendKeySetResponseChunks(LocalRegion region, String regionName,
      KeySetOperationContext context, ServerConnection servConn) throws IOException {

    // Get the key set
    Set keySet = region.keys();
    KeySetOperationContext keySetContext = context;

    // Post-operation filtering
    AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
    if (postAuthzRequest != null) {
      keySetContext = postAuthzRequest.keySetAuthorize(regionName, keySet, keySetContext);
      keySet = keySetContext.getKeySet();
    }

    List keyList = new ArrayList(MAXIMUM_CHUNK_SIZE);
    final boolean isTraceEnabled = logger.isTraceEnabled();
    for (Iterator it = keySet.iterator(); it.hasNext();) {
      Object entryKey = it.next();
      keyList.add(entryKey);
      if (isTraceEnabled) {
        logger.trace("{}: fillAndSendKeySetResponseKey <{}>; list size was {}; region: {}",
            servConn.getName(), entryKey, keyList.size(), region.getFullPath());
      }
      if (keyList.size() == MAXIMUM_CHUNK_SIZE) {
        // Send the chunk and clear the list
        sendKeySetResponseChunk(region, keyList, false, servConn);
        keyList.clear();
      }
    }
    // Send the last chunk even if the list is of zero size.
    sendKeySetResponseChunk(region, keyList, true, servConn);
  }

  private static void sendKeySetResponseChunk(Region region, List list, boolean lastChunk,
      ServerConnection servConn) throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();

    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, false);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} key set response chunk for region={}{}", servConn.getName(),
          (lastChunk ? " last " : " "), region.getFullPath(),
          (logger.isTraceEnabled() ? " keys=" + list + " chunk=<" + chunkedResponseMsg + ">" : ""));
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

  boolean isInTransaction() {
    return TXManagerImpl.getCurrentTXState() != null;
  }
}
