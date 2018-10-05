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
import java.util.List;

import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.operations.RegisterInterestOperationContext;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class RegisterInterestList extends BaseCommand {

  private static final RegisterInterestList singleton = new RegisterInterestList();

  public static Command getCommand() {
    return singleton;
  }

  RegisterInterestList() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Part regionNamePart = null, keyPart = null, numberOfKeysPart = null;
    String regionName = null;
    Object key = null;
    InterestResultPolicy policy;
    List keys = null;
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    int numberOfKeys = 0, partNumber = 0;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    ChunkedMessage chunkedResponseMsg = serverConnection.getRegisterInterestResponseMessage();

    // bserverStats.incLong(readDestroyRequestTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyRequestsId, 1);
    // start = DistributionStats.getStatTime();
    // Retrieve the data from the message parts
    regionNamePart = clientMessage.getPart(0);
    regionName = regionNamePart.getString();

    // Retrieve the InterestResultPolicy
    try {
      policy = (InterestResultPolicy) clientMessage.getPart(1).getObject();
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    boolean isDurable = false;
    try {
      Part durablePart = clientMessage.getPart(2);
      byte[] durablePartBytes = (byte[]) durablePart.getObject();
      isDurable = durablePartBytes[0] == 0x01;
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    numberOfKeysPart = clientMessage.getPart(3);
    numberOfKeys = numberOfKeysPart.getInt();

    partNumber = 4;
    keys = new ArrayList();
    for (int i = 0; i < numberOfKeys; i++) {
      keyPart = clientMessage.getPart(partNumber + i);
      try {
        key = keyPart.getStringOrObject();
      } catch (Exception e) {
        writeChunkedException(clientMessage, e, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
      keys.add(key);
    }

    boolean sendUpdatesAsInvalidates = false;

    // VJR: Check for an extra part for client version 6.0.3 onwards for the
    // time being until refactoring into a new command version.
    if (clientMessage.getNumberOfParts() > (numberOfKeys + partNumber)) {
      try {
        Part notifyPart = clientMessage.getPart(numberOfKeys + partNumber);
        byte[] notifyPartBytes = (byte[]) notifyPart.getObject();
        sendUpdatesAsInvalidates = notifyPartBytes[0] == 0x01;
      } catch (Exception e) {
        writeChunkedException(clientMessage, e, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Received register interest request ({} bytes) from {} for the following {} keys in region {}: {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), numberOfKeys, regionName, keys);
    }

    /*
     * AcceptorImpl acceptor = servConn.getAcceptor();
     *
     * // Check if the Server is running in NotifyBySubscription=true mode. if
     * (!acceptor.getCacheClientNotifier().getNotifyBySubscription()) { // This should have been
     * taken care at the client. String err = LocalizedStrings.
     * RegisterInterest_INTEREST_REGISTRATION_IS_SUPPORTED_ONLY_FOR_SERVERS_WITH_NOTIFYBYSUBSCRIPTION_SET_TO_TRUE
     * ); writeChunkedErrorResponse(msg,
     * MessageType.REGISTER_INTEREST_DATA_ERROR, err, servConn); servConn.setAsTrue(RESPONDED);
     * return; }
     */

    // Process the register interest request
    if (keys.isEmpty() || regionName == null) {
      String errMessage = null;
      if (keys.isEmpty() && regionName == null) {
        errMessage =
            "The input list of keys is empty and the input region name is null for the register interest request.";
      } else if (keys.isEmpty()) {
        errMessage =
            "The input list of keys for the register interest request is empty.";
      } else if (regionName == null) {
        errMessage =
            "The input region name for the register interest request is null.";
      }
      logger.warn("{}: {}", serverConnection.getName(), errMessage);
      writeChunkedErrorResponse(clientMessage, MessageType.REGISTER_INTEREST_DATA_ERROR, errMessage,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // key not null
    LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      logger.info("{}: Region named {} was not found during register interest list request.",
          serverConnection.getName(), regionName);
      // writeChunkedErrorResponse(msg,
      // MessageType.REGISTER_INTEREST_DATA_ERROR, message);
      // responded = true;
    } // else { // region not null
    try {
      securityService.authorize(Resource.DATA, Operation.READ, regionName);
      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        if (!DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          RegisterInterestOperationContext registerContext =
              authzRequest.registerInterestListAuthorize(regionName, keys, policy);
          keys = (List) registerContext.getKey();
        }
      }
      // Register interest
      serverConnection.getAcceptor().getCacheClientNotifier().registerClientInterest(regionName,
          keys, serverConnection.getProxyID(), isDurable, sendUpdatesAsInvalidates, false, 0, true);
    } catch (Exception ex) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, ex);
      // Otherwise, write an exception message and continue
      writeChunkedException(clientMessage, ex, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // Update the statistics and write the reply
    // bserverStats.incLong(processDestroyTimeId,
    // DistributionStats.getStatTime() - start);
    // start = DistributionStats.getStatTime();

    boolean isPrimary = serverConnection.getAcceptor().getCacheClientNotifier()
        .getClientProxy(serverConnection.getProxyID()).isPrimary();
    if (!isPrimary) {
      chunkedResponseMsg.setMessageType(MessageType.RESPONSE_FROM_SECONDARY);
      chunkedResponseMsg.setTransactionId(clientMessage.getTransactionId());
      chunkedResponseMsg.sendHeader();
      chunkedResponseMsg.setLastChunk(true);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: Sending register interest response chunk from secondary for region: {} for key: {} chunk=<{}>",
            serverConnection.getName(), regionName, key, chunkedResponseMsg);
      }
      chunkedResponseMsg.sendChunk(serverConnection);
    } else { // isPrimary
      // Send header which describes how many chunks will follow
      chunkedResponseMsg.setMessageType(MessageType.RESPONSE_FROM_PRIMARY);
      chunkedResponseMsg.setTransactionId(clientMessage.getTransactionId());
      chunkedResponseMsg.sendHeader();

      // Send chunk response
      try {
        fillAndSendRegisterInterestResponseChunks(region, keys, InterestType.KEY, policy,
            serverConnection);
        serverConnection.setAsTrue(RESPONDED);
      } catch (Exception e) {
        // If an interrupted exception is thrown , rethrow it
        checkForInterrupt(serverConnection, e);

        // otherwise send the exception back to client
        writeChunkedException(clientMessage, e, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }

      if (logger.isDebugEnabled()) {
        // logger.debug(getName() + ": Sent chunk (1 of 1) of register interest
        // response (" + chunkedResponseMsg.getBufferLength() + " bytes) for
        // region " + regionName + " key " + key);
        logger.debug(
            "{}: Sent register interest response for the following {} keys in region {}: {}",
            serverConnection.getName(), numberOfKeys, regionName, keys);
      }
      // bserverStats.incLong(writeDestroyResponseTimeId,
      // DistributionStats.getStatTime() - start);
      // bserverStats.incInt(destroyResponsesId, 1);
    } // isPrimary
    // } // region not null
  }

}
