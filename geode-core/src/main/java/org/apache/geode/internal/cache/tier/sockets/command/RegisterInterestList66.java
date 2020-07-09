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
import java.util.List;

import org.apache.geode.annotations.Immutable;
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
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * All keys of the register interest list are being sent as a single part since 6.6. There is no
 * need to send no keys as a separate part.In earlier versions {@link RegisterInterestList61} number
 * of keys & each individual key was sent as a separate part.
 *
 * @since GemFire 6.6
 */
public class RegisterInterestList66 extends BaseCommand {

  @Immutable
  private static final RegisterInterestList66 singleton = new RegisterInterestList66();

  public static Command getCommand() {
    return singleton;
  }

  RegisterInterestList66() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Part regionNamePart = null, keyPart = null;
    String regionName = null;
    Object key = null;
    InterestResultPolicy policy;
    List keys = null;
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    int numberOfKeys = 0, partNumber = 0;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    ChunkedMessage chunkedResponseMsg = serverConnection.getRegisterInterestResponseMessage();

    // Retrieve the data from the message parts
    regionNamePart = clientMessage.getPart(0);
    regionName = regionNamePart.getCachedString();

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
    // region data policy
    byte[] regionDataPolicyPartBytes;
    boolean serializeValues = false;
    try {
      Part regionDataPolicyPart = clientMessage.getPart(clientMessage.getNumberOfParts() - 1);
      regionDataPolicyPartBytes = (byte[]) regionDataPolicyPart.getObject();
      if (serverConnection.getClientVersion().isNotOlderThan(Version.GFE_80)) {
        // The second byte here is serializeValues
        serializeValues = regionDataPolicyPartBytes[1] == (byte) 0x01;
      }
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    partNumber = 3;
    Part list = clientMessage.getPart(partNumber);
    try {
      keys = (List) list.getObject();
      numberOfKeys = keys.size();
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    boolean sendUpdatesAsInvalidates = false;
    try {
      Part notifyPart = clientMessage.getPart(partNumber + 1);
      byte[] notifyPartBytes = (byte[]) notifyPart.getObject();
      sendUpdatesAsInvalidates = notifyPartBytes[0] == 0x01;
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Received register interest 66 request ({} bytes) from {} for the following {} keys in region {}: {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), numberOfKeys, regionName, keys);
    }

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
    }

    // key not null
    LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      logger.info("{}: Region named {} was not found during register interest list request.",
          new Object[] {serverConnection.getName(), regionName});
    }
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
          keys, serverConnection.getProxyID(), isDurable, sendUpdatesAsInvalidates, true,
          regionDataPolicyPartBytes[0], true);
    } catch (Exception ex) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, ex);
      // Otherwise, write an exception message and continue
      writeChunkedException(clientMessage, ex, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

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
        fillAndSendRegisterInterestResponseChunks(region, keys, InterestType.KEY, serializeValues,
            policy, serverConnection);
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
        logger.debug(
            "{}: Sent register interest response for the following {} keys in region {}: {}",
            serverConnection.getName(), numberOfKeys, regionName, keys);
      }
    } // isPrimary
  }

}
