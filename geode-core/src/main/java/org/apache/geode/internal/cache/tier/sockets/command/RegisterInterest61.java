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

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.client.internal.RegisterInterestOp;
import org.apache.geode.cache.operations.RegisterInterestOperationContext;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.vmotion.VMotionObserver;
import org.apache.geode.internal.cache.vmotion.VMotionObserverHolder;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * Command for {@link RegisterInterestOp}
 */
public class RegisterInterest61 extends BaseCommand {

  @Immutable
  private static final RegisterInterest61 singleton = new RegisterInterest61();

  /**
   * A debug flag used for testing vMotion during CQ registration
   */
  public static final boolean VMOTION_DURING_REGISTER_INTEREST_FLAG = false;

  public static Command getCommand() {
    return singleton;
  }

  RegisterInterest61() {}

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, final long start)
      throws IOException, InterruptedException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    // Retrieve the data from the message parts
    final Part regionNamePart = clientMessage.getPart(0);
    final String regionName = regionNamePart.getCachedString();

    final InterestType interestType = InterestType.valueOf(clientMessage.getPart(1).getInt());

    final InterestResultPolicy policy;
    try {
      policy = (InterestResultPolicy) clientMessage.getPart(2).getObject();
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final boolean isDurable;
    try {
      Part durablePart = clientMessage.getPart(3);
      byte[] durablePartBytes = (byte[]) durablePart.getObject();
      isDurable = durablePartBytes[0] == 0x01;
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final DataPolicy regionDataPolicy;
    final boolean serializeValues;
    try {
      final Part regionDataPolicyPart = clientMessage.getPart(clientMessage.getNumberOfParts() - 1);
      final byte[] regionDataPolicyPartBytes = (byte[]) regionDataPolicyPart.getObject();
      // The second byte here is serializeValues
      regionDataPolicy = DataPolicy.fromOrdinal(regionDataPolicyPartBytes[0]);
      serializeValues = regionDataPolicyPartBytes[1] == (byte) 0x01;
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    Object key;
    try {
      final Part keyPart = clientMessage.getPart(4);
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final boolean sendUpdatesAsInvalidates;
    try {
      final Part notifyPart = clientMessage.getPart(5);
      final byte[] notifyPartBytes = (byte[]) notifyPart.getObject();
      sendUpdatesAsInvalidates = notifyPartBytes[0] == 0x01;
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Received register interest 61 request ({} bytes) from {} for region {} key {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), regionName, key);
    }

    testHookToTriggerVMotionBeforeRegisterInterest();

    // Process the register interest request
    if (key == null || regionName == null) {
      String message = null;
      if (key == null) {
        message =
            "The input key for the register interest request is null";
      }
      if (regionName == null) {
        message =
            "The input region name for the register interest request is null.";
      }
      logger.warn("{}: {}", serverConnection.getName(), message);
      writeChunkedErrorResponse(clientMessage, MessageType.REGISTER_INTEREST_DATA_ERROR,
          message, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      logger.info("{}: Region named {} was not found during register interest request.",
          serverConnection.getName(), regionName);
    }

    try {
      if (interestType == InterestType.REGULAR_EXPRESSION) {
        securityService.authorize(Resource.DATA, Operation.READ, regionName);
      } else {
        securityService.authorize(Resource.DATA, Operation.READ, regionName, key);
      }
      AuthorizeRequest authorizeRequest = serverConnection.getAuthzRequest();
      if (authorizeRequest != null) {
        if (!DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          RegisterInterestOperationContext registerContext =
              authorizeRequest.registerInterestAuthorize(regionName, key, interestType, policy);
          key = registerContext.getKey();
        }
      }
      serverConnection.getAcceptor().getCacheClientNotifier().registerClientInterest(regionName,
          key, serverConnection.getProxyID(), interestType, isDurable, sendUpdatesAsInvalidates,
          true, regionDataPolicy, true);
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, e);
      // Otherwise, write an exception message and continue
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    CacheClientProxy ccp = serverConnection.getAcceptor().getCacheClientNotifier()
        .getClientProxy(serverConnection.getProxyID());
    if (ccp == null) {
      IOException ioException =
          new IOException("CacheClientProxy for this client is no longer on the server");
      writeChunkedException(clientMessage, ioException, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    boolean isPrimary = ccp.isPrimary();
    ChunkedMessage chunkedResponseMsg = serverConnection.getRegisterInterestResponseMessage();
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
    } else {

      // Send header which describes how many chunks will follow
      chunkedResponseMsg.setMessageType(MessageType.RESPONSE_FROM_PRIMARY);
      chunkedResponseMsg.setTransactionId(clientMessage.getTransactionId());
      chunkedResponseMsg.sendHeader();

      // Send chunk response
      try {
        if (region instanceof PartitionedRegion
            && region.getDistributionManager() instanceof LonerDistributionManager) {
          throw new IllegalStateException(
              "Should not register interest for a partitioned region when mcast-port is 0 and no locator is present");
        }
        fillAndSendRegisterInterestResponseChunks(region, key, interestType, serializeValues,
            policy, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
      } catch (Exception e) {
        writeChunkedException(clientMessage, e, serverConnection, chunkedResponseMsg);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: Sent register interest response for region {} key {}",
            serverConnection.getName(), regionName, key);
      }
    }

  }

  /**
   * test hook to trigger vMotion during register Interest
   */
  private static void testHookToTriggerVMotionBeforeRegisterInterest() {
    if (VMOTION_DURING_REGISTER_INTEREST_FLAG) {
      VMotionObserver vmo = VMotionObserverHolder.getInstance();
      vmo.vMotionBeforeRegisterInterest();
    }
  }

}
