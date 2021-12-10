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
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.operations.UnregisterInterestOperationContext;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class UnregisterInterest extends BaseCommand {

  @Immutable
  private static final UnregisterInterest singleton = new UnregisterInterest();

  public static Command getCommand() {
    return singleton;
  }

  UnregisterInterest() {}

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start)
      throws ClassNotFoundException, IOException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    final Part regionNamePart = clientMessage.getPart(0);
    final InterestType interestType = InterestType.valueOf(clientMessage.getPart(1).getInt());
    final Part keyPart = clientMessage.getPart(2);
    final Part isClosingPart = clientMessage.getPart(3);
    final byte[] isClosingPartBytes = (byte[]) isClosingPart.getObject();
    final boolean isClosing = isClosingPartBytes[0] == 0x01;
    final String regionName = regionNamePart.getCachedString();
    Object key;
    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    final boolean keepAlive;
    try {
      final Part keepAlivePart = clientMessage.getPart(4);
      final byte[] keepAliveBytes = (byte[]) keepAlivePart.getObject();
      keepAlive = keepAliveBytes[0] != 0x00;
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Received unregister interest request ({} bytes) from {} for region {} key {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), regionName, key);
    }

    // Process the unregister interest request
    String errMessage = null;
    if ((key == null) && (regionName == null)) {
      errMessage = "The input region name and key for the unregister interest request are null.";
    } else if (key == null) {
      errMessage = "The input key for the unregister interest request is null.";
    } else if (regionName == null) {
      errMessage = "The input region name for the unregister interest request is null.";
    }

    if (errMessage != null) {
      logger.warn("{}: {}", serverConnection.getName(), errMessage);
      writeErrorResponse(clientMessage, MessageType.UNREGISTER_INTEREST_DATA_ERROR, errMessage,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    try {
      if (interestType == InterestType.REGULAR_EXPRESSION) {
        securityService.authorize(Resource.DATA, Operation.READ, regionName);
      } else {
        securityService.authorize(Resource.DATA, Operation.READ, regionName, key);
      }
    } catch (NotAuthorizedException ex) {
      writeException(clientMessage, ex, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    AuthorizeRequest authorizeRequest = serverConnection.getAuthzRequest();
    if (authorizeRequest != null) {
      if (!DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
        try {
          UnregisterInterestOperationContext unregisterContext =
              authorizeRequest.unregisterInterestAuthorize(regionName, key, interestType);
          key = unregisterContext.getKey();
        } catch (NotAuthorizedException ex) {
          writeException(clientMessage, ex, false, serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }
      }
    }

    serverConnection.getAcceptor().getCacheClientNotifier().unregisterClientInterest(regionName,
        key, interestType, isClosing, serverConnection.getProxyID(), keepAlive);

    writeReply(clientMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent unregister interest response for region {} key {}",
          serverConnection.getName(), regionName, key);
    }
  }
}
