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
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

public class PeriodicAck extends BaseCommand {

  @Immutable
  private static final PeriodicAck singleton = new PeriodicAck();

  public static Command getCommand() {
    return singleton;
  }

  private PeriodicAck() {}

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start)
      throws IOException, ClassNotFoundException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received periodic ack request ({} bytes) from {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString());
    }
    try {
      int numEvents = clientMessage.getNumberOfParts();
      boolean success = false;
      CacheClientNotifier ccn = serverConnection.getAcceptor().getCacheClientNotifier();
      CacheClientProxy proxy = ccn.getClientProxy(serverConnection.getProxyID());
      if (proxy != null) {
        proxy.getHARegionQueue().createAckedEventsMap();
        for (int i = 0; i < numEvents; i++) {
          Part eventIdPart = clientMessage.getPart(i);
          eventIdPart.setVersion(serverConnection.getClientVersion());
          EventID eid = (EventID) eventIdPart.getObject();
          success = ccn.processDispatchedMessage(serverConnection.getProxyID(), eid);
          if (!success) {
            break;
          }
        }
      }
      if (success) {
        proxy.getHARegionQueue().setAckedEvents();
        writeReply(clientMessage, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
      }

    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent periodic ack response for {}", serverConnection.getName(),
          serverConnection.getSocketString());
    }

  }

}
