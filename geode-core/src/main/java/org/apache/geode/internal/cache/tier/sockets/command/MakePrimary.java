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
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;


public class MakePrimary extends BaseCommand {

  @Immutable
  private static final MakePrimary singleton = new MakePrimary();

  public static Command getCommand() {
    return singleton;
  }

  private MakePrimary() {}

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start)
      throws IOException, ClassNotFoundException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    Part isClientReadyPart = clientMessage.getPart(0);
    byte[] isClientReadyPartBytes = (byte[]) isClientReadyPart.getObject();
    boolean isClientReady = isClientReadyPartBytes[0] == 0x01;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: Received make primary request ({} bytes) isClientReady={}: from {}",
          serverConnection.getName(), clientMessage.getPayloadLength(), isClientReady,
          serverConnection.getSocketString());
    }
    try {
      serverConnection.getAcceptor().getCacheClientNotifier()
          .makePrimary(serverConnection.getProxyID(), isClientReady);
      writeReply(clientMessage, serverConnection);
      serverConnection.setAsTrue(RESPONDED);

      if (isDebugEnabled) {
        logger.debug("{}: Sent make primary response for {}", serverConnection.getName(),
            serverConnection.getSocketString());
      }
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    }
  }

}
