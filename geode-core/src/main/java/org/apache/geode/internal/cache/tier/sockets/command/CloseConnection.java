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
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;

public class CloseConnection extends BaseCommand {

  @Immutable
  private static final CloseConnection singleton = new CloseConnection();

  public static Command getCommand() {
    return singleton;
  }

  private CloseConnection() {}

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start) throws IOException {
    CacheServerStats stats = serverConnection.getCacheServerStats();
    long oldStart = start;
    boolean respondToClient =
        serverConnection.getClientVersion().isNotOlderThan(KnownVersion.GFE_90);
    start = DistributionStats.getStatTime();
    stats.incReadCloseConnectionRequestTime(start - oldStart);

    if (respondToClient) {
      // newer clients will wait for a response or EOFException
      serverConnection.setAsTrue(REQUIRES_RESPONSE);
    }

    try {
      serverConnection.setClientDisconnectCleanly();
      String clientHost = serverConnection.getSocketHost();
      int clientPort = serverConnection.getSocketPort();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Received close request ({} bytes) from {}:{}", serverConnection.getName(),
            clientMessage.getPayloadLength(), clientHost, clientPort);
      }

      Part keepalivePart = clientMessage.getPart(0);
      byte[] keepaliveByte = keepalivePart.getSerializedForm();
      boolean keepalive = (keepaliveByte == null || keepaliveByte[0] == 0) ? false : true;

      serverConnection.getAcceptor().getCacheClientNotifier()
          .setKeepAlive(serverConnection.getProxyID(), keepalive);

      if (logger.isDebugEnabled()) {
        logger.debug("{}: Processed close request from {}:{}, keepAlive: {}",
            serverConnection.getName(), clientHost, clientPort, keepalive);
      }
    } finally {
      if (respondToClient) {
        writeReply(clientMessage, serverConnection);
      }
      serverConnection.setFlagProcessMessagesAsFalse();

      stats.incProcessCloseConnectionTime(DistributionStats.getStatTime() - start);
    }

  }

}
