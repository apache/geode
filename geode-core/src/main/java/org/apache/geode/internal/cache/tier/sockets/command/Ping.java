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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedPingMessage;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

public class Ping extends BaseCommand {

  @Immutable
  private static final Ping singleton = new Ping();

  public static Command getCommand() {
    return singleton;
  }

  private Ping() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    logger.info("[JUAN]: {}: Received PING with tx {} from {} at {}", serverConnection.getName(),
        clientMessage.getTransactionId(), serverConnection.getSocketString(),
        (DistributionStats.getStatTime() - start));

    if (clientMessage.getNumberOfParts() > 0) {
      try {
        InternalDistributedMember targetServer =
            (InternalDistributedMember) clientMessage.getPart(0).getObject();
        InternalDistributedMember myID = serverConnection.getCache().getMyId();

        if (!myID.equals(targetServer)) {
          logger.info("[JUAN]: I'm ({}) not the target ({}) -> myID.equals(targetServer) = {}...",
              myID, targetServer, myID.equals(targetServer));

          if (myID.compareTo(targetServer, true, false) == 0) {
            logger.info(
                "[JUAN]: I'm ({}) the Target ({}) but the client has an invalid viewId ({}) cached, writing error...",
                myID, targetServer, myID.getVmViewId());
            writeErrorResponse(clientMessage, MessageType.EXCEPTION, serverConnection);
            logger.info(
                "[JUAN]: I'm ({}) the Target ({}) but the client has an invalid viewId ({}) cached, writing error... Done!.",
                myID, targetServer, myID.getVmViewId());
          } else {
            logger.info("[JUAN]: Redirecting ping to {}...", targetServer);
            pingCorrectServer(clientMessage, targetServer, serverConnection);
            logger.info("[JUAN]: Redirecting ping to {}... Done!.", targetServer);
          }

          serverConnection.setAsTrue(RESPONDED);
          return;
        }
      } catch (ClassNotFoundException e) {
        logger.warn("Unable to deserialize message from " + serverConnection.getProxyID());
        writeErrorResponse(clientMessage, MessageType.EXCEPTION, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    ClientHealthMonitor chm = ClientHealthMonitor.getInstance();
    if (chm != null) {
      chm.receivedPing(serverConnection.getProxyID());
    }

    writeReply(clientMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
    // if (isDebugEnabled) {
    // logger.debug("{}: Sent ping reply to {}", serverConnection.getName(),
    // serverConnection.getSocketString());
    // }
    logger.info("[JUAN]: {}: PING replied with tx {} to {}.", serverConnection.getName(),
        clientMessage.getTransactionId(), serverConnection.getSocketString());
  }

  /**
   * Process a ping request that was sent to the wrong server
   */
  protected void pingCorrectServer(Message clientMessage, DistributedMember targetServer,
      ServerConnection serverConnection) throws IOException {
    logger.info("[JUAN]: Received a Ping request from {} intended for {}. Forwarding the ping...",
        serverConnection.getProxyID(), targetServer);

    if (!serverConnection.getCache().getDistributionManager().isCurrentMember(targetServer)) {
      logger.info("[JUAN]: Unable to ping non-member {} for client {}, writing error...",
          targetServer, serverConnection.getProxyID());
      writeErrorResponse(clientMessage, MessageType.EXCEPTION, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      logger.info("[JUAN]: Unable to ping non-member {} for client {}, writing error... Done!",
          targetServer, serverConnection.getProxyID());
    } else {
      logger.info("[JUAN]: Redirecting ping through DistributedPingMessage...");
      // send a ping message to the server. This is a one-way message that doesn't send a reply
      final DistributedPingMessage distributedPingMessage =
          new DistributedPingMessage(targetServer, serverConnection.getProxyID());
      serverConnection.getCache().getDistributionManager().putOutgoing(distributedPingMessage);
      writeReply(clientMessage, serverConnection);
      logger.info("[JUAN]: Redirecting ping through DistributedPingMessage... Done!.");
    }
  }

  @Override
  protected void writeReply(Message origMsg, ServerConnection serverConnection) throws IOException {
    Message replyMsg = serverConnection.getReplyMessage();
    serverConnection.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(1);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(okBytes());
    replyMsg.send(serverConnection);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {}", serverConnection.getName(), origMsg.getTransactionId());
    }
  }
}
