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

import org.apache.geode.cache.client.internal.TXSynchronizationOp.CompletionType;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.MessageTooLargeException;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

public class TXSynchronizationCommand extends BaseCommand {

  private static final TXSynchronizationCommand singleton = new TXSynchronizationCommand();

  public static Command getCommand() {
    return singleton;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.tier.sockets.BaseCommand#cmdExecute(org.apache.geode.internal.
   * cache.tier.sockets.Message, org.apache.geode.internal.cache.tier.sockets.ServerConnection,
   * long)
   */
  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start)
      throws IOException, ClassNotFoundException, InterruptedException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    CompletionType type = CompletionType.values()[clientMessage.getPart(0).getInt()];
    /* int txIdInt = */ clientMessage.getPart(1).getInt(); // [bruce] not sure if we need to
                                                           // transmit this
    final Part statusPart;
    if (type == CompletionType.AFTER_COMPLETION) {
      statusPart = clientMessage.getPart(2);
    } else {
      statusPart = null;
    }

    final TXManagerImpl txMgr = getTXManager(serverConnection);
    final InternalDistributedMember member = getDistributedMember(serverConnection);

    final TXStateProxy txProxy = txMgr.getTXState();
    assert txProxy != null;

    final TXId txId = txProxy.getTxId();
    TXCommitMessage commitMessage = txMgr.getRecentlyCompletedMessage(txId);
    if (commitMessage != null && commitMessage != TXCommitMessage.ROLLBACK_MSG) {
      assert type == CompletionType.AFTER_COMPLETION;
      try {
        writeCommitResponse(clientMessage, serverConnection, commitMessage);
      } catch (IOException e) {
        if (isDebugEnabled) {
          logger.debug("Problem writing reply to client", e);
        }
      } catch (RuntimeException e) {
        try {
          writeException(clientMessage, e, false, serverConnection);
        } catch (IOException ioe) {
          if (isDebugEnabled) {
            logger.debug("Problem writing reply to client", ioe);
          }
        }
      }
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    try {
      if (type == CompletionType.BEFORE_COMPLETION) {
        if (isDebugEnabled) {
          logger.debug("Executing beforeCompletion() notification for transaction {}",
              clientMessage.getTransactionId());
        }
        Throwable failureException = null;
        try {
          txProxy.setIsJTA(true);
          txProxy.beforeCompletion();
          try {
            writeReply(clientMessage, serverConnection);
          } catch (IOException e) {
            if (isDebugEnabled) {
              logger.debug("Problem writing reply to client", e);
            }
          }
          serverConnection.setAsTrue(RESPONDED);
        } catch (ReplyException e) {
          failureException = e.getCause();
        } catch (Exception e) {
          failureException = e;
        }
        if (failureException != null) {
          try {
            writeException(clientMessage, failureException, false, serverConnection);
          } catch (IOException ioe) {
            if (isDebugEnabled) {
              logger.debug("Problem writing reply to client", ioe);
            }
          }
          serverConnection.setAsTrue(RESPONDED);
        }
      } else {
        try {
          int status = statusPart.getInt();
          if (isDebugEnabled) {
            logger.debug("Executing afterCompletion({}) notification for transaction {}",
                status, clientMessage.getTransactionId());
          }
          txProxy.setIsJTA(true);
          txProxy.setCommitOnBehalfOfRemoteStub(true);
          txProxy.afterCompletion(status);
          // GemFire commits during afterCompletion - send the commit info back to the client
          // where it can be applied to the local cache
          TXCommitMessage cmsg = txProxy.getCommitMessage();
          try {
            writeCommitResponse(clientMessage, serverConnection, cmsg);
            txMgr.removeHostedTXState(txProxy.getTxId());
          } catch (IOException e) {
            // not much can be done here
            if (isDebugEnabled || (e instanceof MessageTooLargeException)) {
              logger.warn("Problem writing reply to client", e);
            }
          }
          serverConnection.setAsTrue(RESPONDED);
        } catch (RuntimeException e) {
          try {
            writeException(clientMessage, e, false, serverConnection);
          } catch (IOException ioe) {
            if (isDebugEnabled) {
              logger.debug("Problem writing reply to client", ioe);
            }
          }
          serverConnection.setAsTrue(RESPONDED);
        }
      }
    } catch (Exception e) {
      writeException(clientMessage, MessageType.EXCEPTION, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    }
    if (isDebugEnabled) {
      logger.debug("Sent tx synchronization response");
    }
  }

  void writeCommitResponse(Message clientMessage, ServerConnection serverConnection,
      TXCommitMessage commitMessage) throws IOException {
    CommitCommand.writeCommitResponse(commitMessage, clientMessage, serverConnection);
  }

  InternalDistributedMember getDistributedMember(ServerConnection serverConnection) {
    return (InternalDistributedMember) serverConnection.getProxyID().getDistributedMember();
  }

  TXManagerImpl getTXManager(ServerConnection serverConnection) {
    return (TXManagerImpl) serverConnection.getCache().getCacheTransactionManager();
  }
}
