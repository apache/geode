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
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.WaitForViewInstallation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.FindRemoteTXMessage;
import org.apache.geode.internal.cache.FindRemoteTXMessage.FindRemoteTXMessageReplyProcessor;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PeerTXStateStub;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

/**
 * Used for bootstrapping txState/PeerTXStateStub on the server. This command is send when in client
 * in a transaction is about to failover to this server
 */
public class TXFailoverCommand extends BaseCommand {

  @Immutable
  private static final Command singleton = new TXFailoverCommand();

  public static Command getCommand() {
    return singleton;
  }

  TXFailoverCommand() {
    // nothing
  }

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start)
      throws IOException, ClassNotFoundException, InterruptedException {

    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    // Build the TXId for the transaction
    InternalDistributedMember client =
        (InternalDistributedMember) serverConnection.getProxyID().getDistributedMember();
    int uniqId = clientMessage.getTransactionId();
    if (logger.isDebugEnabled()) {
      logger.debug("TX: Transaction {} from {} is failing over to this server", uniqId, client);
    }
    TXId txId = createTXId(client, uniqId);
    TXManagerImpl mgr = (TXManagerImpl) serverConnection.getCache().getCacheTransactionManager();
    mgr.waitForCompletingTransaction(txId); // in case it's already completing here in another
                                            // thread
    if (mgr.isHostedTxRecentlyCompleted(txId)) {
      writeReply(clientMessage, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      mgr.removeHostedTXState(txId);
      return;
    }

    boolean wasInProgress = mgr.setInProgress(true); // fixes bug 43350
    TXStateProxy tx = mgr.getTXState();
    Assert.assertTrue(tx != null);

    if (!tx.isRealDealLocal()) {
      // send message to all peers to find out who hosts the transaction
      FindRemoteTXMessageReplyProcessor processor =
          sendFindRemoteTXMessage(serverConnection.getCache(), txId);
      try {
        processor.waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        e.handleCause();
      }
      // if hosting member is not null, bootstrap PeerTXStateStub to that member
      // if hosting member is null, rebuild TXCommitMessage from partial TXCommitMessages
      InternalDistributedMember hostingMember = processor.getHostingMember();
      if (hostingMember != null) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "TX: txState is not local, bootstrapping PeerTXState stub for targetNode: {}",
              hostingMember);
        }
        // GEODE-3310 set the target node in the tx
        if (tx.getTarget() == null) {
          tx.setTarget(hostingMember);
        }
        // inject the real deal
        tx.setLocalTXState(new PeerTXStateStub(tx, hostingMember, client));
      } else {
        // bug #42228 and bug #43504 - this cannot return until the current view
        // has been installed by all members, so that dlocks are released and
        // the same keys can be used in a new transaction by the same client thread
        InternalCache cache = serverConnection.getCache();
        try {
          WaitForViewInstallation.send((ClusterDistributionManager) cache.getDistributionManager());
        } catch (InterruptedException e) {
          cache.getDistributionManager().getCancelCriterion().checkCancelInProgress(e);
          Thread.currentThread().interrupt();
        }
        // tx host has departed, rebuild the tx
        if (processor.getTxCommitMessage() != null) {
          if (logger.isDebugEnabled()) {
            logger.debug("TX: for txId: {} rebuilt a recently completed tx", txId);
          }
          mgr.saveTXCommitMessageForClientFailover(txId, processor.getTxCommitMessage());
        } else {
          writeException(clientMessage, new TransactionDataNodeHasDepartedException(
              "Could not find transaction host for " + txId), false, serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          mgr.removeHostedTXState(txId, true);
          return;
        }
      }
    }
    if (!wasInProgress) {
      mgr.setInProgress(false);
    }
    writeReply(clientMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
  }

  FindRemoteTXMessageReplyProcessor sendFindRemoteTXMessage(InternalCache cache, TXId txId) {
    return FindRemoteTXMessage.send(cache, txId);
  }

  TXId createTXId(InternalDistributedMember client, int uniqId) {
    return new TXId(client, uniqId);
  }

}
