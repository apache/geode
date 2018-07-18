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
import java.util.concurrent.TimeoutException;

import org.apache.geode.CancelException;
import org.apache.geode.cache.TransactionInDoubtException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

/**
 * This is the base command which read the parts for the MessageType.COMMIT.<br>
 *
 * @since GemFire 6.6
 */
public class CommitCommand extends BaseCommand {

  private static final CommitCommand singleton = new CommitCommand();

  public static Command getCommand() {
    return singleton;
  }

  private CommitCommand() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {


    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    TXManagerImpl txMgr = (TXManagerImpl) serverConnection.getCache().getCacheTransactionManager();
    InternalDistributedMember client =
        (InternalDistributedMember) serverConnection.getProxyID().getDistributedMember();
    int uniqId = clientMessage.getTransactionId();
    TXId txId = new TXId(client, uniqId);
    TXCommitMessage commitMsg = txMgr.getRecentlyCompletedMessage(txId);
    if (commitMsg != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("TX: returning a recently committed txMessage for tx: {}", txId);
      }
      if (!txMgr.isExceptionToken(commitMsg)) {
        writeCommitResponse(commitMsg, clientMessage, serverConnection);
        commitMsg.setClientVersion(null); // fixes bug 46529
        serverConnection.setAsTrue(RESPONDED);
      } else {
        sendException(clientMessage, serverConnection, txMgr.getExceptionForToken(commitMsg, txId));
      }
      txMgr.removeHostedTXState(txId);
      return;
    }
    boolean wasInProgress = txMgr.setInProgress(true); // fixes bug 43350
    final TXStateProxy txProxy = txMgr.getTXState();
    Assert.assertTrue(txProxy != null);
    if (logger.isDebugEnabled()) {
      logger.debug("TX: committing client tx: {}", txId);
    }
    commitTransaction(clientMessage, serverConnection, txMgr, wasInProgress,
        txProxy);
  }

  protected void commitTransaction(Message clientMessage, ServerConnection serverConnection,
      TXManagerImpl txMgr,
      boolean wasInProgress, TXStateProxy txProxy) throws IOException {
    Exception txException = null;
    TXCommitMessage commitMsg = null;
    TXId txId = txProxy.getTxId();
    try {
      txProxy.setCommitOnBehalfOfRemoteStub(true);
      txMgr.commit();

      commitMsg = txProxy.getCommitMessage();
      logger.debug("Sending commit response to client: {}", commitMsg);
      writeCommitResponse(commitMsg, clientMessage, serverConnection);
      serverConnection.setAsTrue(RESPONDED);

    } catch (Exception e) {
      txException = e;
    } finally {
      if (txId != null) {
        txMgr.removeHostedTXState(txId);
      }
      if (!wasInProgress) {
        txMgr.setInProgress(false);
      }
      if (commitMsg != null) {
        commitMsg.setClientVersion(null); // fixes bug 46529
      }
    }
    if (txException != null) {
      DistributedMember target = txProxy.getTarget();
      // a TransactionInDoubtException caused by the TX host shutting down means that
      // the transaction may still be active and hold locks. We must wait for the transaction
      // host to finish shutting down before responding to the client or it could encounter
      // conflicts in retrying the transaction
      try {
        if ((txException instanceof TransactionInDoubtException)
            && (txException.getCause() instanceof CancelException)) {
          // base the wait time on the client's read-timeout setting so that we respond before
          // it gives up reading. Since we've already done a commit we've eaten up some time
          // so we use a WAG of half the read-timeout
          int timeToWait = serverConnection.getHandshake().getClientReadTimeout() / 2;
          if (timeToWait < 0) {
            return;
          }
          logger.info(
              "Waiting up to {}ms for departure of {} before throwing TransactionInDoubtException.",
              timeToWait, target);
          try {
            serverConnection.getCache().getDistributionManager().getMembershipManager()
                .waitForDeparture(target, timeToWait);
          } catch (TimeoutException e) {
            // status will be logged below
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          logger.info("Done waiting.  Transaction host {} in the cluster.",
              serverConnection.getCache().getDistributionManager().isCurrentMember(target)
                  ? "is still"
                  : "is no longer");
        }
      } finally {
        sendException(clientMessage, serverConnection, txException);
      }
    }
  }


  protected static void writeCommitResponse(TXCommitMessage response, Message origMsg,
      ServerConnection servConn) throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.setNumberOfParts(1);
    if (response != null) {
      response.setClientVersion(servConn.getClientVersion());
    }
    responseMsg.addObjPart(response, false);
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    if (logger.isDebugEnabled()) {
      logger.debug("TX: sending a nonNull response for transaction: {}",
          new TXId((InternalDistributedMember) servConn.getProxyID().getDistributedMember(),
              origMsg.getTransactionId()));
    }
    responseMsg.send(servConn);
    origMsg.clearParts();
  }

  private void sendException(Message msg, ServerConnection servConn, Throwable e)
      throws IOException {
    writeException(msg, MessageType.EXCEPTION, e, false, servConn);
    servConn.setAsTrue(RESPONDED);
  }


}
