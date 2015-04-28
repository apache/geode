/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.TXCommitMessage;
import com.gemstone.gemfire.internal.cache.TXId;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.IOException;

/**
 * This is the base command which read the parts for the
 * MessageType.COMMIT.<br>
 * 
 * @author gregp
 * @since 6.6
 */
public class CommitCommand extends BaseCommand {

  private final static CommitCommand singleton = new CommitCommand();

  public static Command getCommand() {
    return singleton;
  }

  private CommitCommand() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    TXManagerImpl txMgr = (TXManagerImpl)servConn.getCache().getCacheTransactionManager();
    InternalDistributedMember client = (InternalDistributedMember) servConn.getProxyID().getDistributedMember();
    int uniqId = msg.getTransactionId();
    TXId txId = new TXId(client, uniqId);
    TXCommitMessage commitMsg = null;
    if (txMgr.isHostedTxRecentlyCompleted(txId)) {
      commitMsg = txMgr.getRecentlyCompletedMessage(txId);
      if (logger.isDebugEnabled()) {
        logger.debug("TX: returning a recently committed txMessage for tx: {}", txId);
      }
      if (!txMgr.isExceptionToken(commitMsg)) {
        writeCommitResponse(commitMsg, msg, servConn);
        commitMsg.setClientVersion(null);  // fixes bug 46529
        servConn.setAsTrue(RESPONDED);
      } else {
        sendException(msg, servConn, txMgr.getExceptionForToken(commitMsg, txId));
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
    try {

      txId = txProxy.getTxId();

      txProxy.setCommitOnBehalfOfRemoteStub(true);
      txMgr.commit();

      commitMsg = txProxy.getCommitMessage();
      writeCommitResponse(commitMsg, msg, servConn);
      servConn.setAsTrue(RESPONDED);
    }
    catch (Exception e) {
      sendException(msg, servConn,e);
    } finally {
      if(txId!=null) {
        txMgr.removeHostedTXState(txId);
      }
      if (!wasInProgress) {
        txMgr.setInProgress(false);
      }
      if (commitMsg != null) {
        commitMsg.setClientVersion(null);  // fixes bug 46529
      }
    }
  }

  
  
  protected static void writeCommitResponse(TXCommitMessage response,
      Message origMsg, ServerConnection servConn)
      throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.setNumberOfParts(1);
    response.setClientVersion(servConn.getClientVersion());
    responseMsg.addObjPart(response, zipValues);
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    if (logger.isDebugEnabled()) {
      logger.debug("TX: sending a nonNull response for transaction: {}", new TXId((InternalDistributedMember) servConn.getProxyID().getDistributedMember(), origMsg.getTransactionId()));
    }
    responseMsg.send(servConn);
    origMsg.flush();
  }
  
  private void sendException(Message msg,
      ServerConnection servConn, Throwable e) throws IOException {
      writeException(msg, MessageType.EXCEPTION,
          e, false,servConn);
      servConn.setAsTrue(RESPONDED);
  }
  
  
}
