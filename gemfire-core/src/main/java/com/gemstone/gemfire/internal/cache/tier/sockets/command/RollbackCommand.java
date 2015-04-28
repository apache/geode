/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.TXId;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;

/**
 * Command for performing Rollback on the server
 * @author sbawaska
 */
public class RollbackCommand extends BaseCommand {

  private final static RollbackCommand singleton = new RollbackCommand();
  
  public static Command getCommand() {
    return singleton;
  }
  
  private RollbackCommand() {
  }
  
  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException, InterruptedException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    TXManagerImpl txMgr = (TXManagerImpl)servConn.getCache().getCacheTransactionManager();
    InternalDistributedMember client = (InternalDistributedMember) servConn.getProxyID().getDistributedMember();
    int uniqId = msg.getTransactionId();
    TXId txId = new TXId(client, uniqId);
    if (txMgr.isHostedTxRecentlyCompleted(txId)) {
      if (logger.isDebugEnabled()) {
        logger.debug("TX: found a recently rolled back tx: {}", txId);
        sendRollbackReply(msg, servConn);
        txMgr.removeHostedTXState(txId);
        return;
      }
    }
    final TXStateProxy txState = txMgr.getTXState();
    try {
      if (txState != null) {
        txId = txState.getTxId();
        txMgr.rollback();
        sendRollbackReply(msg, servConn);
      }
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
    } finally {
      if (logger.isDebugEnabled()) {
        logger.debug("TX: removing tx state for {}", txId);
      }
      if (txId != null) {
        TXStateProxy proxy = txMgr.removeHostedTXState(txId);
        if (logger.isDebugEnabled()) {
          logger.debug("TX: removed tx state proxy {}", proxy);
        }
      }
    }
  }

  private void sendRollbackReply(Message msg, ServerConnection servConn)
      throws IOException {
    writeReply(msg, servConn);
    servConn.setAsTrue(RESPONDED);
  }

}
