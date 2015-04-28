/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * Indicates to the server that a transaction is
 * failing over to this server. The server then
 * performs the necessary bootstrapping for the tx.
 * @author sbawaska
 * @since 6.6
 */
public class TXFailoverOp {

  public static void execute(ExecutablePool pool, int txId) {
    pool.execute(new TXFailoverOpImpl(txId));
  }
  
  private TXFailoverOp() {
    // no instance
  }
  
  private static class TXFailoverOpImpl extends AbstractOp {
    int txId;

    protected TXFailoverOpImpl(int txId) {
      super(MessageType.TX_FAILOVER, 1);
      getMessage().setTransactionId(txId);
      this.txId = txId;
    }
    
    @Override
    public String toString() {
      return "TXFailoverOp(txId="+this.txId+")";
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "txFailover");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.EXCEPTION;
    }

    @Override  
    protected long startAttempt(ConnectionStats stats) {
      return stats.startTxFailover();
    }
    @Override  
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endTxFailoverSend(start, hasFailed());
    }
    @Override  
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endTxFailover(start, hasTimedOut(), hasFailed());
    }

    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().setEarlyAck((byte)(getMessage().getEarlyAckByte() & Message.MESSAGE_HAS_SECURE_PART));
      getMessage().send(false);
    }
  }
}
