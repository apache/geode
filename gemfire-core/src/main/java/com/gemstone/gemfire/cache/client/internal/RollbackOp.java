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
 * Does a Rollback on the server
 * @since 6.6
 * @author sbawaska
 */
public class RollbackOp {

  /**
   * Does a rollback on the server for given transaction
   * @param pool the pool to use to communicate with the server.
   * @param txId the id of the transaction to rollback
   */
  public static void execute(ExecutablePool pool, int txId) {
    RollbackOpImpl op = new RollbackOpImpl(txId);
    pool.execute(op);
  }
  
  private RollbackOp() {
    // no instance allowed
  }
  
  private static class RollbackOpImpl extends AbstractOp {
    private int txId;

    protected RollbackOpImpl(int txId) {
      super(MessageType.ROLLBACK, 1);
      getMessage().setTransactionId(txId);
      this.txId = txId;
    }
    
    @Override
    public String toString() {
      return "Rollback(txId="+this.txId+")";
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "rollback");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.EXCEPTION;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startRollback();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endRollbackSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endRollback(start, hasTimedOut(), hasFailed());
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
