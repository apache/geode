/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.cache.TXCommitMessage;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * Does a commit on a server
 * @author gregp
 * @since 6.6
 */
public class CommitOp {
  /**
   * Does a commit on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   */
  public static TXCommitMessage execute(ExecutablePool pool,int txId)
  {
    CommitOpImpl op = new CommitOpImpl(txId);
    pool.execute(op);
    return op.getTXCommitMessageResponse();
  }
                                                               
  private CommitOp() {
    // no instances allowed
  }
  
    
  private static class CommitOpImpl extends AbstractOp {
    private int txId;
    
    private TXCommitMessage tXCommitMessageResponse = null;
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public CommitOpImpl(int txId) {
      super(MessageType.COMMIT, 1);
      getMessage().setTransactionId(txId);
      this.txId = txId;
    }

    public TXCommitMessage getTXCommitMessageResponse() {
      return tXCommitMessageResponse;
    }
    
    @Override
    public String toString() {
      return "TXCommit(txId="+this.txId+")";
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      TXCommitMessage rcs = (TXCommitMessage)processObjResponse(msg, "commit");
      assert rcs != null : "TxCommit response was null";
      this.tXCommitMessageResponse = rcs;
      return rcs;
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
    
    @Override  
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.EXCEPTION;
    }
    @Override  
    protected long startAttempt(ConnectionStats stats) {
      return stats.startCommit();
    }
    @Override  
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endCommitSend(start, hasFailed());
    }
    @Override  
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endCommit(start, hasTimedOut(), hasFailed());
    }
  }
}
