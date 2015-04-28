/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.SynchronizationCommitConflictException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.internal.cache.TXCommitMessage;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;

/**
 * TXSynchronizationOp sends JTA beforeCompletion and afterCompletion
 * messages to the server pool.
 * 
 * @author bruce
 *
 */
public class TXSynchronizationOp {

  public static enum CompletionType {
    BEFORE_COMPLETION, AFTER_COMPLETION
  }

  /**
   * @param pool
   * @param status - the status of an afterCompletion notification
   * @param txId - the transaction identifier
   * @param type - BEFORE_COMPLETION or AFTER_COMPLETION
   * @return the server's commit message
   */
  public static TXCommitMessage execute(InternalPool pool, int status, int txId, CompletionType type) {
    Impl impl = new Impl(status, txId, type);
    pool.execute(impl);
    return impl.tXCommitMessageResponse;
  }
  
  static class Impl extends AbstractOp {

    private int status;
    private CompletionType type;
    TXCommitMessage tXCommitMessageResponse;

    /**
     * @param status
     * @param type
     */
    public Impl(int status, int txId, CompletionType type) {
      super(MessageType.TX_SYNCHRONIZATION, (type==CompletionType.AFTER_COMPLETION)? 3 : 2);
      this.status = status;
      this.type = type;
      getMessage().addIntPart(type.ordinal());
      getMessage().addIntPart(txId);
      if (type == CompletionType.AFTER_COMPLETION) {
        getMessage().addIntPart(status);
      }
    }
    
    @Override
    public String toString() {
      return "TXSynchronization(threadTxId=" + TXManagerImpl.getCurrentTXUniqueId()
      +"; "+this.type + "; status=" + this.status + ")";
    }

    @Override
  protected void processAck(Message msg, String opName)
    throws Exception
  {
    final int msgType = msg.getMessageType();
    if (msgType == MessageType.REPLY) {
      return;
    } else {
      Part part = msg.getPart(0);
      if (msgType == MessageType.EXCEPTION) {
        Throwable t = (Throwable) part.getObject();
        if (t instanceof CommitConflictException ||
            t instanceof SynchronizationCommitConflictException) {
          throw (GemFireException)t;
        }
      }
      super.processAck(msg, opName);
    }
  }

    
    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.client.internal.AbstractOp#processResponse(com.gemstone.gemfire.internal.cache.tier.sockets.Message)
     */
    @Override
    protected Object processResponse(Message msg) throws Exception {
      if (this.type == CompletionType.BEFORE_COMPLETION) {
        try {
          processAck(msg, type.toString());
        } catch (ServerOperationException e) {
          if (e.getCause() instanceof SynchronizationCommitConflictException) {
            throw (SynchronizationCommitConflictException)e.getCause();
          }
        }
        return null;
      } else {
        TXCommitMessage rcs = (TXCommitMessage)processObjResponse(msg, this.type.toString());
        this.tXCommitMessageResponse = rcs;
        return rcs;
      }
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.client.internal.AbstractOp#isErrorResponse(int)
     */
    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.REQUESTDATAERROR;
    }

    @Override  
    protected long startAttempt(ConnectionStats stats) {
      return stats.startTxSynchronization();
    }
    @Override  
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endTxSynchronizationSend(start, hasFailed());
    }
    @Override  
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endTxSynchronization(start, hasTimedOut(), hasFailed());
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
