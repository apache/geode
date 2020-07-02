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

package org.apache.geode.cache.client.internal;

import org.apache.geode.GemFireException;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;

/**
 * TXSynchronizationOp sends JTA beforeCompletion and afterCompletion messages to the server pool.
 *
 *
 */
public class TXSynchronizationOp {

  public static enum CompletionType {
    BEFORE_COMPLETION, AFTER_COMPLETION
  }

  /**
   * @param status - the status of an afterCompletion notification
   * @param txId - the transaction identifier
   * @param type - BEFORE_COMPLETION or AFTER_COMPLETION
   * @return the server's commit message
   */
  public static TXCommitMessage execute(InternalPool pool, int status, int txId,
      CompletionType type) {
    Impl impl = new Impl(status, txId, type);
    pool.execute(impl);
    return impl.tXCommitMessageResponse;
  }

  static class Impl extends AbstractOp {

    private int status;
    private CompletionType type;
    TXCommitMessage tXCommitMessageResponse;

    public Impl(int status, int txId, CompletionType type) {
      super(MessageType.TX_SYNCHRONIZATION, (type == CompletionType.AFTER_COMPLETION) ? 3 : 2);
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
      return "TXSynchronization(threadTxId=" + TXManagerImpl.getCurrentTXUniqueId() + "; "
          + this.type + "; status=" + this.status + ")";
    }

    @Override
    protected void processAck(Message msg, String opName) throws Exception {
      final int msgType = msg.getMessageType();
      if (msgType == MessageType.REPLY) {
        return;
      } else {
        Part part = msg.getPart(0);
        if (msgType == MessageType.EXCEPTION) {
          Throwable t = (Throwable) part.getObject();
          if (t instanceof CommitConflictException
              || t instanceof SynchronizationCommitConflictException) {
            throw (GemFireException) t;
          }
        }
        super.processAck(msg, opName);
      }
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.geode.cache.client.internal.AbstractOp#processResponse(org.apache.geode.internal.
     * cache.tier.sockets.Message)
     */
    @Override
    protected Object processResponse(Message msg) throws Exception {
      if (this.type == CompletionType.BEFORE_COMPLETION) {
        try {
          processAck(msg, type.toString());
        } catch (ServerOperationException e) {
          if (e.getCause() instanceof SynchronizationCommitConflictException) {
            throw (SynchronizationCommitConflictException) e.getCause();
          }
        }
        return null;
      } else {
        TXCommitMessage rcs = (TXCommitMessage) processObjResponse(msg, this.type.toString());
        this.tXCommitMessageResponse = rcs;
        return rcs;
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.cache.client.internal.AbstractOp#isErrorResponse(int)
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
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(ClientCacheConnection cnx) throws Exception {
      getMessage().clearMessageHasSecurePartFlag();
      getMessage().send(false);
    }
  }

}
