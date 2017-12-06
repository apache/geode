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

import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;

/**
 * Does a commit on a server
 *
 * @since GemFire 6.6
 */
public class CommitOp {
  /**
   * Does a commit on a server using connections from the given pool to communicate with the server.
   *
   * @param pool the pool to use to communicate with the server.
   */
  public static TXCommitMessage execute(ExecutablePool pool, int txId) {
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
     * @throws org.apache.geode.SerializationException if serialization fails
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
      return "TXCommit(txId=" + this.txId + ")";
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      TXCommitMessage rcs = (TXCommitMessage) processObjResponse(msg, "commit");
      assert rcs != null : "TxCommit response was null";
      this.tXCommitMessageResponse = rcs;
      return rcs;
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().clearMessageHasSecurePartFlag();
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
