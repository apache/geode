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

import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;

/**
 * Does a Rollback on the server
 *
 * @since GemFire 6.6
 */
public class RollbackOp {

  /**
   * Does a rollback on the server for given transaction
   *
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
      return "Rollback(txId=" + this.txId + ")";
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
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().clearMessageHasSecurePartFlag();
      getMessage().send(false);
    }
  }
}
