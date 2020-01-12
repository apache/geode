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
 * Tells the server we are ready to receive server-to-client events from durable subscriptions.
 *
 * @since GemFire 5.7
 */
public class ReadyForEventsOp {
  /**
   * Tells the primary server we are ready to receive server-to-client events from durable
   * subscriptions.
   *
   * @param pool the pool to use to communicate with the server.
   */
  public static void execute(ExecutablePool pool, QueueConnectionImpl primary) {
    AbstractOp op = new ReadyForEventsOpImpl();
    pool.executeOn(primary, op);
  }

  private ReadyForEventsOp() {
    // no instances allowed
  }

  private static class ReadyForEventsOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public ReadyForEventsOpImpl() {
      super(MessageType.CLIENT_READY, 1);
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
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "readyForEvents");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startReadyForEvents();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endReadyForEventsSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endReadyForEvents(start, hasTimedOut(), hasFailed());
    }
  }
}
