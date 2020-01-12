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

import java.util.Iterator;
import java.util.List;

import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;

/**
 * Send the primary server acknowledgement on the events this client has received and processed from
 * it.
 *
 * @since GemFire 5.7
 */
public class PrimaryAckOp {
  /**
   * Send the primary server acknowledgement on the events this client has received and processed
   * from it using connections from the given pool to communicate with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param events list of events to acknowledge
   */
  public static void execute(Connection connection, ExecutablePool pool, List events) {
    AbstractOp op = new PrimaryAckOpImpl(events);
    pool.executeOn(connection, op);
  }

  private PrimaryAckOp() {
    // no instances allowed
  }

  private static class PrimaryAckOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public PrimaryAckOpImpl(List events) {
      super(MessageType.PERIODIC_ACK, events.size());
      for (Iterator i = events.iterator(); i.hasNext();) {
        getMessage().addObjPart(i.next());
      }
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
      processAck(msg, "primaryAck");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startPrimaryAck();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endPrimaryAckSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endPrimaryAck(start, hasTimedOut(), hasFailed());
    }
  }
}
