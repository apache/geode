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

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;

/**
 * Ping a server to see if it is still alive.
 *
 * @since GemFire 5.7
 */
public class PingOp {

  /**
   * Ping the specified server to see if it is still alive
   *
   * @param pool the pool to use to communicate with the server.
   * @param serverLocation the server to do the execution on
   */
  public static void execute(ExecutablePool pool, ServerLocation serverLocation,
      DistributedMember serverID) {
    AbstractOp op = new PingOpImpl(serverID);
    pool.executeOn(serverLocation, op, false, false);
  }

  private PingOp() {
    // no instances allowed
  }

  static class PingOpImpl extends AbstractOp {

    private final DistributedMember serverID;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    PingOpImpl(DistributedMember serverID) {
      super(MessageType.PING, 0);
      this.serverID = serverID;
    }

    @Override
    protected void processSecureBytes(ClientCacheConnection cnx, Message message) throws Exception {
      super.processSecureBytes(cnx, message);
      Message.MESSAGE_TYPE.set(null);
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(ClientCacheConnection cnx) throws Exception {
      getMessage().clearMessageHasSecurePartFlag();
      getMessage().setNumberOfParts(1);
      getMessage().addObjPart(serverID);
      getMessage().send(true);
      Message.MESSAGE_TYPE.set(MessageType.PING);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "ping");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startPing();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endPingSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endPing(start, hasTimedOut(), hasFailed());
    }
  }
}
