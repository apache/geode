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
 * Tell a server to become the primary host of a server-to-client queue
 *
 * @since GemFire 5.7
 */
public class MakePrimaryOp {
  /**
   * Tell the given server to become the primary host of a server-to-client queue
   *
   * @param pool the pool to use to communicate with the server.
   * @param conn the connection to do the execution on
   * @param sentClientReady true if the client ready message has already been sent
   */
  public static void execute(ExecutablePool pool, ClientCacheConnection conn,
      boolean sentClientReady) {
    AbstractOp op = new MakePrimaryOpImpl(sentClientReady);
    pool.executeOn(conn, op);
  }

  private MakePrimaryOp() {
    // no instances allowed
  }

  private static class MakePrimaryOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public MakePrimaryOpImpl(boolean sentClientReady) {
      super(MessageType.MAKE_PRIMARY, 1);
      getMessage().addBytesPart(new byte[] {(byte) (sentClientReady ? 0x01 : 0x00)});
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

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "makePrimary");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startMakePrimary();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endMakePrimarySend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endMakePrimary(start, hasTimedOut(), hasFailed());
    }
  }
}
