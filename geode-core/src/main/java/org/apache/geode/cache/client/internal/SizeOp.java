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
 * Does a region size on a server
 *
 * @since GemFire 6.6
 */
public class SizeOp {
  /**
   * Does a region size on a server using connections from the given pool to communicate with the
   * server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the entry keySet on
   */
  public static Integer execute(InternalPool pool, String region) {
    AbstractOp op = new SizeOpImpl(region);
    return (Integer) pool.execute(op);
  }

  private SizeOp() {
    // no instances allowed
  }

  private static class SizeOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public SizeOpImpl(String region) {
      super(MessageType.SIZE, 1);
      getMessage().addStringPart(region, true);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {

      return processObjResponse(msg, "size");
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.SIZE_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startSize();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endSizeSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endSize(start, hasTimedOut(), hasFailed());
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
