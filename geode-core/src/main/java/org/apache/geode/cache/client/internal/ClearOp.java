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
import org.apache.geode.internal.cache.EventID;

/**
 * Does a region clear (or create) on a server
 * 
 * @since GemFire 5.7
 */
public class ClearOp {
  /**
   * Does a region clear on a server using connections from the given pool to communicate with the
   * server.
   * 
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the clear on
   * @param eventId the event id for this clear
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static void execute(ExecutablePool pool, String region, EventID eventId,
      Object callbackArg) {
    AbstractOp op = new ClearOpImpl(region, eventId, callbackArg);
    pool.execute(op);
  }

  /**
   * Does a region clear on a server using the given connection from the given pool to communicate
   * with the server.
   * 
   * @param con the connection to use to send to the server
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the clear on
   * @param eventId the event id for this clear
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static void execute(Connection con, ExecutablePool pool, String region, EventID eventId,
      Object callbackArg) {
    AbstractOp op = new ClearOpImpl(region, eventId, callbackArg);
    pool.executeOn(con, op);
  }

  private ClearOp() {
    // no instances allowed
  }

  private static class ClearOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public ClearOpImpl(String region, EventID eventId, Object callbackArg) {
      super(MessageType.CLEAR_REGION, callbackArg != null ? 3 : 2);
      getMessage().addStringPart(region);
      getMessage().addBytesPart(eventId.calcBytes());
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "clear region");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.CLEAR_REGION_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startClear();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endClearSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endClear(start, hasTimedOut(), hasFailed());
    }
  }
}
