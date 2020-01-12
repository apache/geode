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

import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;

/**
 * Does a region destroyRegion (or create) on a server
 *
 * @since GemFire 5.7
 */
public class DestroyRegionOp {
  /**
   * Does a region destroyRegion on a server using connections from the given pool to communicate
   * with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the destroyRegion on
   * @param eventId the event id for this destroyRegion
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static void execute(ExecutablePool pool, String region, EventID eventId,
      Object callbackArg) {
    AbstractOp op = new DestroyRegionOpImpl(region, eventId, callbackArg);
    pool.execute(op);
  }

  /**
   * Does a region destroyRegion on a server using the given connection to communicate with the
   * server.
   *
   * @param con the connection to use to send to the server
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the destroyRegion on
   * @param eventId the event id for this destroyRegion
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static void execute(Connection con, ExecutablePool pool, String region, EventID eventId,
      Object callbackArg) {
    AbstractOp op = new DestroyRegionOpImpl(region, eventId, callbackArg);
    pool.executeOn(con, op);
  }

  private DestroyRegionOp() {
    // no instances allowed
  }

  private static class DestroyRegionOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public DestroyRegionOpImpl(String region, EventID eventId, Object callbackArg) {
      super(MessageType.DESTROY_REGION, callbackArg != null ? 3 : 2);
      getMessage().addStringPart(region, true);
      getMessage().addBytesPart(eventId.calcBytes());
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "destroyRegion");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.DESTROY_REGION_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startDestroyRegion();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endDestroyRegionSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endDestroyRegion(start, hasTimedOut(), hasFailed());
    }
  }
}
