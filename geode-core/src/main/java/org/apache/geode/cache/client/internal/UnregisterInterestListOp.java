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

import org.jetbrains.annotations.NotNull;

import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;

/**
 * Does a region unregisterInterestList on a server
 *
 * @since GemFire 5.7
 */
public class UnregisterInterestListOp {
  /**
   * Does a region unregisterInterestList on a server using connections from the given pool to
   * communicate with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the unregisterInterestList on
   * @param keys list of keys we are interested in
   * @param isClosing true if this unregister is done by a close
   * @param keepAlive true if this unregister should not undo a durable registration
   */
  public static void execute(ExecutablePool pool, String region, List keys, boolean isClosing,
      boolean keepAlive) {
    AbstractOp op = new UnregisterInterestListOpImpl(region, keys, isClosing, keepAlive);
    pool.executeOnAllQueueServers(op);
  }

  private UnregisterInterestListOp() {
    // no instances allowed
  }

  private static class UnregisterInterestListOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public UnregisterInterestListOpImpl(String region, List keys, boolean isClosing,
        boolean keepAlive) {
      super(MessageType.UNREGISTER_INTEREST_LIST, 4 + keys.size());
      getMessage().addStringPart(region, true);
      {
        byte closingByte = (byte) (isClosing ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {closingByte});
      }
      {
        byte keepAliveByte = (byte) (keepAlive ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {keepAliveByte});
      }
      getMessage().addIntPart(keys.size());
      for (Iterator i = keys.iterator(); i.hasNext();) {
        getMessage().addStringOrObjPart(i.next());
      }
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      processAck(msg, "unregisterInterestList");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.UNREGISTER_INTEREST_DATA_ERROR;
    }

    // using UnregisterInterest stats
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startUnregisterInterest();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endUnregisterInterestSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endUnregisterInterest(start, hasTimedOut(), hasFailed());
    }
  }
}
