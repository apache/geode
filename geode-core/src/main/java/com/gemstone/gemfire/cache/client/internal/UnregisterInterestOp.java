/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.InterestType;

/**
 * Does a region unregisterInterest on a server
 * @since 5.7
 */
public class UnregisterInterestOp {
  /**
   * Does a region unregisterInterest on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the unregisterInterest on
   * @param key describes what we are no longer interested in
   * @param interestType the {@link InterestType} for this unregister
   * @param isClosing true if this unregister is done by a close
   * @param keepAlive true if this unregister should not undo a durable registration
   */
  public static void execute(ExecutablePool pool,
                             String region,
                             Object key,
                             int interestType,
                             boolean isClosing,
                             boolean keepAlive)
  {
    AbstractOp op = new UnregisterInterestOpImpl(region, key, interestType, isClosing, keepAlive);
    pool.executeOnAllQueueServers(op);
  }
                                                               
  private UnregisterInterestOp() {
    // no instances allowed
  }
  
  private static class UnregisterInterestOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public UnregisterInterestOpImpl(String region,
                                    Object key,
                                    int interestType,
                                    boolean isClosing,
                                    boolean keepAlive) {
      super(MessageType.UNREGISTER_INTEREST, 5);
      getMessage().addStringPart(region);
      getMessage().addIntPart(interestType);
      getMessage().addStringOrObjPart(key);
      {
        byte closingByte = (byte)(isClosing ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {closingByte});
      }
      {
        byte keepAliveByte = (byte)(keepAlive ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {keepAliveByte});
      }
    }
    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "unregisterInterest");
      return null;
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.UNREGISTER_INTEREST_DATA_ERROR;
    }
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
