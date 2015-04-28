/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

import java.util.Iterator;
import java.util.List;

/**
 * Does a region unregisterInterestList on a server
 * @author darrel
 * @since 5.7
 */
public class UnregisterInterestListOp {
  /**
   * Does a region unregisterInterestList on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the unregisterInterestList on
   * @param keys list of keys we are interested in
   * @param isClosing true if this unregister is done by a close
   * @param keepAlive true if this unregister should not undo a durable registration
   */
  public static void execute(ExecutablePool pool,
                             String region,
                             List keys,
                             boolean isClosing,
                             boolean keepAlive)
  {
    AbstractOp op = new UnregisterInterestListOpImpl(region, keys, isClosing, keepAlive);
    pool.executeOnAllQueueServers(op);
  }
                                                               
  private UnregisterInterestListOp() {
    // no instances allowed
  }
  
  private static class UnregisterInterestListOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public UnregisterInterestListOpImpl(String region,
                                        List keys,
                                        boolean isClosing,
                                        boolean keepAlive) {
      super(MessageType.UNREGISTER_INTEREST_LIST, 4+keys.size());
      getMessage().addStringPart(region);
      {
        byte closingByte = (byte)(isClosing ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {closingByte});
      }
      {
        byte keepAliveByte = (byte)(keepAlive ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {keepAliveByte});
      }
      getMessage().addIntPart(keys.size());
      for (Iterator i = keys.iterator(); i.hasNext();) {
        getMessage().addStringOrObjPart(i.next());
      }
    }
    @Override
    protected Object processResponse(Message msg) throws Exception {
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
