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
import com.gemstone.gemfire.internal.cache.EventID;

/**
 * Does a region destroyRegion (or create) on a server
 * @author darrel
 * @since 5.7
 */
public class DestroyRegionOp {
  /**
   * Does a region destroyRegion on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the destroyRegion on
   * @param eventId the event id for this destroyRegion
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static void execute(ExecutablePool pool,
                             String region,
                             EventID eventId,
                             Object callbackArg)
  {
    AbstractOp op = new DestroyRegionOpImpl(region, eventId, callbackArg);
    pool.execute(op);
  }
  /**
   * Does a region destroyRegion on a server using the given connection
   * to communicate with the server.
   * @param con the connection to use to send to the server
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the destroyRegion on
   * @param eventId the event id for this destroyRegion
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static void execute(Connection con,
                             ExecutablePool pool,
                             String region,
                             EventID eventId,
                             Object callbackArg)
  {
    AbstractOp op = new DestroyRegionOpImpl(region, eventId, callbackArg);
    pool.executeOn(con, op);
  }
                                                               
  private DestroyRegionOp() {
    // no instances allowed
  }
  
  private static class DestroyRegionOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public DestroyRegionOpImpl(String region,
                     EventID eventId,
                     Object callbackArg) {
      super(MessageType.DESTROY_REGION, callbackArg != null ? 3 : 2);
      getMessage().addStringPart(region);
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
