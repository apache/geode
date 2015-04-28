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

/**
 * Tells the server we are ready to receive server-to-client events
 * from durable subscriptions.
 * @author darrel
 * @since 5.7
 */
public class ReadyForEventsOp {
  /**
   * Tells the primary server we are ready to receive server-to-client events
   * from durable subscriptions.
   * @param pool the pool to use to communicate with the server.
   * @param primary 
   */
  public static void execute(ExecutablePool pool, QueueConnectionImpl primary)
  {
    AbstractOp op = new ReadyForEventsOpImpl();
    pool.executeOn(primary, op);
  }
                                                               
  private ReadyForEventsOp() {
    // no instances allowed
  }
  
  private static class ReadyForEventsOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public ReadyForEventsOpImpl() {
      super(MessageType.CLIENT_READY, 1);
    }

    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().setEarlyAck((byte)(getMessage().getEarlyAckByte() & Message.MESSAGE_HAS_SECURE_PART));
      getMessage().send(false);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "readyForEvents");
      return null;
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startReadyForEvents();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endReadyForEventsSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endReadyForEvents(start, hasTimedOut(), hasFailed());
    }
  }
}
