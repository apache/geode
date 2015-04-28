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
 * Send the primary server acknowledgement on the events this client
 * has received and processed from it.
 * @author darrel
 * @since 5.7
 */
public class PrimaryAckOp {
  /**
   * Send the primary server acknowledgement on the events this client
   * has received and processed from it
   * using connections from the given pool
   * to communicate with the server.
   * @param connection 
   * @param pool the pool to use to communicate with the server.
   * @param events list of events to acknowledge
   */
  public static void execute(Connection connection, ExecutablePool pool,
                             List events)
  {
    AbstractOp op = new PrimaryAckOpImpl(events);
    pool.executeOn(connection, op);
  }
                                                               
  private PrimaryAckOp() {
    // no instances allowed
  }
  
  private static class PrimaryAckOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public PrimaryAckOpImpl(List events) {
      super(MessageType.PERIODIC_ACK, events.size());
      for (Iterator i = events.iterator(); i.hasNext();) {
        getMessage().addObjPart(i.next());
      }
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
      processAck(msg, "primaryAck");
      return null;
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startPrimaryAck();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endPrimaryAckSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endPrimaryAck(start, hasTimedOut(), hasFailed());
    }
  }
}
