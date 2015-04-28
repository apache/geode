/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * Ping a server to see if it is still alive.
 * @author darrel
 * @since 5.7
 */
public class PingOp {
  /**
   * Ping the specified server to see if it is still alive
   * @param pool the pool to use to communicate with the server.
   * @param server the server to do the execution on
   */
  public static void execute(ExecutablePool pool, ServerLocation server)
  {
    AbstractOp op = new PingOpImpl();
    pool.executeOn(server, op, false,false);
  }
                                                               
  private PingOp() {
    // no instances allowed
  }

  static class PingOpImpl extends AbstractOp {
    
    private long startTime;
    
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public PingOpImpl() {
      super(MessageType.PING, 0);
    }

    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
      Message.messageType.set(null);
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().setEarlyAck((byte)(getMessage().getEarlyAckByte() & Message.MESSAGE_HAS_SECURE_PART));
      startTime = System.currentTimeMillis();
      getMessage().send(false);
      Message.messageType.set(MessageType.PING);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "ping");
      final int msgType = msg.getMessageType();
      if (msgType == MessageType.REPLY  &&  msg.getNumberOfParts() > 1) {
        long endTime = System.currentTimeMillis();
        long serverTime = msg.getPart(1).getLong();
        // the new clock offset is computed assuming that the server's timestamp was
        // taken mid-way between when the ping was sent and the reply was
        // received:
        //    timestampElapsedTime = (endTime - startTime)/2
        //    localTime = startTime + timestampElapsedTime
        //    offsetFromServer = serverTime - localTime
        long newCacheTimeOffset = serverTime - startTime/2 - endTime/2;
        InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
        if (ds != null && ds.isLoner()) { // check for loner so we don't jump time offsets across WAN connections
          ds.getClock().setCacheTimeOffset(null, newCacheTimeOffset, false);
        }
      }
      return null;
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startPing();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endPingSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endPing(start, hasTimedOut(), hasFailed());
    }
  }
}
