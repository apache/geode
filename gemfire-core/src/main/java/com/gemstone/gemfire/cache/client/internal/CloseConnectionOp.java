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
 * Tell a server that a connection is being closed
 * @author darrel
 * @since 5.7
 */
public class CloseConnectionOp {
  /**
   * Tell a server that a connection is being closed
   * @param con the connection that is being closed
   * @param keepAlive whether to keep the proxy alive on the server
   */
  public static void execute(Connection con, boolean keepAlive)
    throws Exception
  {
    AbstractOp op = new CloseConnectionOpImpl(keepAlive);
    con.execute(op);
  }
                                                               
  private CloseConnectionOp() {
    // no instances allowed
  }
  
  private static class CloseConnectionOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public CloseConnectionOpImpl(boolean keepAlive)  {
      super(MessageType.CLOSE_CONNECTION, 1);
      getMessage().addRawPart(new byte[]{(byte)(keepAlive?1:0)}, false);
    }
    @Override  
    protected Message createResponseMessage() {
      // no response is sent
      return null;
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
      throw new IllegalStateException("should never be called");
    }
    @Override  
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override  
    protected long startAttempt(ConnectionStats stats) {
      return stats.startCloseCon();
    }
    @Override  
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endCloseConSend(start, hasFailed());
    }
    @Override  
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endCloseCon(start, hasTimedOut(), hasFailed());
    }
  }
}
