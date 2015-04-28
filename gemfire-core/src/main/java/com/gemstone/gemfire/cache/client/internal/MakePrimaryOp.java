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
 * Tell a server to become the primary host of a server-to-client queue
 * @author darrel
 * @since 5.7
 */
public class MakePrimaryOp {
  /**
   * Tell the given server to become the primary host of a server-to-client queue
   * @param pool the pool to use to communicate with the server.
   * @param conn the connection to do the execution on
   * @param sentClientReady true if the client ready message has already been sent
   */
  public static void execute(ExecutablePool pool, Connection conn, boolean sentClientReady)
  {
    AbstractOp op = new MakePrimaryOpImpl(sentClientReady);
    pool.executeOn(conn, op);
  }
                                                               
  private MakePrimaryOp() {
    // no instances allowed
  }
  
  private static class MakePrimaryOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public MakePrimaryOpImpl(boolean sentClientReady) {
      super(MessageType.MAKE_PRIMARY, 1);
      getMessage().addBytesPart(new byte[] {(byte)(sentClientReady?0x01:0x00)});
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
      processAck(msg, "makePrimary");
      return null;
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startMakePrimary();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endMakePrimarySend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endMakePrimary(start, hasTimedOut(), hasFailed());
    }
  }
}
