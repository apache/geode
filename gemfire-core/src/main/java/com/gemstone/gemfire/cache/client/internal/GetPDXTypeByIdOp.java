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
import com.gemstone.gemfire.pdx.internal.PdxType;

/**
 * Retrieve the PDXType, given an integer PDX id, from a server.
 * @author dsmith
 * @since 6.6
 */
public class GetPDXTypeByIdOp {
  /**
   * Get a PdxType from the given pool.
   * @param pool the pool to use to communicate with the server.
   */
  public static PdxType execute(ExecutablePool pool,
                             int pdxId)
  {
    AbstractOp op = new GetPDXTypeByIdOpImpl(pdxId);
    return (PdxType) pool.execute(op);
  }
                                                               
  private GetPDXTypeByIdOp() {
    // no instances allowed
  }
  
  private static class GetPDXTypeByIdOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public GetPDXTypeByIdOpImpl(int pdxId) {
      super(MessageType.GET_PDX_TYPE_BY_ID, 1);
      getMessage().addIntPart(pdxId);
    }
    @Override
    protected Object processResponse(Message msg) throws Exception {
      return processObjResponse(msg, "getPDXTypeById");
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetPDXTypeById();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetPDXTypeByIdSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGetPDXTypeById(start, hasTimedOut(), hasFailed());
    }
    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
    }
    @Override
    protected boolean needsUserId() {
      return false;
    }
    //Don't send the transaction id for this message type.
    @Override
    protected boolean participateInTransaction() {
      return false;
    }
    
    //TODO - no idea what this mumbo jumbo means, but it's on
    //most of the other messages like this.
    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().setEarlyAck((byte)(getMessage().getEarlyAckByte() & Message.MESSAGE_HAS_SECURE_PART));
      getMessage().send(false);
    }
  }
}
