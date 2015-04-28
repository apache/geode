/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.pdx.internal.EnumInfo;

/**
 * Retrieve the PDXType, given an integer PDX id, from a server.
 * @author darrel
 * @since 6.6.2
 */
public class GetPDXEnumByIdOp {
  /**
   * Get a enum from the given pool.
   * @param pool the pool to use to communicate with the server.
   */
  public static EnumInfo execute(ExecutablePool pool,
                             int enumId)
  {
    AbstractOp op = new GetPDXEnumByIdOpImpl(enumId);
    return (EnumInfo) pool.execute(op);
  }
                                                               
  private GetPDXEnumByIdOp() {
    // no instances allowed
  }
  
  private static class GetPDXEnumByIdOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public GetPDXEnumByIdOpImpl(int enumId) {
      super(MessageType.GET_PDX_ENUM_BY_ID, 1);
      getMessage().addIntPart(enumId);
    }
    @Override
    protected Object processResponse(Message msg) throws Exception {
      return processObjResponse(msg, "getPDXEnumById");
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetPDXTypeById(); // reuse PDXType stats instead of adding new enum ones
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
