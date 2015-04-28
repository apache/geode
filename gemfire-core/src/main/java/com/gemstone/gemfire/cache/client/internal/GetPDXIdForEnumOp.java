/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.pdx.internal.EnumInfo;

/**
 * Retrieve the PDXType, given an integer PDX id, from a server.
 * @author darrel
 * @since 6.6.2
 */
public class GetPDXIdForEnumOp {
  /**
   * Register a bunch of instantiators on a server
   * using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   */
  public static int execute(ExecutablePool pool,
                             EnumInfo ei)
  {
    AbstractOp op = new GetPDXIdForEnumOpImpl(ei);
    return ((Integer) pool.execute(op)).intValue();
  }
                                                               
  private GetPDXIdForEnumOp() {
    // no instances allowed
  }
  
  private static class GetPDXIdForEnumOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public GetPDXIdForEnumOpImpl(EnumInfo ei) {
      super(MessageType.GET_PDX_ID_FOR_ENUM, 1);
      getMessage().addObjPart(ei);
    }
    @Override
    protected Object processResponse(Message msg) throws Exception {
      Part part = msg.getPart(0);
      final int msgType = msg.getMessageType();
      if (msgType == MessageType.RESPONSE) {
        return Integer.valueOf(part.getInt());
      } else {
        if (msgType == MessageType.EXCEPTION) {
          String s = "While performing a remote " + "getPdxIdForEnum";
          throw new ServerOperationException(s, (Throwable) part.getObject());
          // Get the exception toString part.
          // This was added for c++ thin client and not used in java
          // Part exceptionToStringPart = msg.getPart(1);
        } else if (isErrorResponse(msgType)) {
          throw new ServerOperationException(part.getString());
        } else {
          throw new InternalGemFireError("Unexpected message type "
                                         + MessageType.getString(msgType));
        }
      }
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
      stats.endGetPDXTypeByIdSend(start, hasFailed()); /* reusing type stats instead of adding enum ones */
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
