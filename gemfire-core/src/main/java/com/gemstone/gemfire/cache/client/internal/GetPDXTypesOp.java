/*=========================================================================
 * Copyright (c) 2012 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.Map;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.pdx.internal.PdxType;

/**
 * Retrieve all known PDX types.
 * 
 * @author bakera
 * @since 7.0
 */
public class GetPDXTypesOp {

  public static Map<Integer, PdxType> execute(ExecutablePool pool) {
    AbstractOp op = new GetPDXTypesOpImpl();
    return (Map<Integer, PdxType>) pool.execute(op);
  }
                                                               
  private GetPDXTypesOp() {
    // no instances allowed
  }
  
  private static class GetPDXTypesOpImpl extends AbstractOp {
    public GetPDXTypesOpImpl() {
      super(MessageType.GET_PDX_TYPES, 1);
      getMessage().addIntPart(0); // must have at least one part
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      Part part = msg.getPart(0);
      int msgType = msg.getMessageType();
      if (msgType == MessageType.RESPONSE) {
        return (Map<Integer, PdxType>) part.getObject();

      } else {
        if (msgType == MessageType.EXCEPTION) {
          String s = "While performing a remote " + "getPdxTypes";
          throw new ServerOperationException(s, (Throwable) part.getObject());

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
      return 0;
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
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
    protected boolean participateInTransaction() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().setEarlyAck((byte)(getMessage().getEarlyAckByte() & Message.MESSAGE_HAS_SECURE_PART));
      getMessage().send(false);
    }
  }
}
