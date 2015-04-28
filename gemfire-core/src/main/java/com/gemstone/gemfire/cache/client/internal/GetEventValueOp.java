/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;

/**
 * Gets (full) value (unlike GetOp, which may get either a full value or a delta
 * depending upon delta flag) of a given event from the ha container on server.
 * 
 * @since 6.1
 */
public class GetEventValueOp {
  /**
   * Does a get on the primary server using connections from the given pool
   * @param pool the pool to use to communicate with the server.
   * @param event the eventid to do the get on
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   * @return the entry value found by the get if any
   */
  public static Object executeOnPrimary(ExecutablePool pool, EventID event,
      Object callbackArg) {
    AbstractOp op = new GetEventValueOpImpl(event, callbackArg);
    return pool.executeOnPrimary(op);
  }


  private GetEventValueOp() {
    // no instances allowed
  }

  static class GetEventValueOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public GetEventValueOpImpl(EventID event, Object callbackArg) {
      super(MessageType.REQUEST_EVENT_VALUE, callbackArg != null ? 2 : 1);
      getMessage().addObjPart(event);
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
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
      Part part = msg.getPart(0);
      final int msgType = msg.getMessageType();
      if (msgType == MessageType.RESPONSE) {
        return part;
      } else {
        if (msgType == MessageType.REQUEST_EVENT_VALUE_ERROR) {
          // Value not found in haContainer.
          return null;
        }
        else if (msgType == MessageType.EXCEPTION) {
          String s = "While performing a remote " + "getFullValue";
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

    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.REQUESTDATAERROR;
    }

    protected long startAttempt(ConnectionStats stats) {
      return stats.startGet();
    }

    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetSend(start, hasFailed());
    }

    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGet(start, hasTimedOut(), hasFailed());
    }
  }
}
