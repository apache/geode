/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

public class GetFunctionAttributeOp {

  public static Object execute(ExecutablePool pool, String functionId) {
    AbstractOp op = new GetFunctionAttributeOpImpl(functionId);
    return pool.execute(op);
  }

  private GetFunctionAttributeOp() {
    // no instances allowed
  }

  static class GetFunctionAttributeOpImpl extends AbstractOp {

    private String functionId = null;

    public GetFunctionAttributeOpImpl(String functionId) {
      super(MessageType.GET_FUNCTION_ATTRIBUTES, 1);
      this.functionId = functionId;
      getMessage().addStringPart(this.functionId);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      return processObjResponse(msg, "getFunctionAttribute");
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.REQUESTDATAERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGet();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGet(start, hasTimedOut(), hasFailed());
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
  }
}
