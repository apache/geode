/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.client.internal;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;

/**
 * Gets (full) value (unlike GetOp, which may get either a full value or a delta depending upon
 * delta flag) of a given event from the ha container on server.
 *
 * @since GemFire 6.1
 */
public class GetEventValueOp {
  /**
   * Does a get on the primary server using connections from the given pool
   *
   * @param pool the pool to use to communicate with the server.
   * @param event the eventid to do the get on
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   * @return the entry value found by the get if any
   */
  public static Object executeOnPrimary(ExecutablePool pool, EventID event, Object callbackArg) {
    AbstractOp op = new GetEventValueOpImpl(event, callbackArg);
    return pool.executeOnPrimary(op);
  }


  private GetEventValueOp() {
    // no instances allowed
  }

  static class GetEventValueOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public GetEventValueOpImpl(EventID event, Object callbackArg) {
      super(MessageType.REQUEST_EVENT_VALUE, callbackArg != null ? 2 : 1);
      getMessage().addObjPart(event);
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
      }
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().clearMessageHasSecurePartFlag();
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
        } else if (msgType == MessageType.EXCEPTION) {
          String s = "While performing a remote " + "getFullValue";
          throw new ServerOperationException(s, (Throwable) part.getObject());
          // Get the exception toString part.
          // This was added for c++ thin client and not used in java
        } else if (isErrorResponse(msgType)) {
          throw new ServerOperationException(part.getString());
        } else {
          throw new InternalGemFireError(
              "Unexpected message type " + MessageType.getString(msgType));
        }
      }
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
  }
}
