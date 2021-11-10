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

import org.jetbrains.annotations.NotNull;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.pdx.internal.PdxType;

/**
 * Retrieve the PDXType, given an integer PDX id, from a server.
 *
 * @since GemFire 6.6
 */
public class GetPDXIdForTypeOp {
  /**
   * Register a bunch of instantiators on a server using connections from the given pool to
   * communicate with the server.
   *
   * @param pool the pool to use to communicate with the server.
   */
  public static int execute(ExecutablePool pool, PdxType type) {
    AbstractOp op = new GetPDXIdForTypeOpImpl(type);
    return ((Integer) pool.execute(op)).intValue();
  }

  private GetPDXIdForTypeOp() {
    // no instances allowed
  }

  private static class GetPDXIdForTypeOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public GetPDXIdForTypeOpImpl(PdxType type) {
      super(MessageType.GET_PDX_ID_FOR_TYPE, 1);
      getMessage().addObjPart(type);
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      Part part = msg.getPart(0);
      final int msgType = msg.getMessageType();
      if (msgType == MessageType.RESPONSE) {
        return Integer.valueOf(part.getInt());
      } else {
        if (msgType == MessageType.EXCEPTION) {
          String s = "While performing a remote " + "getPdxIdForType";
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

    // Don't send the transaction id for this message type.
    @Override
    protected boolean participateInTransaction() {
      return false;
    }
  }
}
