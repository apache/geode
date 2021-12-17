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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * Does a region keySet on a server
 *
 * @since GemFire 5.7
 */
public class KeySetOp {
  /**
   * Does a region entry keySet on a server using connections from the given pool to communicate
   * with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the entry keySet on
   */
  public static Set execute(ExecutablePool pool, String region) {
    AbstractOp op = new KeySetOpImpl(region);
    return (Set) pool.execute(op);
  }

  private KeySetOp() {
    // no instances allowed
  }

  private static class KeySetOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public KeySetOpImpl(String region) {
      super(MessageType.KEY_SET, 1);
      getMessage().addStringPart(region, true);
    }

    @Override
    protected @NotNull Message createResponseMessage() {
      return new ChunkedMessage(1, KnownVersion.CURRENT);
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {

      ChunkedMessage keySetResponseMessage = (ChunkedMessage) msg;
      final HashSet result = new HashSet();
      final Exception[] exceptionRef = new Exception[1];

      keySetResponseMessage.readHeader();
      final int msgType = keySetResponseMessage.getMessageType();
      if (msgType == MessageType.RESPONSE) {
        do {
          keySetResponseMessage.receiveChunk();
          Part part = keySetResponseMessage.getPart(0);
          Object o = part.getObject();
          if (o instanceof Throwable) {
            String s = "While performing a remote keySet";
            exceptionRef[0] = new ServerOperationException(s, (Throwable) o);
          } else {
            result.addAll((List) o);
          }
        } while (!keySetResponseMessage.isLastChunk());
      } else {
        if (msgType == MessageType.EXCEPTION) {
          keySetResponseMessage.receiveChunk();
          Part part = msg.getPart(0);
          String s = "While performing a remote " + "keySet";
          throw new ServerOperationException(s, (Throwable) part.getObject());
          // Get the exception toString part.
          // This was added for c++ thin client and not used in java
        } else if (isErrorResponse(msgType)) {
          keySetResponseMessage.receiveChunk();
          Part part = msg.getPart(0);
          throw new ServerOperationException(part.getString());
        } else {
          throw new InternalGemFireError(
              "Unexpected message type " + MessageType.getString(msgType));
        }
      }

      if (exceptionRef[0] != null) {
        throw exceptionRef[0];
      } else {
        return result;
      }
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.KEY_SET_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startKeySet();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endKeySetSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endKeySet(start, hasTimedOut(), hasFailed());
    }
  }
}
