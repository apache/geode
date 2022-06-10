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
package org.apache.geode.cache.query.cq.internal.ops;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.AbstractOp;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.ConnectionStats;
import org.apache.geode.cache.client.internal.ExecutablePool;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.security.NotAuthorizedException;

/**
 * Creates a CQ on a server
 *
 * @since GemFire 5.7
 */
public class CreateCQOp {
  /**
   * Create a continuous query on the server using connections from the given pool to communicate
   * with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param cqName name of the CQ to create
   * @param queryStr string OQL statement to be executed
   * @param cqState int cqState to be set.
   * @param isDurable true if CQ is durable
   * @param regionDataPolicy the data policy ordinal of the region
   */
  public static Object execute(final @NotNull ExecutablePool pool, final @NotNull String cqName,
      final @NotNull String queryStr, final int cqState,
      final boolean isDurable, final @NotNull DataPolicy regionDataPolicy) {
    AbstractOp op = new CreateCQOpImpl(cqName, queryStr, cqState, isDurable, regionDataPolicy);
    return pool.executeOnQueuesAndReturnPrimaryResult(op);
  }


  /**
   * Create a continuous query on the server using a specific connections from the given pool.
   *
   * @param pool the pool to use to communicate with the server.
   * @param conn the actual connection to use
   * @param cqName name of the CQ to create
   * @param queryStr string OQL statement to be executed
   * @param cqState int cqState to be set.
   * @param isDurable true if CQ is durable
   * @param regionDataPolicy the data policy ordinal of the region
   */
  public static Object executeOn(final @NotNull ExecutablePool pool, final @NotNull Connection conn,
      final @NotNull String cqName,
      final @NotNull String queryStr, final int cqState, final boolean isDurable,
      final @NotNull DataPolicy regionDataPolicy) {
    AbstractOp op = new CreateCQOpImpl(cqName, queryStr, cqState, isDurable, regionDataPolicy);
    return pool.executeOn(conn, op);
  }


  private CreateCQOp() {
    // no instances allowed
  }

  /**
   * Note both StopCQOpImpl and CloseCQOpImpl extend this class
   */
  protected static class CreateCQOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public CreateCQOpImpl(final @NotNull String cqName, final @NotNull String queryStr,
        final int cqState, final boolean isDurable,
        final @NotNull DataPolicy regionDataPolicy) {
      super(MessageType.EXECUTECQ, 5);
      getMessage().addStringPart(cqName);
      getMessage().addStringPart(queryStr);
      getMessage().addIntPart(cqState);
      {
        byte durableByte = (byte) (isDurable ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {durableByte});
      }
      getMessage().addBytesPart(new byte[] {(byte) regionDataPolicy.ordinal()});
    }

    @Override
    protected @NotNull Message createResponseMessage() {
      return new ChunkedMessage(1, KnownVersion.CURRENT);
    }

    @Override
    protected Object processResponse(final @NotNull Message m) throws Exception {
      ChunkedMessage msg = (ChunkedMessage) m;
      msg.readHeader();
      MessageType msgType = msg.getMessageType();
      msg.receiveChunk();
      if (msgType == MessageType.REPLY) {
        return Boolean.TRUE;
      } else {
        if (msgType == MessageType.EXCEPTION) {
          Part part = msg.getPart(0);
          String s = "While performing a remote " + getOpName();
          throw new ServerOperationException(s, (Throwable) part.getObject());
        } else if (isErrorResponse(msgType)) {
          Part part = msg.getPart(0);
          // Dan Smith- a hack, but I don't want to change the protocol right
          // now. We need to throw a security exception so that the exception
          // will be propagated up properly. Ideally, this exception would be
          // contained in the message.
          String errorMessage = part.getString();
          if (errorMessage.contains("Not authorized")) {
            throw new NotAuthorizedException(errorMessage);
          }

          throw new ServerOperationException(errorMessage);
        } else {
          throw new InternalGemFireError(
              "Unexpected message type " + msgType);
        }
      }
    }

    /**
     * This constructor is for our subclasses
     *
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    protected CreateCQOpImpl(MessageType msgType, int numParts) {
      super(msgType, numParts);
    }

    protected String getOpName() {
      return "createCQ";
    }

    @Override
    protected boolean isErrorResponse(MessageType msgType) {
      return msgType == MessageType.CQDATAERROR
          || msgType == MessageType.CQ_EXCEPTION_TYPE;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startCreateCQ();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endCreateCQSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endCreateCQ(start, hasTimedOut(), hasFailed());
    }
  }
}
