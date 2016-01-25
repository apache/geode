/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.internal.AbstractOp;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.ConnectionStats;
import com.gemstone.gemfire.cache.client.internal.ExecutablePool;

/**
 * Creates a CQ on a server
 * @author darrel
 * @since 5.7
 */
public class CreateCQOp {
  /**
   * Create a continuous query on the server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param cqName name of the CQ to create
   * @param queryStr string OQL statement to be executed
   * @param cqState int cqState to be set.
   * @param isDurable true if CQ is durable
   * @param regionDataPolicy the data policy ordinal of the region
   */
  public static Object execute(ExecutablePool pool, String cqName,
      String queryStr, int cqState, boolean isDurable, byte regionDataPolicy)
  {
    AbstractOp op = new CreateCQOpImpl(cqName, queryStr, cqState,
        isDurable, regionDataPolicy);
    return pool.executeOnQueuesAndReturnPrimaryResult(op);
  }
  
  
  /**
   * Create a continuous query on the server using a specific connections from the given pool.
   * @param pool the pool to use to communicate with the server.
   * @param conn the actual connection to use
   * @param cqName name of the CQ to create
   * @param queryStr string OQL statement to be executed
   * @param cqState int cqState to be set.
   * @param isDurable true if CQ is durable
   * @param regionDataPolicy the data policy ordinal of the region
   */
  public static Object executeOn(ExecutablePool pool, Connection conn,
      String cqName, String queryStr, int cqState, boolean isDurable,
      byte regionDataPolicy)
  {
    AbstractOp op = new CreateCQOpImpl(cqName, queryStr, cqState,
        isDurable, regionDataPolicy);
    return pool.executeOn(conn,op);
  }
  
                                                               
  private CreateCQOp() {
    // no instances allowed
  }

  /**
   * Note both StopCQOpImpl and CloseCQOpImpl extend this class
   */
  protected static class CreateCQOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public CreateCQOpImpl(String cqName, String queryStr, int cqState,
        boolean isDurable, byte regionDataPolicy) {
      super(MessageType.EXECUTECQ_MSG_TYPE, 5);
      getMessage().addStringPart(cqName);
      getMessage().addStringPart(queryStr);
      getMessage().addIntPart(cqState);
      {
        byte durableByte = (byte)(isDurable ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {durableByte});
      }
      getMessage().addBytesPart(new byte[] {regionDataPolicy});
    }
    @Override  
    protected Message createResponseMessage() {
      return new ChunkedMessage(1, Version.CURRENT);
    }
    @Override  
    protected Object processResponse(Message m) throws Exception {
      ChunkedMessage msg = (ChunkedMessage)m;
      msg.readHeader();
      int msgType = msg.getMessageType();
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
          // contained in the message..
          String errorMessage = part.getString();
          if(errorMessage.indexOf("Not authorized") >= 0) {
            throw new NotAuthorizedException(errorMessage);
          }
          
          throw new ServerOperationException(errorMessage);
        } else {
          throw new InternalGemFireError("Unexpected message type "
                                         + MessageType.getString(msgType));
        }
      }
    }
    /**
     * This constructor is for our subclasses
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    protected CreateCQOpImpl(int msgType, int numParts) {
      super(msgType, numParts);
    }
    protected String getOpName() {
      return "createCQ";
    }
    @Override  
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.CQDATAERROR_MSG_TYPE
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
