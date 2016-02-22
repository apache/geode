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

import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.internal.AbstractOp;
import com.gemstone.gemfire.cache.client.internal.ConnectionStats;
import com.gemstone.gemfire.cache.client.internal.ExecutablePool;
import com.gemstone.gemfire.cache.client.internal.CreateCQOp.CreateCQOpImpl;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;

/**
 * Retrieves all durable cqs for a client from a server
 * @author jhuynh
 * @since 7.0
 */
public class GetDurableCQsOp {
  /**
   * Retrieves all durable continuous queries on the server using 
   * connections from the given pool to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   */
  public static List<String> execute(ExecutablePool pool)
  {
    AbstractOp op = new GetDurableCQsOpImpl();
    return (List<String>)pool.execute(op);
  }
                                                               
  private GetDurableCQsOp() {
    // no instances allowed
  }
  
  private static class GetDurableCQsOpImpl extends CreateCQOpImpl {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public GetDurableCQsOpImpl() {
      super(MessageType.GETDURABLECQS_MSG_TYPE, 1 /*numparts*/);
    }
    @Override
    protected String getOpName() {
      return "getDurableCQs";
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetDurableCQs();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetDurableCQsSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGetDurableCQs(start, hasTimedOut(), hasFailed());
    }
    
    @Override  
    protected Message createResponseMessage() {
      return new ChunkedMessage(1, Version.CURRENT);
    }
    @Override  
    protected Object processResponse(Message msg) throws Exception {
      
      ChunkedMessage getDurableCQsResponseMsg = (ChunkedMessage)msg;
      final List<String> result = new LinkedList<String>();
      final Exception[] exceptionRef = new Exception[1];
      
      getDurableCQsResponseMsg.readHeader();
      final int msgType = getDurableCQsResponseMsg.getMessageType();
      if (msgType == MessageType.RESPONSE) {
        do {
          getDurableCQsResponseMsg.receiveChunk();
          //callback.handle(msg);
          Part part = getDurableCQsResponseMsg.getPart(0);
          Object o = part.getObject();
          if (o instanceof Throwable) {
            String s = "While performing a remote GetDurableCQs";
            exceptionRef[0] = new ServerOperationException(s, (Throwable)o);
          } else {
            result.addAll((List)o);
          }
        } while (!getDurableCQsResponseMsg.isLastChunk());
      } else {
        if (msgType == MessageType.EXCEPTION) {
          getDurableCQsResponseMsg.receiveChunk();
          Part part = msg.getPart(0);
          String s = "While performing a remote GetDurableCQs";
          throw new ServerOperationException(s, (Throwable) part.getObject());
          // Get the exception toString part.
          // This was added for c++ thin client and not used in java
          // Part exceptionToStringPart = msg.getPart(1);
        } else if (isErrorResponse(msgType)) {
          getDurableCQsResponseMsg.receiveChunk();
          Part part = msg.getPart(0);
          throw new ServerOperationException(part.getString());
        } else {
          throw new InternalGemFireError("Unexpected message type "
                                         + MessageType.getString(msgType));
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
      return msgType == MessageType.GET_DURABLE_CQS_DATA_ERROR;
    }
  }
}
