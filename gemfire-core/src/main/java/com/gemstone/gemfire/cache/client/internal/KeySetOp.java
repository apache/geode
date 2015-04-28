/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.cache.client.ServerOperationException;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Does a region keySet on a server
 * @author darrel
 * @since 5.7
 */
public class KeySetOp {
  /**
   * Does a region entry keySet on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the entry keySet on
   */
  public static Set execute(ExecutablePool pool,
                            String region)
  {
    AbstractOp op = new KeySetOpImpl(region);
    return (Set)pool.execute(op);
  }
                                                               
  private KeySetOp() {
    // no instances allowed
  }
  
  private static class KeySetOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public KeySetOpImpl(String region) {
      super(MessageType.KEY_SET, 1);
      getMessage().addStringPart(region);
    }
    @Override  
    protected Message createResponseMessage() {
      return new ChunkedMessage(1, Version.CURRENT);
    }
    @Override  
    protected Object processResponse(Message msg) throws Exception {
      
      ChunkedMessage keySetResponseMessage = (ChunkedMessage)msg;
      final HashSet result = new HashSet();
      final Exception[] exceptionRef = new Exception[1];
      
      keySetResponseMessage.readHeader();
      final int msgType = keySetResponseMessage.getMessageType();
      if (msgType == MessageType.RESPONSE) {
        do {
          keySetResponseMessage.receiveChunk();
          //callback.handle(msg);
          Part part = keySetResponseMessage.getPart(0);
          Object o = part.getObject();
          if (o instanceof Throwable) {
            String s = "While performing a remote keySet";
            exceptionRef[0] = new ServerOperationException(s, (Throwable)o);
          } else {
            result.addAll((List)o);
          }
        } while (!keySetResponseMessage.isLastChunk());
      } else {
        if (msgType == MessageType.EXCEPTION) {
          keySetResponseMessage.receiveChunk();
          Part part = msg.getPart(0);
          String s = "While performing a remote " +  "keySet";
          throw new ServerOperationException(s, (Throwable) part.getObject());
          // Get the exception toString part.
          // This was added for c++ thin client and not used in java
          // Part exceptionToStringPart = msg.getPart(1);
        } else if (isErrorResponse(msgType)) {
          keySetResponseMessage.receiveChunk();
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
