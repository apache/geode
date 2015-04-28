/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * Does a region containsKey on a server
 * @author darrel
 * @since 5.7
 */
public class ContainsKeyOp {
  /**
   * Does a region entry containsKey on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the entry containsKey on
   * @param key the entry key to do the containsKey on
   * @return the result of invoking containsKey on the server
   */
  public static boolean execute(ExecutablePool pool,
                                String region,
                                Object key,
                                MODE mode)
  {
    AbstractOp op = new ContainsKeyOpImpl(region, key, mode);
    Boolean result = (Boolean)pool.execute(op);
    return result.booleanValue();
  }
                                                               
  private ContainsKeyOp() {
    // no instances allowed
  }
  
  private static class ContainsKeyOpImpl extends AbstractOp {
    
    private String region;
    private Object key;
    private final MODE mode;
    
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public ContainsKeyOpImpl(String region,
                         Object key,
                         MODE mode) {
      super(MessageType.CONTAINS_KEY, 3);
      getMessage().addStringPart(region);
      getMessage().addStringOrObjPart(key);
      getMessage().addIntPart(mode.ordinal());
      this.region = region;
      this.key = key;
      this.mode = mode;
    }
    @Override
    protected Object processResponse(Message msg) throws Exception {
      return processObjResponse(msg, "containsKey");
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.CONTAINS_KEY_DATA_ERROR;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startContainsKey();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endContainsKeySend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endContainsKey(start, hasTimedOut(), hasFailed());
    }
    
    @Override
    public String toString() {
      return "ContainsKeyOp(region=" + region + ";key=" + key+";mode="+mode;
    }
  }
  
  public enum MODE {
    KEY,
    VALUE_FOR_KEY,
    VALUE;
  }
}
