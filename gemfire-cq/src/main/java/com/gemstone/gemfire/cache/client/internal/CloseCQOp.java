/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.cache.client.internal.AbstractOp;
import com.gemstone.gemfire.cache.client.internal.ConnectionStats;
import com.gemstone.gemfire.cache.client.internal.ExecutablePool;
import com.gemstone.gemfire.cache.client.internal.CreateCQOp.CreateCQOpImpl;
import com.gemstone.gemfire.internal.cache.tier.MessageType;

/**
 * Close a continuous query on the server
 * @author darrel
 * @since 5.7
 */
public class CloseCQOp {
  /**
   * Close a continuous query on the given server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param cqName name of the CQ to close
   */
  public static void execute(ExecutablePool pool, String cqName)
  {
    AbstractOp op = new CloseCQOpImpl(cqName);
    pool.executeOnAllQueueServers(op);
  }
                                                               
  private CloseCQOp() {
    // no instances allowed
  }
  
  private static class CloseCQOpImpl extends CreateCQOpImpl {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public CloseCQOpImpl(String cqName) {
      super(MessageType.CLOSECQ_MSG_TYPE, 1);
      getMessage().addStringPart(cqName);
    }
    @Override
    protected String getOpName() {
      return "closeCQ";
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startCloseCQ();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endCloseCQSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endCloseCQ(start, hasTimedOut(), hasFailed());
    }
  }
}
