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
import com.gemstone.gemfire.cache.client.internal.QueryOp;
import com.gemstone.gemfire.cache.client.internal.QueryOp.QueryOpImpl;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.cache.tier.MessageType;

/**
 * Creates a CQ and fetches initial results on a server
 * @author darrel
 * @since 5.7
 */
public class CreateCQWithIROp {
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
  public static SelectResults execute(ExecutablePool pool, String cqName,
      String queryStr, int cqState, boolean isDurable, byte regionDataPolicy)
  {
    AbstractOp op = new CreateCQWithIROpImpl(cqName,
        queryStr, cqState, isDurable, regionDataPolicy);
    return (SelectResults)pool.executeOnQueuesAndReturnPrimaryResult(op);
  }
                                                               
  private CreateCQWithIROp() {
    // no instances allowed
  }

  /**
   * Note we extend QueryOpImpl to inherit processResponse and isErrorResponse
   */
  private static class CreateCQWithIROpImpl extends QueryOpImpl {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public CreateCQWithIROpImpl(String cqName, String queryStr,
        int cqState, boolean isDurable, byte regionDataPolicy) {
      super(MessageType.EXECUTECQ_WITH_IR_MSG_TYPE, 5);
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
    protected String getOpName() {
      return "createCQfetchInitialResult";
    }
    // using same stats as CreateCQOp
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
