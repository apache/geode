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
