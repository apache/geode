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
