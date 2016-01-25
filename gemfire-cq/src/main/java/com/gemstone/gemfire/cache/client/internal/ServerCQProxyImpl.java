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

import java.util.List;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.internal.CqStateImpl;
import com.gemstone.gemfire.cache.query.internal.cq.ClientCQ;

/**
 * Used to send CQ operations from a client to a server
 * @author darrel
 * @since 5.7
 */
public class ServerCQProxyImpl extends ServerProxy {
  /**
   * Creates a server CQ proxy for the given pool name.
   * @param pool the pool that this proxy will use to communicate with servers
   */
  public ServerCQProxyImpl(InternalPool pool) {
    super(pool);
  }
  /**
   * Creates a server CQ proxy given using the same pool as that of
   * the given server proxy.
   * @param sp server proxy whose pool we are to use
   */
  public ServerCQProxyImpl(ServerProxy sp) {
    this(sp.pool);
  }

  /**
   * Create a continuous query on the given pool
   * @param cq the CQ to create on the server
   */
  public Object create(ClientCQ cq)
  {
    pool.getRITracker().addCq(cq, cq.isDurable());
    byte regionDataPolicyOrdinal = cq.getCqBaseRegion()==null ? (byte) 0 : cq.getCqBaseRegion()
        .getAttributes().getDataPolicy().ordinal;
    return CreateCQOp.execute(this.pool, cq.getName(), cq.getQueryString(),
        CqStateImpl.RUNNING, cq.isDurable(), regionDataPolicyOrdinal);
  }
  
  /**
   * Create a continuous query on the given server
   * @param conn the connection to use
   * @param cqName name of the CQ to create
   * @param queryStr string OQL statement to be executed
   * @param cqState int cqState to be set.
   * @param isDurable true if CQ is durable
   * @param regionDataPolicy the data policy ordinal of the region
   */
  public Object createOn(String cqName, Connection conn, String queryStr,
      int cqState, boolean isDurable, byte regionDataPolicy)
  {
    
    return CreateCQOp.executeOn(this.pool, conn, cqName, queryStr, cqState,
        isDurable, regionDataPolicy);
  }
  
  /**
   * Create a continuous query on the given server and return the initial query results.
   * @param cq the CQ to create on the server
   */
  public SelectResults createWithIR(ClientCQ cq)
  {
    pool.getRITracker().addCq(cq, cq.isDurable());
    byte regionDataPolicyOrdinal = cq.getCqBaseRegion()==null ? (byte) 0 : cq.getCqBaseRegion()
        .getAttributes().getDataPolicy().ordinal;
    return CreateCQWithIROp.execute(this.pool, cq.getName(), cq
        .getQueryString(), CqStateImpl.RUNNING, cq.isDurable(),regionDataPolicyOrdinal);
  }

  /**
   * Does a CQ stop on all relevant servers
   * @param cq the CQ to stop on the server
   */
  public void stop(ClientCQ cq) {
    pool.getRITracker().removeCq(cq, cq.isDurable());
    StopCQOp.execute(this.pool, cq.getName());
  }

  /**
   * Does a CQ close on all relevant servers
   * @param cq the CQ to close on the server
   */
  public void close(ClientCQ cq) {
    pool.getRITracker().removeCq(cq, cq.isDurable());
    CloseCQOp.execute(this.pool, cq.getName());
  }
  
  public List<String> getAllDurableCqsFromServer() {
    return GetDurableCQsOp.execute((ExecutablePool)pool);
  }
}
