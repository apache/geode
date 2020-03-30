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

package org.apache.geode.cache.client.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.pooling.ConnectionManager;
import org.apache.geode.distributed.internal.ServerLocation;

public class TXFailoverOpTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ConnectionManager manager;
  private EndpointManager endpointManager;
  private QueueManager queueManager;
  private RegisterInterestTracker riTracker;
  private CancelCriterion cancelCriterion;
  private PoolImpl mockPool;

  @Before
  public void setUp() {
    endpointManager = mock(EndpointManager.class);
    queueManager = mock(QueueManager.class);
    manager = mock(ConnectionManager.class);
    riTracker = mock(RegisterInterestTracker.class);
    cancelCriterion = mock(CancelCriterion.class);
    mockPool = mock(PoolImpl.class);
    when(mockPool.execute(any())).thenThrow(new TransactionException()).thenReturn(true);
  }

  private OpExecutorImpl getTestableOpExecutorImpl() {
    return new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, 3, 10,
        PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT,
        cancelCriterion, mockPool) {

      @Override
      public ServerLocation getNextOpServerLocation() {
        return mock(ServerLocation.class);
      }

      @Override
      protected Object executeOnServer(ServerLocation p_server, Op op, boolean accessed,
          boolean onlyUseExistingCnx) {
        throw new ServerConnectivityException();
      }
    };
  }

  @Test
  public void txFailoverThrowsTransactionExceptionBack() throws Exception {
    OpExecutorImpl exec = getTestableOpExecutorImpl();
    exec.setupServerAffinity(Boolean.TRUE);
    expectedException.expect(TransactionException.class);
    TXFailoverOp.execute(exec, 1);
  }

}
