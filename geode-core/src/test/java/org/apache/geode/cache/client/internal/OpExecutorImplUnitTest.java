/*
 *
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
 *
 */

package org.apache.geode.cache.client.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.client.internal.pooling.ConnectionManager;
import org.apache.geode.distributed.internal.ServerLocation;

public class OpExecutorImplUnitTest {
  private OpExecutorImpl executor;
  private ConnectionManager connectionManager;
  private QueueManager queueManager;
  private EndpointManager endpointManager;
  private RegisterInterestTracker tracker;
  private CancelCriterion cancelCriterion;
  private PoolImpl pool;
  private ServerLocation server;
  private Connection connection;
  private AbstractOp op;

  @Before
  public void before() throws Exception {
    connectionManager = mock(ConnectionManager.class);
    queueManager = mock(QueueManager.class);
    endpointManager = mock(EndpointManager.class);
    tracker = mock(RegisterInterestTracker.class);
    cancelCriterion = mock(CancelCriterion.class);
    server = mock(ServerLocation.class);
    connection = mock(Connection.class);
    pool = mock(PoolImpl.class);
    op = mock(AbstractOp.class);
    when(connection.getServer()).thenReturn(server);

    executor = new OpExecutorImpl(connectionManager, queueManager, endpointManager, tracker, 1, 5L,
        5L, cancelCriterion, pool);
  }

  @Test
  public void authenticateIfRequired_noOp_WhenNotRequireCredential() {
    when(server.getRequiresCredentials()).thenReturn(false);
    executor.authenticateIfMultiUser(connection, op);
    verify(pool, never()).executeOn(any(Connection.class), any(Op.class));
  }

  @Test
  public void authenticateIfRequired_noOp_WhenOpNeedsNoUserId() {
    when(server.getRequiresCredentials()).thenReturn(true);
    when(op.needsUserId()).thenReturn(false);
    executor.authenticateIfMultiUser(connection, op);
    verify(pool, never()).executeOn(any(Connection.class), any(Op.class));
  }

  @Test
  public void authenticateIfRequired_noOp_singleUser_hasId() {
    when(server.getRequiresCredentials()).thenReturn(true);
    when(op.needsUserId()).thenReturn(true);
    when(pool.getMultiuserAuthentication()).thenReturn(false);
    when(server.getUserId()).thenReturn(123L);
    executor.authenticateIfMultiUser(connection, op);
    verify(pool, never()).executeOn(any(Connection.class), any(Op.class));

  }

  @Test
  public void authenticateIfRequired_setId_singleUser_hasNoId() {
    when(server.getRequiresCredentials()).thenReturn(true);
    when(op.needsUserId()).thenReturn(true);
    when(pool.getMultiuserAuthentication()).thenReturn(false);
    when(server.getUserId()).thenReturn(-1L);
    when(pool.executeOn(any(Connection.class), any(Op.class))).thenReturn(123L);
    when(connection.getWrappedConnection()).thenReturn(connection);
    executor.authenticateIfMultiUser(connection, op);
    verify(pool, never()).executeOn(any(Connection.class), any(Op.class));
  }

  @Test
  public void execute_calls_authenticateIfMultiUser() throws Exception {
    when(connection.execute(any())).thenReturn(123L);
    when(connectionManager.borrowConnection(5)).thenReturn(connection);
    OpExecutorImpl spy = spy(executor);

    spy.execute(op, 1);
    verify(spy).authenticateIfMultiUser(any(), any());
  }

  @Test
  public void authenticateIfMultiUser_calls_authenticateMultiUser() {
    OpExecutorImpl spy = spy(executor);
    when(connection.getServer()).thenReturn(server);
    when(pool.executeOn(any(ServerLocation.class), any())).thenReturn(123L);
    UserAttributes userAttributes = new UserAttributes(null, null);

    when(server.getRequiresCredentials()).thenReturn(false);
    spy.authenticateIfMultiUser(connection, op);
    verify(spy, never()).authenticateMultiuser(any(), any(), any());

    when(server.getRequiresCredentials()).thenReturn(true);
    when(op.needsUserId()).thenReturn(false);
    spy.authenticateIfMultiUser(connection, op);
    verify(spy, never()).authenticateMultiuser(any(), any(), any());

    when(server.getRequiresCredentials()).thenReturn(true);
    when(op.needsUserId()).thenReturn(true);
    when(pool.getMultiuserAuthentication()).thenReturn(false);
    spy.authenticateIfMultiUser(connection, op);
    verify(spy, never()).authenticateMultiuser(any(), any(), any());

    when(server.getRequiresCredentials()).thenReturn(true);
    when(op.needsUserId()).thenReturn(true);
    when(pool.getMultiuserAuthentication()).thenReturn(true);
    spy.authenticateIfMultiUser(connection, op);
    verify(spy, never()).authenticateMultiuser(any(), any(), any());

    when(server.getRequiresCredentials()).thenReturn(true);
    when(op.needsUserId()).thenReturn(true);
    when(pool.getMultiuserAuthentication()).thenReturn(true);
    doReturn(userAttributes).when(spy).getUserAttributesFromThreadLocal();
    spy.authenticateIfMultiUser(connection, op);
    verify(spy).authenticateMultiuser(pool, connection, userAttributes);

    // calling it again wont' increase the invocation time
    spy.authenticateIfMultiUser(connection, op);
    verify(spy).authenticateMultiuser(pool, connection, userAttributes);
  }
}
