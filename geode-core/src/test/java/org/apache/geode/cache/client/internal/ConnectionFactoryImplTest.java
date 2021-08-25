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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ConnectionFactoryImplTest {
  private ConnectionFactoryImpl factory;
  private ConnectionConnector connector;
  private ConnectionSource source;
  private PoolImpl pool;
  private CancelCriterion cancelCriterion;
  private Connection connection;
  private ServerLocation server;

  @Before
  public void before() throws Exception {
    server = mock(ServerLocation.class);
    connection = mock(Connection.class);
    connector = mock(ConnectionConnector.class);
    source = mock(ConnectionSource.class);
    pool = mock(PoolImpl.class);
    cancelCriterion = mock(CancelCriterion.class);
    factory = new ConnectionFactoryImpl(connector, source, 5, pool, cancelCriterion);
  }

  @Test
  public void authenticateIfRequired_noOp_whenUsedByGateway() {
    when(pool.isUsedByGateway()).thenReturn(true);
    factory.authenticateIfRequired(connection);
    verify(connection, never()).getServer();
  }

  @Test
  public void authenticateIfRequired_noOp_whenMultiUser() {
    when(pool.isUsedByGateway()).thenReturn(false);
    when(pool.getMultiuserAuthentication()).thenReturn(true);
    factory.authenticateIfRequired(connection);
    verify(connection, never()).getServer();
  }

  @Test
  public void authenticateIfRequired_noOp_whenServerNotRequireCredential() {
    when(pool.isUsedByGateway()).thenReturn(false);
    when(pool.getMultiuserAuthentication()).thenReturn(false);
    when(connection.getServer()).thenReturn(server);
    when(server.getRequiresCredentials()).thenReturn(false);
    factory.authenticateIfRequired(connection);
    verify(connection).getServer();
    verify(pool, never()).executeOn(any(Connection.class), any(Op.class));
  }

  @Test
  public void authenticateIfRequired_noOp_whenServerHasUserId() {
    when(pool.isUsedByGateway()).thenReturn(false);
    when(pool.getMultiuserAuthentication()).thenReturn(false);
    when(connection.getServer()).thenReturn(server);
    when(server.getRequiresCredentials()).thenReturn(true);
    when(server.getUserId()).thenReturn(1234L);
    factory.authenticateIfRequired(connection);
    verify(connection).getServer();
    verify(pool, never()).executeOn(any(Connection.class), any(Op.class));
  }

  @Test
  public void authenticateIfRequired() {
    when(pool.isUsedByGateway()).thenReturn(false);
    when(pool.getMultiuserAuthentication()).thenReturn(false);
    when(connection.getServer()).thenReturn(server);
    when(server.getRequiresCredentials()).thenReturn(true);
    when(server.getUserId()).thenReturn(-1L);
    when(pool.executeOn(any(Connection.class), any(Op.class))).thenReturn(123L);
    factory.authenticateIfRequired(connection);
    verify(connection).getServer();
    verify(pool).executeOn(any(Connection.class), any(Op.class));
    verify(server).setUserId(123L);
  }

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Test
  public void getSetServerIdShouldBeSynchronizedToAvoidExtraLoginRequest() throws Exception {
    when(pool.isUsedByGateway()).thenReturn(false);
    when(pool.getMultiuserAuthentication()).thenReturn(false);
    when(pool.executeOn(any(Connection.class), any())).thenReturn(123L);
    when(server.getRequiresCredentials()).thenReturn(true);
    when(connection.getServer()).thenReturn(server);

    doCallRealMethod().when(server).setUserId(anyLong());
    doCallRealMethod().when(server).getUserId();
    // set up the initial userId to be -1
    server.setUserId(-1);

    CompletableFuture<Void> future1 =
        executor.runAsync(() -> factory.authenticateIfRequired(connection));
    CompletableFuture<Void> future2 =
        executor.runAsync(() -> factory.authenticateIfRequired(connection));
    future1.get();
    future2.get();

    // setUserId should only be called once by this two threads. The other time
    // is called by the test setup.
    verify(server, times(2)).setUserId(anyLong());
  }
}
