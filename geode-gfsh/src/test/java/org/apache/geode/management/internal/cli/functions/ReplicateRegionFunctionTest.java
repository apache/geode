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
package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException;
import org.apache.geode.cache.client.internal.pooling.PooledConnection;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.cache.wan.GatewaySenderEventDispatcher;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class ReplicateRegionFunctionTest {

  private ReplicateRegionFunction rrf;
  private long startTime;
  private final int entries = 25;
  private Clock clockMock;
  private ReplicateRegionFunction.ThreadSleeper threadSleeperMock;
  private InternalCache internalCacheMock;
  private GatewaySender gatewaySenderMock;
  private PoolImpl poolMock;
  private Connection connectionMock;
  private GatewaySenderEventDispatcher dispatcherMock;

  @SuppressWarnings("unchecked")
  private final FunctionContext<Object[]> contextMock = mock(FunctionContext.class);

  @SuppressWarnings("unchecked")
  private final Region<Object, Object> regionMock = mock(InternalRegion.class, RETURNS_DEEP_STUBS);

  @SuppressWarnings("unchecked")
  private final Region.Entry<String, String> entryMock = mock(Region.Entry.class);

  @SuppressWarnings("unchecked")
  private final Region.Entry<String, String> entryMock2 = mock(Region.Entry.class);

  @Before
  public void setUp() throws InterruptedException {
    clockMock = mock(Clock.class);
    threadSleeperMock = mock(ReplicateRegionFunction.ThreadSleeper.class);
    doNothing().when(threadSleeperMock).millis(anyLong());
    internalCacheMock = mock(InternalCache.class);
    gatewaySenderMock = mock(AbstractGatewaySender.class, RETURNS_DEEP_STUBS);
    when(gatewaySenderMock.getId()).thenReturn("mySender");
    poolMock = mock(PoolImpl.class);
    connectionMock = mock(PooledConnection.class);
    dispatcherMock = mock(GatewaySenderEventDispatcher.class);
    rrf = new ReplicateRegionFunction();
    rrf.setClock(clockMock);
    rrf.setThreadSleeper(threadSleeperMock);
    startTime = System.currentTimeMillis();
  }

  @Test
  public void doPostSendBatchActions_DoNotSleepIfGetTimeToSleepIsZero()
      throws InterruptedException {
    ReplicateRegionFunction rrfSpy = spy(rrf);
    doReturn(0L).when(rrfSpy).getTimeToSleep(anyLong(), anyInt(), anyLong());
    rrfSpy.doPostSendBatchActions(startTime, entries, 1L);
    verify(threadSleeperMock, never()).millis(anyLong());
  }

  @Test
  public void doPostSendBatchActions_SleepIfGetTimeToSleepIsNotZero()
      throws InterruptedException {
    long expectedMsToSleep = 1100L;
    ReplicateRegionFunction rrfSpy = spy(rrf);
    doReturn(expectedMsToSleep).when(rrfSpy).getTimeToSleep(anyLong(), anyInt(), anyLong());
    rrfSpy.doPostSendBatchActions(startTime, entries, 1L);
    verify(threadSleeperMock, times(1)).millis(expectedMsToSleep);
  }

  @Test
  public void doPostSendBatchActions_ThrowInterruptedIfInterrupted() {
    long maxRate = 100;
    Thread.currentThread().interrupt();
    assertThatThrownBy(
        () -> rrf.doPostSendBatchActions(startTime, entries, maxRate))
            .isInstanceOf(InterruptedException.class);
  }

  @Test
  public void executeFunction_verifyOutputWhenRegionNotFound() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(internalCacheMock.getRegion(any())).thenReturn(null);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    CliFunctionResult result = rrf.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Region myRegion not found");
  }

  @Test
  public void executeFunction_verifyOutputWhenSenderNotFound() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(internalCacheMock.getRegion(any())).thenReturn(regionMock);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(null);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    CliFunctionResult result = rrf.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Sender mySender not found");
  }

  @Test
  public void executeFunction_verifyOutputWhenSenderIsNotRunning() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(gatewaySenderMock.isRunning()).thenReturn(false);
    when(regionMock.getAttributes().getGatewaySenderIds().contains(anyString())).thenReturn(true);
    when(internalCacheMock.getRegion(any())).thenReturn(regionMock);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    CliFunctionResult result = rrf.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Sender mySender is not running");
  }

  @Test
  public void executeFunction_verifyOutputWhenSenderIsSerialAndSenderIsNotPrimary() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(gatewaySenderMock.isRunning()).thenReturn(true);
    when(regionMock.getAttributes().getGatewaySenderIds().contains(anyString())).thenReturn(true);
    when(gatewaySenderMock.isParallel()).thenReturn(false);
    when(((InternalGatewaySender) gatewaySenderMock).isPrimary()).thenReturn(false);
    when(internalCacheMock.getRegion(any())).thenReturn(regionMock);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    CliFunctionResult result = rrf.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Sender mySender is serial and not primary. 0 entries replicated.");
  }

  @Test
  public void replicateRegion_verifyOutputWhenNoPoolAvailableAndEntriesInRegion() {
    ReplicateRegionFunction rrfSpy = spy(rrf);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(null);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("No connection pool available towards receiver");
  }

  @Test
  public void replicateRegion_verifyOutputWhenNoPoolAvailableAndNoEntriesInRegion() {
    ReplicateRegionFunction rrfSpy = spy(rrf);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(null);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    doReturn(new HashSet<>()).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries replicated: 0");
  }

  @Test
  public void replicateRegion_verifyErrorWhenSenderNotConfiguredWithForRegion() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    Set<String> senders = new HashSet<>();
    senders.add("notMySender");
    when(gatewaySenderMock.isParallel()).thenReturn(true);
    when(internalCacheMock.getRegion(any())).thenReturn(regionMock);
    when(regionMock.getAttributes().getGatewaySenderIds()).thenReturn(senders);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = rrf.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Region myRegion is not configured to use sender mySender");
  }

  @Test
  public void replicateRegion_verifyErrorWhenNoConnectionAvailableAtStartAndEntriesInRegion() {
    ReplicateRegionFunction rrfSpy = spy(rrf);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());

    assertThatThrownBy(
        () -> rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10))
            .isInstanceOf(NoAvailableServersException.class);
  }

  @Test
  public void replicateRegion_verifySuccessWhenNoConnectionAvailableAtStartAndNoEntriesInRegion() {
    ReplicateRegionFunction rrfSpy = spy(rrf);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    doReturn(new HashSet<>()).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries replicated: 0");
  }

  @Test
  public void replicateRegion_verifyOutputWhenNoConnectionAvailableAfterReplicatingSomeEntries()
      throws BatchException70 {
    ReplicateRegionFunction rrfSpy = spy(rrf);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    entries.add(entryMock2);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy)
        .createGatewaySenderEvent(any(), any(),
            any(), any());
    doNothing().doThrow(ConnectionDestroyedException.class).doNothing().when(dispatcherMock)
        .sendBatch(anyList(), any(), any(), anyInt(), anyBoolean());

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 1);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("No connection available towards receiver after having replicated 1 entries");
  }

  @Test
  public void replicateRegion_verifySuccessfulReplication() throws BatchException70 {
    ReplicateRegionFunction rrfSpy = spy(rrf);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());
    doNothing().when(dispatcherMock).sendBatch(anyList(), any(), any(), anyInt(), anyBoolean());

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries replicated: 1");
  }


  @Test
  public void replicateRegion_verifySuccessWithRetryWhenConnectionDestroyed()
      throws BatchException70 {

    ReplicateRegionFunction rrfSpy = spy(rrf);
    ConnectionDestroyedException exceptionWhenSendingBatch =
        new ConnectionDestroyedException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());
    doThrow(exceptionWhenSendingBatch).doNothing().when(dispatcherMock).sendBatch(anyList(), any(),
        any(), anyInt(), anyBoolean());

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries replicated: 1");
  }

  @Test
  public void replicateRegion_verifyErrorWhenConnectionDestroyedTwice() throws BatchException70 {
    ReplicateRegionFunction rrfSpy = spy(rrf);
    ConnectionDestroyedException exceptionWhenSendingBatch =
        new ConnectionDestroyedException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Connection error) in operation after having replicated 0 entries");
  }

  @Test
  public void replicateRegion_verifySuccessWithRetryWhenServerConnectivityException()
      throws BatchException70 {

    ReplicateRegionFunction rrfSpy = spy(rrf);
    ServerConnectivityException exceptionWhenSendingBatch =
        new ServerConnectivityException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());
    doThrow(exceptionWhenSendingBatch).doNothing().when(dispatcherMock).sendBatch(anyList(), any(),
        any(), anyInt(), anyBoolean());

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries replicated: 1");
  }

  @Test
  public void replicateRegion_verifyErrorWhenServerConnectivityExceptionTwice()
      throws BatchException70 {
    ReplicateRegionFunction rrfSpy = spy(rrf);
    ServerConnectivityException exceptionWhenSendingBatch =
        new ServerConnectivityException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Connection error) in operation after having replicated 0 entries");
  }

  @Test
  public void replicateRegion_verifyErrorWhenBatchExceptionWhileSendingBatch()
      throws BatchException70 {

    ReplicateRegionFunction rrfSpy = spy(rrf);
    BatchException70 exceptionWhenSendingBatch =
        new BatchException70("My batch exception", new Exception(), 0, 0);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (My batch exception) in operation after having replicated 0 entries");
  }

  @Test
  public void replicateRegion_verifyExceptionThrownWhenExceptionWhileSendingBatch()
      throws BatchException70 {

    ReplicateRegionFunction rrfSpy = spy(rrf);
    RuntimeException exceptionWhenSendingBatch =
        new RuntimeException("Exception when sending batch");
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);

    assertThatThrownBy(
        () -> rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10))
            .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenMaxRateIsZero() {
    assertThat(rrf.getTimeToSleep(startTime, 1, 0)).isEqualTo(0);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenReplicatedEntriesIsZero() {
    assertThat(rrf.getTimeToSleep(startTime, 0, 1)).isEqualTo(0);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenBelowMaxRate() {
    long elapsedTime = 2000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(rrf.getTimeToSleep(startTime, 1, 1)).isEqualTo(0);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenOnMaxRate() {
    long elapsedTime = 1000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(rrf.getTimeToSleep(startTime, 1, 1)).isEqualTo(0);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenAboveMaxRate_value1() {
    long elapsedTime = 1000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(rrf.getTimeToSleep(startTime, 2, 1)).isEqualTo(1000);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenAboveMaxRate_value2() {
    long elapsedTime = 1000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(rrf.getTimeToSleep(startTime, 4, 1)).isEqualTo(3000);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenAboveMaxRate_value3() {
    long elapsedTime = 2000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(rrf.getTimeToSleep(startTime, 4, 1)).isEqualTo(2000);
  }
}
