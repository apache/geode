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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

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
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class WanCopyRegionFunctionTest {

  private WanCopyRegionFunction rrf;
  private long startTime;
  private final int entries = 25;
  private Clock clockMock;
  private WanCopyRegionFunction.ThreadSleeper threadSleeperMock;
  private InternalCache internalCacheMock;
  private GatewaySender gatewaySenderMock;
  private PoolImpl poolMock;
  private Connection connectionMock;
  private GatewaySenderEventDispatcher dispatcherMock;

  private final FunctionContext<Object[]> contextMock = uncheckedCast(mock(FunctionContext.class));

  private final Region<Object, Object> regionMock =
      uncheckedCast(mock(InternalRegion.class, RETURNS_DEEP_STUBS));

  private final Region.Entry<String, String> entryMock = uncheckedCast(mock(Region.Entry.class));

  private final Region.Entry<String, String> entryMock2 = uncheckedCast(mock(Region.Entry.class));

  @Before
  public void setUp() throws InterruptedException {
    clockMock = mock(Clock.class);
    threadSleeperMock = mock(WanCopyRegionFunction.ThreadSleeper.class);
    doNothing().when(threadSleeperMock).millis(anyLong());
    internalCacheMock = mock(InternalCache.class);
    gatewaySenderMock = mock(AbstractGatewaySender.class, RETURNS_DEEP_STUBS);
    when(gatewaySenderMock.getId()).thenReturn("mySender");
    poolMock = mock(PoolImpl.class);
    connectionMock = mock(PooledConnection.class);
    when(connectionMock.getWanSiteVersion()).thenReturn(KnownVersion.GEODE_1_15_0.ordinal());
    dispatcherMock = mock(GatewaySenderEventDispatcher.class);
    rrf = new WanCopyRegionFunction(clockMock, threadSleeperMock);
    startTime = System.currentTimeMillis();
  }

  @Test
  public void doPostSendBatchActions_DoNotSleepIfGetTimeToSleepIsZero()
      throws InterruptedException {
    WanCopyRegionFunction rrfSpy = spy(rrf);
    doReturn(0L).when(rrfSpy).getTimeToSleep(anyLong(), anyInt(), anyLong());
    rrfSpy.doPostSendBatchActions(startTime, entries, 1L);
    verify(threadSleeperMock, never()).millis(anyLong());
  }

  @Test
  public void doPostSendBatchActions_SleepIfGetTimeToSleepIsNotZero()
      throws InterruptedException {
    long expectedMsToSleep = 1100L;
    WanCopyRegionFunction rrfSpy = spy(rrf);
    doReturn(expectedMsToSleep).when(rrfSpy).getTimeToSleep(anyLong(), anyInt(), anyLong());
    rrfSpy.doPostSendBatchActions(startTime, entries, 1L);
    verify(threadSleeperMock, times(1)).millis(expectedMsToSleep);
  }

  @Test
  public void doPostSendBatchActions_ThrowInterruptedIfInterruptedTimeToSleepNotZero() {
    long maxRate = 1;
    long elapsedTime = 20L;
    when(clockMock.millis()).thenAnswer((Answer<?>) invocation -> {
      Thread.sleep(1000L);
      return startTime + elapsedTime;
    });
    Thread.currentThread().interrupt();
    assertThatThrownBy(
        () -> rrf.doPostSendBatchActions(startTime, entries, maxRate))
            .isInstanceOf(InterruptedException.class);
  }

  @Test
  public void doPostSendBatchActions_ThrowInterruptedIfInterruptedTimeToSleepIsZero() {
    long maxRate = 0;
    Thread.currentThread().interrupt();
    assertThatThrownBy(
        () -> rrf.doPostSendBatchActions(startTime, entries, maxRate))
            .isInstanceOf(InterruptedException.class);
  }

  @Test
  public void executeFunction_verifyErrorWhenRegionNotFound() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(internalCacheMock.getRegion(any())).thenReturn(null);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    CliFunctionResult result = rrf.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Region myRegion not found");
  }

  @Test
  public void executeFunction_verifyErrorWhenSenderNotFound() {
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
  public void executeFunction_verifyErrorWhenSenderIsNotRunning() {
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
  public void executeFunction_verifySuccessWhenSenderIsSerialAndSenderIsNotPrimary() {
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
        .isEqualTo("Sender mySender is serial and not primary. 0 entries copied.");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenRemoteSiteDoesNotSupportCommand()
      throws InterruptedException {
    WanCopyRegionFunction rrfSpy = spy(rrf);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    PooledConnection oldWanSiteConn = mock(PooledConnection.class);
    when(oldWanSiteConn.getWanSiteVersion()).thenReturn(KnownVersion.GEODE_1_14_0.ordinal());
    when(poolMock.acquireConnection()).thenReturn(oldWanSiteConn);

    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<>();
    entries.add(entryMock);
    doReturn(entries).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);
    doReturn(mock(GatewayQueueEvent.class)).when(rrfSpy).createGatewaySenderEvent(any(), any(),
        any(), any());

    CliFunctionResult result =
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Command not supported at remote site.");
  }


  @Test
  public void wanCopyRegion_verifyErrorWhenNoPoolAvailableAndEntriesInRegion()
      throws InterruptedException {
    WanCopyRegionFunction rrfSpy = spy(rrf);
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
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("No connection pool available to receiver");
  }

  @Test
  public void wanCopyRegion_verifySuccessWhenNoPoolAvailableAndNoEntriesInRegion()
      throws InterruptedException {
    WanCopyRegionFunction rrfSpy = spy(rrf);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(null);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    doReturn(new HashSet<>()).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);

    CliFunctionResult result =
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 0");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenSenderNotConfiguredWithForRegion() {
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
  public void wanCopyRegion_verifyErrorWhenNoConnectionAvailableAtStartAndEntriesInRegion() {
    WanCopyRegionFunction rrfSpy = spy(rrf);
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
        () -> rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10))
            .isInstanceOf(NoAvailableServersException.class);
  }

  @Test
  public void wanCopyRegion_verifySuccessWhenNoConnectionAvailableAtStartAndNoEntriesInRegion()
      throws InterruptedException {
    WanCopyRegionFunction rrfSpy = spy(rrf);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    doReturn(new HashSet<>()).when(rrfSpy).getEntries(regionMock, gatewaySenderMock);

    CliFunctionResult result =
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 0");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenNoConnectionAvailableAfterCopyingSomeEntries()
      throws BatchException70, InterruptedException {
    WanCopyRegionFunction rrfSpy = spy(rrf);
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
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 1);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("No connection available to receiver after having copied 1 entries");
  }

  @Test
  public void wanCopyRegion_verifySuccess() throws BatchException70, InterruptedException {
    WanCopyRegionFunction rrfSpy = spy(rrf);
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
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 1");
  }

  @Test
  public void wanCopyRegion_verifySuccessWithRetryWhenConnectionDestroyed()
      throws BatchException70, InterruptedException {

    WanCopyRegionFunction rrfSpy = spy(rrf);
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
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 1");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenConnectionDestroyedTwice()
      throws BatchException70, InterruptedException {
    WanCopyRegionFunction rrfSpy = spy(rrf);
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
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Connection error) in operation after having copied 0 entries");
  }

  @Test
  public void wanCopyRegion_verifySuccessWithRetryWhenServerConnectivityException()
      throws BatchException70, InterruptedException {

    WanCopyRegionFunction rrfSpy = spy(rrf);
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
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 1");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenServerConnectivityExceptionTwice()
      throws BatchException70, InterruptedException {
    WanCopyRegionFunction rrfSpy = spy(rrf);
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
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Connection error) in operation after having copied 0 entries");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenBatchExceptionWhileSendingBatch()
      throws BatchException70, InterruptedException {

    WanCopyRegionFunction rrfSpy = spy(rrf);
    BatchException70 exceptionWhenSendingBatch =
        new BatchException70("My batch exception", new Exception("test exception"), 0, 0);
    BatchException70 topLevelException =
        new BatchException70(Collections.singletonList(exceptionWhenSendingBatch));
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
    doThrow(topLevelException).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    CliFunctionResult result =
        rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo(
            "Error (java.lang.Exception: test exception) in operation after having copied 0 entries");
  }

  @Test
  public void wanCopyRegion_verifyExceptionThrownWhenExceptionWhileSendingBatch()
      throws BatchException70 {

    WanCopyRegionFunction rrfSpy = spy(rrf);
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
        () -> rrfSpy.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10))
            .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenMaxRateIsZero() {
    assertThat(rrf.getTimeToSleep(startTime, 1, 0)).isEqualTo(0);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenCopiedEntriesIsZero() {
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
