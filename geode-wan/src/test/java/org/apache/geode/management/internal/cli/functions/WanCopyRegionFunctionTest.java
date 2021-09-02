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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
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
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.cache.wan.GatewaySenderEventDispatcher;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.management.internal.cli.functions.WanCopyRegionFunction.EventCreator;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class WanCopyRegionFunctionTest {

  private WanCopyRegionFunction function;
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

  private final Region<?, ?> regionMock =
      uncheckedCast(mock(InternalRegion.class));

  private EventCreator eventCreatorMock;

  @Before
  public void setUp() throws InterruptedException {
    startTime = System.currentTimeMillis();

    clockMock = mock(Clock.class);

    threadSleeperMock = mock(WanCopyRegionFunction.ThreadSleeper.class);
    doNothing().when(threadSleeperMock).sleep(anyLong());

    gatewaySenderMock = mock(AbstractGatewaySender.class);
    when(gatewaySenderMock.getId()).thenReturn("mySender");

    poolMock = mock(PoolImpl.class);

    connectionMock = mock(PooledConnection.class);
    when(connectionMock.getWanSiteVersion()).thenReturn(KnownVersion.GEODE_1_15_0.ordinal());

    dispatcherMock = mock(GatewaySenderEventDispatcher.class);

    eventCreatorMock = mock(EventCreator.class);
    when(eventCreatorMock.createGatewaySenderEvent(any(), any(), any(), any()))
        .thenReturn(uncheckedCast(mock(GatewayQueueEvent.class)));

    function = new WanCopyRegionFunction(clockMock, threadSleeperMock,
        mock(WanCopyRegionFunction.WanCopyRegionFunctionExecutorFactory.class), eventCreatorMock);

    AbstractGatewaySenderEventProcessor eventProcessorMock =
        mock(AbstractGatewaySenderEventProcessor.class);
    when(eventProcessorMock.getDispatcher()).thenReturn(dispatcherMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor())
        .thenReturn(eventProcessorMock);

    RegionAttributes<?, ?> attributesMock = mock(RegionAttributes.class);
    Set<?> idsMock = mock(Set.class);
    when(idsMock.contains(anyString())).thenReturn(true);
    when(attributesMock.getGatewaySenderIds()).thenReturn(uncheckedCast(idsMock));
    when(regionMock.getAttributes()).thenReturn(uncheckedCast(attributesMock));

    internalCacheMock = mock(InternalCache.class);
    when(internalCacheMock.getRegion(any())).thenReturn(uncheckedCast(regionMock));
  }

  @Test
  public void doPostSendBatchActions_DoNotSleepIfMaxRateIsZero()
      throws InterruptedException {
    Object[] options = new Object[] {"myRegion", "mySender", false, 0L, 10};
    when(internalCacheMock.getRegion(any())).thenReturn(null);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    function.executeFunction(contextMock);
    verify(threadSleeperMock, never()).sleep(anyLong());
  }

  @Test
  public void doPostSendBatchActions_SleepIfMaxRateIsNotZero()
      throws InterruptedException, BatchException70, ExecutionException, TimeoutException {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 2};
    WanCopyRegionFunction.ThreadSleeper sleeperMock =
        mock(WanCopyRegionFunction.ThreadSleeper.class);
    doNothing().when(sleeperMock).sleep(anyLong());

    executeAsyncWanCopyRegionFunction(options, sleeperMock)
        .get(GeodeAwaitility.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(sleeperMock, times(1)).sleep(captor.capture());
    assertThat(captor.getValue()).isGreaterThan(0).isLessThanOrEqualTo(2000);
  }

  @Test
  public void doPostSendBatchActions_ThrowInterruptedIfInterruptedTimeToSleepNotZero()
      throws InterruptedException {
    long maxRate = 1;
    long elapsedTime = 20L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);

    // Sleep so that the sleep can be interrupted
    doAnswer(invocation -> {
      Thread.sleep(1);
      return null;
    }).when(threadSleeperMock).sleep(anyLong());

    Thread.currentThread().interrupt();

    assertThatThrownBy(
        () -> function.doPostSendBatchActions(startTime, entries, maxRate))
            .isInstanceOf(InterruptedException.class);
  }

  @Test
  public void doPostSendBatchActions_ThrowInterruptedIfInterruptedTimeToSleepIsZero() {
    long maxRate = 0;
    Thread.currentThread().interrupt();
    assertThatThrownBy(
        () -> function.doPostSendBatchActions(startTime, entries, maxRate))
            .isInstanceOf(InterruptedException.class);
  }

  @Test
  public void executeFunction_verifyErrorWhenRegionNotFound() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(internalCacheMock.getRegion(any())).thenReturn(null);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Region myRegion not found");
  }

  @Test
  public void executeFunction_verifyErrorWhenSenderNotFound() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(internalCacheMock.getGatewaySender(any())).thenReturn(null);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Sender mySender not found");
  }

  @Test
  public void executeFunction_verifyErrorWhenSenderIsNotRunning() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(gatewaySenderMock.isRunning()).thenReturn(false);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Sender mySender is not running");
  }

  @Test
  public void executeFunction_verifySuccessWhenSenderIsSerialAndSenderIsNotPrimary() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(gatewaySenderMock.isRunning()).thenReturn(true);
    when(gatewaySenderMock.isParallel()).thenReturn(false);
    when(((InternalGatewaySender) gatewaySenderMock).isPrimary()).thenReturn(false);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Sender mySender is serial and not primary. 0 entries copied.");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenRemoteSiteDoesNotSupportCommand()
      throws InterruptedException {
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    PooledConnection oldWanSiteConn = mock(PooledConnection.class);
    when(oldWanSiteConn.getWanSiteVersion()).thenReturn(KnownVersion.GEODE_1_14_0.ordinal());
    when(poolMock.acquireConnection()).thenReturn(oldWanSiteConn);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Command not supported at remote site.");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenNoPoolAvailableAndEntriesInRegion()
      throws InterruptedException {
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(null);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("No connection pool available to receiver");
  }

  @Test
  public void wanCopyRegion_verifySuccessWhenNoPoolAvailableAndNoEntriesInRegion()
      throws InterruptedException {
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(null);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(regionMock.entrySet()).thenReturn(new HashSet<>());

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
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
    when(regionMock.getAttributes().getGatewaySenderIds()).thenReturn(senders);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Region myRegion is not configured to use sender mySender");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenNoConnectionAvailableAtStartAndEntriesInRegion() {
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));

    assertThatThrownBy(
        () -> function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10))
            .isInstanceOf(NoAvailableServersException.class);
  }

  @Test
  public void wanCopyRegion_verifySuccessWhenNoConnectionAvailableAtStartAndNoEntriesInRegion()
      throws InterruptedException {
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(regionMock.entrySet()).thenReturn(new HashSet<>());

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 0");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenNoConnectionAvailableAfterCopyingSomeEntries()
      throws BatchException70, InterruptedException {
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock)
        .thenThrow(NoAvailableServersException.class);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doNothing().doThrow(ConnectionDestroyedException.class).doNothing().when(dispatcherMock)
        .sendBatch(anyList(), any(), any(), anyInt(), anyBoolean());

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 1);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("No connection available to receiver after having copied 1 entries");
  }

  @Test
  public void wanCopyRegion_verifySuccess() throws BatchException70, InterruptedException {
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doNothing().when(dispatcherMock).sendBatch(anyList(), any(), any(), anyInt(), anyBoolean());

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 1");
  }

  @Test
  public void wanCopyRegion_verifySuccessWithRetryWhenConnectionDestroyed()
      throws BatchException70, InterruptedException {
    ConnectionDestroyedException exceptionWhenSendingBatch =
        new ConnectionDestroyedException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).doNothing().when(dispatcherMock).sendBatch(anyList(), any(),
        any(), anyInt(), anyBoolean());

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 1");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenConnectionDestroyedTwice()
      throws BatchException70, InterruptedException {
    ConnectionDestroyedException exceptionWhenSendingBatch =
        new ConnectionDestroyedException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Connection error) in operation after having copied 0 entries");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenRegionDestroyed()
      throws BatchException70, InterruptedException {
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doNothing().when(dispatcherMock).sendBatch(anyList(), any(), any(), anyInt(), anyBoolean());
    doReturn(true).when(regionMock).isDestroyed();

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Region destroyed) in operation after having copied 1 entries");
  }

  @Test
  public void wanCopyRegion_verifySuccessWithRetryWhenServerConnectivityException()
      throws BatchException70, InterruptedException {
    ServerConnectivityException exceptionWhenSendingBatch =
        new ServerConnectivityException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).doNothing().when(dispatcherMock).sendBatch(anyList(), any(),
        any(), anyInt(), anyBoolean());

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 1");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenServerConnectivityExceptionTwice()
      throws BatchException70, InterruptedException {
    ServerConnectivityException exceptionWhenSendingBatch =
        new ServerConnectivityException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Connection error) in operation after having copied 0 entries");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenBatchExceptionWhileSendingBatch()
      throws BatchException70, InterruptedException {
    BatchException70 exceptionWhenSendingBatch =
        new BatchException70("My batch exception", new Exception("test exception"), 0, 0);
    BatchException70 topLevelException =
        new BatchException70(Collections.singletonList(exceptionWhenSendingBatch));
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(topLevelException).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    CliFunctionResult result =
        function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo(
            "Error (java.lang.Exception: test exception) in operation after having copied 0 entries");
  }

  @Test
  public void wanCopyRegion_verifyExceptionThrownWhenExceptionWhileSendingBatch()
      throws BatchException70 {
    RuntimeException exceptionWhenSendingBatch =
        new RuntimeException("Exception when sending batch");
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    assertThatThrownBy(
        () -> function.wanCopyRegion(contextMock, regionMock, gatewaySenderMock, 1, 10))
            .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void executeWanCopyRegionFunctionInNewThread_verifyErrorWhenAlreadyRunningCommand()
      throws BatchException70, InterruptedException, ExecutionException, TimeoutException {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 1};
    Future<CliFunctionResult> future = executeAsyncWanCopyRegionFunction(options);

    // Wait for the execute function to start
    await()
        .until(
            () -> WanCopyRegionFunction.getNumberOfCurrentExecutions() == 1);

    // Execute another function instance for the same region and sender-id
    Future<CliFunctionResult> future2 = executeAsyncWanCopyRegionFunction(options);
    CliFunctionResult result1 = future2.get();

    assertThat(result1.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result1.getStatusMessage())
        .isEqualTo("There is already a command running for region myRegion and sender mySender");

    // Wait for the first function to finish
    future.get(GeodeAwaitility.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  public void executeWanCopyRegionFunctionInNewThread_cancelExecution()
      throws ExecutionException, InterruptedException, BatchException70, TimeoutException {
    Object[] executeOptions = new Object[] {"myRegion", "mySender", false, 1L, 1};
    Future<CliFunctionResult> future = executeAsyncWanCopyRegionFunction(executeOptions);

    // Wait for the function to start execution
    await()
        .until(
            () -> WanCopyRegionFunction.getNumberOfCurrentExecutions() == 1);

    // Cancel the function execution
    Object[] cancelOptions = new Object[] {"myRegion", "mySender", true, 1L, 1};
    final FunctionContext<Object[]> cancelContextMock =
        uncheckedCast(mock(FunctionContext.class));
    when(cancelContextMock.getArguments()).thenReturn(cancelOptions);

    WanCopyRegionFunction cancelFunction = new WanCopyRegionFunction();
    CliFunctionResult result1 = cancelFunction.executeFunction(cancelContextMock);

    assertThat(result1.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result1.getStatusMessage())
        .isEqualTo("Execution canceled");

    future.get(GeodeAwaitility.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  public void executeWanCopyRegionFunctionInNewThread_cancelAllExecutions()
      throws ExecutionException, InterruptedException, BatchException70, TimeoutException {
    Object[] options1 = new Object[] {"myRegion", "mySender", false, 1L, 1};
    Future<CliFunctionResult> future1 = executeAsyncWanCopyRegionFunction(options1);

    Object[] options2 = new Object[] {"myRegion", "mySender1", false, 1L, 1};
    Future<CliFunctionResult> future2 = executeAsyncWanCopyRegionFunction(options2);

    // Wait for the functions to start execution
    await()
        .until(
            () -> WanCopyRegionFunction.getNumberOfCurrentExecutions() == 2);

    // Cancel the function executions
    Object[] cancelAllOptions = new Object[] {"*", "*", true, 1L, 1};
    final FunctionContext<Object[]> cancelAllContextMock =
        uncheckedCast(mock(FunctionContext.class));
    when(cancelAllContextMock.getArguments()).thenReturn(cancelAllOptions);

    WanCopyRegionFunction function2 = new WanCopyRegionFunction();
    CliFunctionResult result1 = function2.executeFunction(cancelAllContextMock);

    assertThat(result1.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result1.getStatusMessage())
        .isEqualTo("Executions canceled: [(myRegion,mySender1), (myRegion,mySender)]");

    future1.get(GeodeAwaitility.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
    future2.get(GeodeAwaitility.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
  }

  private Future<CliFunctionResult> executeAsyncWanCopyRegionFunction(Object[] options)
      throws BatchException70 {
    return executeAsyncWanCopyRegionFunction(options, new WanCopyRegionFunction.ThreadSleeper());
  }

  private Future<CliFunctionResult> executeAsyncWanCopyRegionFunction(Object[] options,
      WanCopyRegionFunction.ThreadSleeper sleeper)
      throws BatchException70 {
    Region<?, ?> regionMock =
        uncheckedCast(mock(InternalRegion.class));

    RegionAttributes<?, ?> attributesMock = mock(RegionAttributes.class);
    Set<?> idsMock = mock(Set.class);
    when(idsMock.contains(anyString())).thenReturn(true);
    when(attributesMock.getGatewaySenderIds()).thenReturn(uncheckedCast(idsMock));
    when(regionMock.getAttributes()).thenReturn(uncheckedCast(attributesMock));

    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));

    doNothing().when(dispatcherMock).sendBatch(anyList(), any(), any(), anyInt(), anyBoolean());

    AbstractGatewaySender gatewaySenderMock = mock(AbstractGatewaySender.class);
    when(gatewaySenderMock.getId()).thenReturn((String) options[1]);
    when(gatewaySenderMock.isRunning()).thenReturn(true);
    when(gatewaySenderMock.isParallel()).thenReturn(true);
    when(gatewaySenderMock.isPrimary()).thenReturn(false);

    when(gatewaySenderMock.getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    AbstractGatewaySenderEventProcessor eventProcessorMock =
        mock(AbstractGatewaySenderEventProcessor.class);
    when(eventProcessorMock.getDispatcher()).thenReturn(dispatcherMock);
    when(gatewaySenderMock.getEventProcessor()).thenReturn(eventProcessorMock);

    InternalCache internalCacheMock = mock(InternalCache.class);
    when(internalCacheMock.getRegion(any())).thenReturn(uncheckedCast(regionMock));
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);

    FunctionContext<Object[]> contextMock = uncheckedCast(mock(FunctionContext.class));
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    WanCopyRegionFunction function = new WanCopyRegionFunction(Clock.systemDefaultZone(),
        sleeper, (cache) -> Executors.newSingleThreadExecutor(), eventCreatorMock);

    return CompletableFuture.supplyAsync(() -> function.executeFunction(contextMock));
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenMaxRateIsZero() {
    assertThat(function.getTimeToSleep(startTime, 1, 0)).isEqualTo(0);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenCopiedEntriesIsZero() {
    assertThat(function.getTimeToSleep(startTime, 0, 1)).isEqualTo(0);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenBelowMaxRate() {
    long elapsedTime = 2000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(function.getTimeToSleep(startTime, 1, 1)).isEqualTo(0);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenOnMaxRate() {
    long elapsedTime = 1000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(function.getTimeToSleep(startTime, 1, 1)).isEqualTo(0);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenAboveMaxRate_value1() {
    long elapsedTime = 1000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(function.getTimeToSleep(startTime, 2, 1)).isEqualTo(1000);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenAboveMaxRate_value2() {
    long elapsedTime = 1000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(function.getTimeToSleep(startTime, 4, 1)).isEqualTo(3000);
  }

  @Test
  public void getTimeToSleep_ReturnZeroWhenAboveMaxRate_value3() {
    long elapsedTime = 2000L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    assertThat(function.getTimeToSleep(startTime, 4, 1)).isEqualTo(2000);
  }
}
