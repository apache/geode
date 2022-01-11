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

import static org.apache.geode.management.internal.cli.functions.WanCopyRegionFunctionDelegate.EventCreatorImpl;
import static org.apache.geode.management.internal.cli.functions.WanCopyRegionFunctionDelegate.ThreadSleeper;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
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
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.DestroyedEntry;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.cache.wan.GatewaySenderEventDispatcher;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.management.internal.cli.functions.WanCopyRegionFunctionDelegate.EventCreator;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class WanCopyRegionFunctionDelegateTest {

  private WanCopyRegionFunctionDelegate function;
  private long startTime;
  private final int entries = 25;
  private Clock clockMock;
  private ThreadSleeper threadSleeperMock;
  private InternalCache internalCacheMock;
  private GatewaySender gatewaySenderMock;
  private PoolImpl poolMock;
  private Connection connectionMock;
  private GatewaySenderEventDispatcher dispatcherMock;

  private final Region<?, ?> regionMock =
      uncheckedCast(mock(InternalRegion.class));

  private RegionAttributes<?, ?> regionAttributesMock;

  private EventCreator eventCreatorMock;

  @Before
  public void setUp() throws InterruptedException {
    startTime = System.currentTimeMillis();
    clockMock = mock(Clock.class);
    threadSleeperMock = mock(ThreadSleeper.class);
    gatewaySenderMock = mock(AbstractGatewaySender.class);
    poolMock = mock(PoolImpl.class);
    connectionMock = mock(PooledConnection.class);
    dispatcherMock = mock(GatewaySenderEventDispatcher.class);
    eventCreatorMock = mock(EventCreator.class);
    AbstractGatewaySenderEventProcessor eventProcessorMock =
        mock(AbstractGatewaySenderEventProcessor.class);
    regionAttributesMock = mock(RegionAttributes.class);
    internalCacheMock = mock(InternalCache.class);

    function = new WanCopyRegionFunctionDelegate(clockMock, threadSleeperMock, eventCreatorMock, 0);

    doNothing().when(threadSleeperMock).sleep(anyLong());
    when(gatewaySenderMock.getId()).thenReturn("mySender");
    when(connectionMock.getWanSiteVersion()).thenReturn(KnownVersion.GEODE_1_15_0.ordinal());
    when(eventCreatorMock.createGatewaySenderEvent(any(), any(), any(), any(), anyLong()))
        .thenReturn(uncheckedCast(mock(GatewayQueueEvent.class)));
    when(eventProcessorMock.getDispatcher()).thenReturn(dispatcherMock);
    when(((InternalGatewaySender) gatewaySenderMock).getEventProcessor())
        .thenReturn(eventProcessorMock);
    when(regionAttributesMock.getGatewaySenderIds())
        .thenReturn(uncheckedCast(Collections.emptySet()));
    when(regionMock.getCache()).thenReturn(internalCacheMock);
    when(regionAttributesMock.getConcurrencyChecksEnabled()).thenReturn(true);
    when(regionMock.getAttributes()).thenReturn(uncheckedCast(regionAttributesMock));
    when(internalCacheMock.getRegion(any())).thenReturn(uncheckedCast(regionMock));
    when(internalCacheMock.cacheTimeMillis()).thenReturn(startTime);
  }

  @Test
  public void doPostSendBatchActions_DoNotSleepIfMaxRateIsZero()
      throws InterruptedException {
    // arrange
    when(internalCacheMock.getRegion(any())).thenReturn(null);

    // act
    function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 0L, 10);

    // assert
    verify(threadSleeperMock, never()).sleep(anyLong());
  }

  @Test
  public void doPostSendBatchActions_SleepIfMaxRateIsNotZero()
      throws InterruptedException, BatchException70 {
    // arrange
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 2};
    ThreadSleeper sleeperMock =
        mock(ThreadSleeper.class);
    doNothing().when(sleeperMock).sleep(anyLong());

    // act
    executeWanCopyRegionFunction(options, sleeperMock);

    // assert
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(sleeperMock, times(1)).sleep(captor.capture());
    assertThat(captor.getValue()).isGreaterThan(0).isLessThanOrEqualTo(2000);
  }

  @Test
  public void doPostSendBatchActions_ThrowInterruptedIfInterruptedTimeToSleepNotZero()
      throws InterruptedException {
    // arrange
    long maxRate = 1;
    long elapsedTime = 20L;
    InterruptedException thrownByThreadSleeper = new InterruptedException("thrownByThreadSleeper");
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    doThrow(thrownByThreadSleeper).when(threadSleeperMock).sleep(anyLong());

    // act
    Throwable thrown =
        catchThrowable(() -> function.doPostSendBatchActions(startTime, entries, maxRate));

    // assert
    assertThat(thrown).isInstanceOf(InterruptedException.class);
  }

  @Test
  public void doPostSendBatchActions_ThrowInterruptedIfInterruptedTimeToSleepIsZero() {
    long maxRate = 0;

    // act
    Thread.currentThread().interrupt();
    Throwable thrown =
        catchThrowable(() -> function.doPostSendBatchActions(startTime, entries, maxRate));

    // assert
    assertThat(thrown).isInstanceOf(InterruptedException.class);
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenRemoteSiteDoesNotSupportCommand()
      throws InterruptedException {
    // arrange
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    PooledConnection oldWanSiteConn = mock(PooledConnection.class);
    when(oldWanSiteConn.getWanSiteVersion()).thenReturn(KnownVersion.GEODE_1_14_0.ordinal());
    when(poolMock.acquireConnection()).thenReturn(oldWanSiteConn);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Command not supported at remote site.");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenNoPoolAvailableAndEntriesInRegion()
      throws InterruptedException {
    // arrange
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(null);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("No connection pool available to receiver");
  }

  @Test
  public void wanCopyRegion_verifySuccessWhenNoPoolAvailableAndNoEntriesInRegion()
      throws InterruptedException {
    // arrange
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(null);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(regionMock.entrySet()).thenReturn(new HashSet<>());

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 0");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenNoConnectionAvailableAtStartAndEntriesInRegion() {
    // arrange
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));

    // act
    Throwable thrown = catchThrowable(() -> function.wanCopyRegion(internalCacheMock, "member1",
        regionMock, gatewaySenderMock, 1, 10));

    // assert
    assertThat(thrown).isInstanceOf(NoAvailableServersException.class);
  }

  @Test
  public void wanCopyRegion_verifySuccessWhenNoConnectionAvailableAtStartAndNoEntriesInRegion()
      throws InterruptedException {
    // arrange
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenThrow(NoAvailableServersException.class)
        .thenThrow(NoAvailableServersException.class);
    when(regionMock.entrySet()).thenReturn(new HashSet<>());

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 0");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenNoConnectionAvailableAfterCopyingSomeEntries()
      throws BatchException70, InterruptedException {
    // arrange
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock)
        .thenThrow(NoAvailableServersException.class);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doNothing().doThrow(ConnectionDestroyedException.class).doNothing().when(dispatcherMock)
        .sendBatch(anyList(), any(), any(), anyInt(), anyBoolean());

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 1);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("No connection available to receiver after having copied 1 entries");
  }

  @Test
  public void wanCopyRegion_verifySuccess() throws BatchException70, InterruptedException {
    // arrange
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doNothing().when(dispatcherMock).sendBatch(anyList(), any(), any(), anyInt(), anyBoolean());

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 1");
  }

  @Test
  public void wanCopyRegion_verifySuccessWithRetryWhenConnectionDestroyed()
      throws BatchException70, InterruptedException {
    // arrange
    ConnectionDestroyedException exceptionWhenSendingBatch =
        new ConnectionDestroyedException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).doNothing().when(dispatcherMock).sendBatch(anyList(), any(),
        any(), anyInt(), anyBoolean());

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 1");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenConnectionDestroyedTwice()
      throws BatchException70, InterruptedException {
    // arrange
    ConnectionDestroyedException exceptionWhenSendingBatch =
        new ConnectionDestroyedException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Connection error) in operation after having copied 0 entries");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenRegionDestroyed()
      throws BatchException70, InterruptedException {
    // arrange
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doNothing().when(dispatcherMock).sendBatch(anyList(), any(), any(), anyInt(), anyBoolean());
    doReturn(true).when(regionMock).isDestroyed();

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Region destroyed) in operation after having copied 1 entries");
  }

  @Test
  public void wanCopyRegion_verifySuccessWithRetryWhenServerConnectivityException()
      throws BatchException70, InterruptedException {
    // arrange
    ServerConnectivityException exceptionWhenSendingBatch =
        new ServerConnectivityException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).doNothing().when(dispatcherMock).sendBatch(anyList(), any(),
        any(), anyInt(), anyBoolean());

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Entries copied: 1");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenServerConnectivityExceptionTwice()
      throws BatchException70, InterruptedException {
    // arrange
    ServerConnectivityException exceptionWhenSendingBatch =
        new ServerConnectivityException("My connection exception", new Exception());
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock).thenReturn(connectionMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (Connection error) in operation after having copied 0 entries");
  }

  @Test
  public void wanCopyRegion_verifyErrorWhenBatchExceptionWhileSendingBatch()
      throws BatchException70, InterruptedException {
    // arrange
    BatchException70 exceptionWhenSendingBatch =
        new BatchException70("My batch exception", new Exception("test exception"), 0, 0);
    BatchException70 topLevelException =
        new BatchException70(Collections.singletonList(exceptionWhenSendingBatch));
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(topLevelException).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    // act
    CliFunctionResult result =
        function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1, 10);

    // assert
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo(
            "Error (java.lang.Exception: test exception) in operation after having copied 0 entries");
  }

  @Test
  public void wanCopyRegion_verifyExceptionThrownWhenExceptionWhileSendingBatch()
      throws BatchException70 {
    // arrange
    RuntimeException exceptionWhenSendingBatch =
        new RuntimeException("Exception when sending batch");
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock);
    Set<Region.Entry<Object, Object>> entries = new HashSet<>();
    entries.add(uncheckedCast(mock(Region.Entry.class)));
    when(regionMock.entrySet()).thenReturn(uncheckedCast(entries));
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt(), anyBoolean());

    // act
    Throwable thrown = catchThrowable(
        () -> function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 1,
            10));

    // assert
    assertThat(thrown).isInstanceOf(RuntimeException.class);
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

  @Test
  public void eventCreator_createGatewaySenderEventDoestNotCreateEventIfEntryIsDestroyed() {
    // arrange
    EventCreator eventCreator = new EventCreatorImpl();
    Region.Entry entry = mock(DestroyedEntry.class);

    // act
    GatewayQueueEvent event = eventCreator.createGatewaySenderEvent(internalCacheMock,
        (InternalRegion) regionMock, gatewaySenderMock, entry, 10);

    // assert
    assertThat(event).isEqualTo(null);
  }

  @Test
  public void eventCreator_createGatewaySenderEventDoesNotCreateEventIfTimestampIsBeforeNonTXEntryTimestamp() {
    // arrange
    EventCreator eventCreator = new EventCreatorImpl();
    long timestamp = System.currentTimeMillis();
    long entryTimestamp = timestamp + 1000;
    NonTXEntry entry = mock(NonTXEntry.class);
    RegionEntry regionEntryMock = mock(RegionEntry.class);
    VersionStamp versionStampMock = mock(VersionStamp.class);
    when(versionStampMock.getVersionTimeStamp()).thenReturn(entryTimestamp);
    when(regionMock.getAttributes().getConcurrencyChecksEnabled()).thenReturn(true);
    when(regionEntryMock.getVersionStamp()).thenReturn(versionStampMock);
    when(entry.getRegionEntry()).thenReturn(regionEntryMock);

    // act
    GatewayQueueEvent event = eventCreator.createGatewaySenderEvent(internalCacheMock,
        (InternalRegion) regionMock, gatewaySenderMock, entry, timestamp);

    // assert
    assertThat(event).isNull();
  }

  @Test
  public void eventCreator_createGatewaySenderEventDoesNotCreateEventIfTimestampIsBeforeEntrySnapshotTimestamp() {
    // arrange
    EventCreator eventCreator = new EventCreatorImpl();
    long timestamp = System.currentTimeMillis();
    long entryTimestamp = timestamp + 1000;
    EntrySnapshot entry = mock(EntrySnapshot.class);
    VersionTag versionTagMock = mock(VersionTag.class);
    when(versionTagMock.getVersionTimeStamp()).thenReturn(entryTimestamp);
    when(entry.getVersionTag()).thenReturn(versionTagMock);

    // act
    GatewayQueueEvent event = eventCreator.createGatewaySenderEvent(internalCacheMock,
        (InternalRegion) regionMock, gatewaySenderMock, entry, timestamp);

    // assert
    assertThat(event).isNull();
  }

  private CliFunctionResult executeWanCopyRegionFunction(Object[] options,
      ThreadSleeper sleeper)
      throws BatchException70, InterruptedException {
    Region<?, ?> regionMock =
        uncheckedCast(mock(InternalRegion.class));

    RegionAttributes<?, ?> attributesMock = mock(RegionAttributes.class);
    Set<?> idsMock = mock(Set.class);
    when(idsMock.contains(anyString())).thenReturn(true);
    when(attributesMock.getGatewaySenderIds()).thenReturn(uncheckedCast(idsMock));
    when(regionMock.getCache()).thenReturn(internalCacheMock);
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

    AbstractGatewaySenderEventProcessor eventProcessorMock =
        mock(AbstractGatewaySenderEventProcessor.class);
    when(eventProcessorMock.getDispatcher()).thenReturn(dispatcherMock);
    when(gatewaySenderMock.getEventProcessor()).thenReturn(eventProcessorMock);

    InternalCache internalCacheMock = mock(InternalCache.class);
    when(internalCacheMock.getRegion(any())).thenReturn(uncheckedCast(regionMock));
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);

    WanCopyRegionFunctionDelegate function =
        new WanCopyRegionFunctionDelegate(Clock.systemDefaultZone(),
            sleeper, eventCreatorMock, 0);

    return function.wanCopyRegion(internalCacheMock, "member1", regionMock, gatewaySenderMock, 10,
        10);
  }
}
