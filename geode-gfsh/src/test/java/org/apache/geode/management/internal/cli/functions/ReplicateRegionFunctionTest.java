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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.pooling.PooledConnection;
import org.apache.geode.cache.execute.FunctionContext;
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
  private FunctionContext<Object[]> contextMock = mock(FunctionContext.class);

  @SuppressWarnings("unchecked")
  private Region<Object, Object> regionMock = mock(InternalRegion.class);

  @SuppressWarnings("unchecked")
  private Region.Entry<String, String> entryMock = mock(Region.Entry.class);

  @Before
  public void setUp() throws InterruptedException {
    clockMock = mock(Clock.class);
    threadSleeperMock = mock(ReplicateRegionFunction.ThreadSleeper.class);
    doNothing().when(threadSleeperMock).millis(anyLong());
    internalCacheMock = mock(InternalCache.class);
    gatewaySenderMock = mock(AbstractGatewaySender.class, RETURNS_DEEP_STUBS);
    poolMock = mock(PoolImpl.class);
    connectionMock = mock(PooledConnection.class);
    dispatcherMock = mock(GatewaySenderEventDispatcher.class);
    rrf = new ReplicateRegionFunction();
    rrf.setClock(clockMock);
    rrf.setThreadSleeper(threadSleeperMock);
    startTime = System.currentTimeMillis();
  }

  @Test
  public void doPostSendBatchActions_DoNothingIfBatchIsIncomplete()
      throws InterruptedException {
    rrf.doPostSendBatchActions(startTime, 5, 1L);
    verify(threadSleeperMock, never()).millis(anyLong());
  }

  @Test
  public void doPostSendBatchActions_DoNotSleepIfBatchIsCompleteAndMaxRateIsZero()
      throws InterruptedException {
    rrf.doPostSendBatchActions(startTime, entries, 0);
    verify(threadSleeperMock, never()).millis(anyLong());
  }

  @Test
  public void doPostSendBatchActions_SleepIfElapsedTimeIsZero()
      throws InterruptedException {
    long maxRate = 100;
    long elapsedTime = 0L;
    long expectedMsToSleep = 250L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    rrf.doPostSendBatchActions(startTime, entries, maxRate);
    verify(threadSleeperMock, times(1)).millis(expectedMsToSleep);
  }

  @Test
  public void doPostSendBatchActions_DoNotSleepIfMaxRateNotReached()
      throws InterruptedException {
    long maxRate = 10000;
    long elapsedTime = 100L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    rrf.doPostSendBatchActions(startTime, entries, maxRate);
    verify(threadSleeperMock, never()).millis(anyLong());
  }

  @Test
  public void doPostSendBatchActions_SleepIfMaxRateReached()
      throws InterruptedException {
    long maxRate = 100;
    long elapsedTime = 100L;
    long expectedMsToSleep = 150L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    rrf.doPostSendBatchActions(startTime, entries, maxRate);
    verify(threadSleeperMock, times(1)).millis(expectedMsToSleep);
  }

  @Test
  public void doPostSendBatchActions_DoNotSleepIfReplicatedEntriesIsZero()
      throws InterruptedException {
    long maxRate = 100;
    rrf.doPostSendBatchActions(startTime, 0, maxRate);
    verify(threadSleeperMock, never()).millis(anyLong());
  }

  @Test
  public void doPostSendBatchActions_SleepForZeroIfReplicatedEntriesIsZeroAndElapsedTimeIsZero()
      throws InterruptedException {
    long maxRate = 100;
    long elapsedTime = 0L;
    when(clockMock.millis()).thenReturn(startTime + elapsedTime);
    rrf.doPostSendBatchActions(startTime, 0, maxRate);
    verify(threadSleeperMock, times(1)).millis(0L);
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
  public void replicateRegion_verifyOutputWhenNoPoolAvailable() {
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(null);
    CliFunctionResult result =
        rrf.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("No connection pool available towards receiver");
  }

  @Test
  public void replicateRegion_verifyOutputWhenNoConnectionAvailable() {
    when(poolMock.acquireConnection()).thenReturn(null);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    CliFunctionResult result =
        rrf.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("No connection available towards receiver");
  }

  @Test
  public void replicateRegion_verifyOutputWhenExceptionWhileSendingBatch() throws BatchException70 {

    ReplicateRegionFunction rrfSpy = spy(rrf);
    BatchException70 exceptionWhenSendingBatch =
        new BatchException70("My batch exception", new Exception(), 0, 0);
    when(((AbstractGatewaySender) gatewaySenderMock).getProxy()).thenReturn(poolMock);
    when(poolMock.acquireConnection()).thenReturn(connectionMock);
    when(contextMock.getCache()).thenReturn(internalCacheMock);
    when(((AbstractGatewaySender) gatewaySenderMock).getEventProcessor().getDispatcher())
        .thenReturn(dispatcherMock);
    Set<Region.Entry<String, String>> entries = new HashSet<Region.Entry<String, String>>();
    entries.add(entryMock);
    doReturn(entries).when(regionMock).entrySet();
    doReturn(new ArrayList<>()).when(rrfSpy).createBatch(any(), any(), anyInt(), any(), any());
    doThrow(exceptionWhenSendingBatch).when(dispatcherMock).sendBatch(anyList(), any(), any(),
        anyInt());

    CliFunctionResult result =
        rrfSpy.replicateRegion(contextMock, regionMock, gatewaySenderMock, 1, 10);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Error (My batch exception) in operation after having replicated 0 entries");
  }
}
