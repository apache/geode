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
package org.apache.geode.cache.query.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.internal.cache.InternalCache;

/**
 * although max_execution_time is set as 10ms, the monitor thread can sleep more than the specified
 * time, so query will be cancelled at un-deterministic time after 10ms. We cannot assert on
 * specific time at which the query will be cancelled. We can only assert that the query will be
 * cancelled at one point after 10ms.
 */
public class QueryMonitorTest {

  private QueryMonitor monitor;
  private long max_execution_time = 5;
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
  private ArgumentCaptor<Runnable> captor;

  @Before
  public void setUp() {
    scheduledThreadPoolExecutor = mock(ScheduledThreadPoolExecutor.class);
    when(scheduledThreadPoolExecutor.getQueue()).thenReturn(new ArrayBlockingQueue<>(1));
    monitor = new QueryMonitor(scheduledThreadPoolExecutor, mock(InternalCache.class),
        max_execution_time);
    captor = ArgumentCaptor.forClass(Runnable.class);
  }

  @After
  public void afterClass() {
    /*
     * If the query cancelation task is run, it will set the isCanceled
     * thread local on the query to true.
     *
     * We need to clean up that state between tests because they can run on this same thread.
     * In production, this cleanup is done in DefaultQuery after the query executes.
     */
    ExecutionContext.isCanceled.get().set(false);
    monitor.setLowMemory(false, 100);
  }

  @Test
  public void monitorQueryThreadCqQueryIsNotMonitored() {
    final ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.isCqQueryContext()).thenReturn(true);
    monitor.monitorQueryExecution(executionContext);

    // Verify that the expiration task was not scheduled for the CQ executionContext
    Mockito.verify(scheduledThreadPoolExecutor, never()).schedule(captor.capture(), anyLong(),
        isA(TimeUnit.class));
  }

  @Test
  public void monitorQueryThreadLowMemoryExceptionThrown() {
    final ExecutionContext executionContext = mock(ExecutionContext.class);
    monitor.setLowMemory(true, 100);

    assertThatThrownBy(() -> monitor.monitorQueryExecution(executionContext))
        .isExactlyInstanceOf(QueryExecutionLowMemoryException.class);
  }

  @Test
  public void monitorQueryThreadExpirationTaskScheduled() {
    final ExecutionContext executionContext = mock(ExecutionContext.class);

    monitor.monitorQueryExecution(executionContext);
    Mockito.verify(scheduledThreadPoolExecutor, times(1)).schedule(captor.capture(), anyLong(),
        isA(TimeUnit.class));
    captor.getValue().run();

    Mockito.verify(executionContext, times(1))
        .setQueryCanceledException(isA(QueryExecutionTimeoutException.class));
    assertThatThrownBy(QueryMonitor::throwExceptionIfQueryOnCurrentThreadIsCanceled)
        .isExactlyInstanceOf(QueryExecutionCanceledException.class);
  }

  @Test
  public void setLowMemoryTrueThenFalseAllowsSubsequentMonitoring() {
    monitor.setLowMemory(true, 1);
    monitor.setLowMemory(false, 1);
    /*
     * Verify we can still monitor and expire a query after
     * cancelling all queries due to low memory.
     */
    monitorQueryThreadExpirationTaskScheduled();
  }
}
