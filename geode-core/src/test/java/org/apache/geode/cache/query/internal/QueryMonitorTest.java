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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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

  private InternalCache cache;
  private QueryMonitor monitor;
  private long max_execution_time = 5;
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
  private ArgumentCaptor<Runnable> captor;

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    scheduledThreadPoolExecutor = mock(ScheduledThreadPoolExecutor.class);
    monitor = new QueryMonitor(scheduledThreadPoolExecutor, cache, max_execution_time);
    captor = ArgumentCaptor.forClass(Runnable.class);
  }

  @After
  public void afterClass() {
    // cleanup the thread local of the queryCancelled status
    DefaultQuery query = mock(DefaultQuery.class);
    doReturn(Optional.empty()).when(query).getExpirationTask();
    when(query.getQueryCompletedForMonitoring()).thenReturn(new boolean[] {true});
    monitor.stopMonitoringQueryThread(Thread.currentThread(), query);
    monitor.setLowMemory(false, 100);
  }

  @Test
  public void monitorQueryThreadCqQueryIsNotMonitored() {
    DefaultQuery query = mock(DefaultQuery.class);
    when(query.isCqQuery()).thenReturn(true);
    monitor.monitorQueryThread(mock(Thread.class), query);

    //Verify that the expiration task was not scheduled for the CQ query
    Mockito.verify(scheduledThreadPoolExecutor, never()).schedule(captor.capture(), anyLong(), isA(TimeUnit.class));
  }

  @Test
  public void monitorQueryThreadLowMemoryExceptionThrown() {
    DefaultQuery query = mock(DefaultQuery.class);
    Thread queryThread = mock(Thread.class);
    monitor.setLowMemory(true, 100);
    assertThatThrownBy(() -> monitor.monitorQueryThread(queryThread, query))
        .isExactlyInstanceOf(QueryExecutionLowMemoryException.class);
  }

  @Test
  public void monitorQueryThreadExpirationTaskScheduled() {
    DefaultQuery query = mock(DefaultQuery.class);
    doReturn(new boolean[]{false}).when(query).getQueryCompletedForMonitoring();
    Thread queryThread = mock(Thread.class);
    monitor.monitorQueryThread(queryThread, query);
    Mockito.verify(scheduledThreadPoolExecutor, times(1)).schedule(captor.capture(), anyLong(), isA(TimeUnit.class));
    captor.getValue().run();
    Mockito.verify(query, times(1)).setCanceled(isA(QueryExecutionTimeoutException.class));
    assertThatThrownBy(() -> QueryMonitor.isQueryExecutionCanceled()).isExactlyInstanceOf(QueryExecutionCanceledException.class);
  }

  private Thread createQueryExecutionThread(int i) {
    Thread thread = new Thread(() -> {
      // make sure the threadlocal variable is updated
      await()
          .untilAsserted(() -> assertThatCode(() -> QueryMonitor.isQueryExecutionCanceled())
              .doesNotThrowAnyException());
    });
    thread.setName("query" + i);
    return thread;
  }
}
