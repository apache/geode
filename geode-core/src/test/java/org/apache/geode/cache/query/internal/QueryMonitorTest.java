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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.internal.cache.InternalCache;

/**
 * although max_execution_time is set as 10ms, the monitor thread can sleep more than the specified
 * time, so query will be cancelled at un-deterministic time after 10ms. We cannot assert on
 * specific time at which the query will be cancelled. We can only assert that the query will be
 * cancelled at one point after 10ms.
 */
public class QueryMonitorTest {

  private static InternalCache cache;
  private static QueryMonitor monitor;
  private static long max_execution_time = 5;

  @BeforeClass
  public static void setUp() {
    cache = mock(InternalCache.class);
    monitor = new QueryMonitor(cache, max_execution_time);
    Thread monitorThread = new Thread(() -> monitor.run(), "query monitor thread");
    monitorThread.setDaemon(true);
    monitorThread.start();
  }

  @AfterClass
  public static void afterClass() {
    // cleanup the thread local of the queryCancelled status
    DefaultQuery query = mock(DefaultQuery.class);
    when(query.getQueryCompletedForMonitoring()).thenReturn(new boolean[] {true});
    monitor.stopMonitoringQueryThread(Thread.currentThread(), query);
  }

  @Test
  public void queryIsCancelled() {
    List<DefaultQuery> queries = new ArrayList<>();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      DefaultQuery query = new DefaultQuery("query" + i, cache, false);
      queries.add(query);
      Thread queryExecutionThread = createQueryExecutionThread(i);
      threads.add(queryExecutionThread);
      monitor.monitorQueryThread(queryExecutionThread, query);
    }

    for (DefaultQuery query : queries) {
      // make sure the isCancelled flag in Query is set correctly
      Awaitility.await().until(() -> query.isCanceled());
    }
    Awaitility.await().until(() -> monitor.getQueryMonitorThreadCount() == 0);
    // make sure all thread died
    for (Thread thread : threads) {
      Awaitility.await().until(() -> !thread.isAlive());
    }
  }

  @Test
  public void cqQueryIsNotMonitored() {
    DefaultQuery query = mock(DefaultQuery.class);
    when(query.isCqQuery()).thenReturn(true);
    monitor.monitorQueryThread(mock(Thread.class), query);
    assertThat(monitor.getQueryMonitorThreadCount()).isEqualTo(0);
  }

  private Thread createQueryExecutionThread(int i) {
    Thread thread = new Thread(() -> {
      // make sure the threadlocal variable is updated
      Awaitility.await().until(() -> QueryMonitor.isQueryExecutionCanceled());
    });
    thread.setName("query" + i);
    return thread;
  }

}
