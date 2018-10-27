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

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Test QueryMonitor, integrated with its ScheduledThreadPoolExecutor.
 *
 * Mock DefaultQuery.
 */
public class QueryMonitorJUnitTest {

  // query expiration duration so long that the query never expires
  public static final int NEVER_EXPIRE_MILLIS = 100000;

  // much much smaller than default maximum wait time of GeodeAwaitility
  public static final int EXPIRE_QUICK_MILLIS = 1;

  private InternalCache cache;
  private DefaultQuery query;
  private volatile CacheRuntimeException queryCanceledException;

  @Before
  public void before() {
    cache = mock(InternalCache.class);
    query = mock(DefaultQuery.class);
    queryCanceledException = null;
  }

  @Test
  public void cancelAllQueriesDueToMemoryCancelsQueriesImmediately() {

    final QueryMonitor queryMonitor = new QueryMonitor(
        () -> new ScheduledThreadPoolExecutor(1),
        cache,
        NEVER_EXPIRE_MILLIS);

    queryMonitor.monitorQueryThread(query);

    queryMonitor.cancelAllQueriesDueToMemory();

    try {
      QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCancelled();
      throw new AssertionError("Expected cancelAllQueriesDueToMemory() to cancel query immediately, but it didn't.");
    } catch (final QueryExecutionCanceledException _ignored) {
      // expected
    }
  }

  @Test
  public void monitorQueryThreadCancelsLongRunningQueriesAndSetsException() throws InterruptedException {

    final QueryMonitor queryMonitor = new QueryMonitor(
        () -> new ScheduledThreadPoolExecutor(1),
        cache,
        EXPIRE_QUICK_MILLIS);

    final Answer<Void> processSetQueryCanceledException = invocation -> {
      final Object[] args = invocation.getArguments();
      if (args[0] instanceof CacheRuntimeException) {
        queryCanceledException = (CacheRuntimeException)args[0];
      } else {
        throw new AssertionError("setQueryCanceledException() received argument that wasn't a CacheRuntimeException");
      }
      return null;
    };

    doAnswer(processSetQueryCanceledException).when(query)
        .setQueryCanceledException(any(CacheRuntimeException.class));

    startQueryThread(queryMonitor, query);

    GeodeAwaitility.await().until(()->queryCanceledException != null);

    assertThat(queryCanceledException).hasMessageContaining("cancelled after exceeding max execution time");
  }

  private void startQueryThread(final QueryMonitor queryMonitor,
                                final DefaultQuery query) {
    final Thread queryThread = new Thread(() -> {
      queryMonitor.monitorQueryThread(query);
      while(true) {
        try {
          QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCancelled();
          Thread.sleep(5 * EXPIRE_QUICK_MILLIS);
        } catch (final QueryExecutionCanceledException _ignored) {
          break;
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("simulated query thread unexpectedly interrupted");
        }
      }
    });
    queryThread.start();
  }
}
