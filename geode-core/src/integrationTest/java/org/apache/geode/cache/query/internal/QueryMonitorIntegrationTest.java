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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Test QueryMonitor, integrated with its ScheduledThreadPoolExecutor.
 *
 * Mock DefaultQuery.
 */
public class QueryMonitorIntegrationTest {

  // executionContext expiration duration so long that the executionContext never expires
  private static final int NEVER_EXPIRE_MILLIS = 100000;

  // much much smaller than default maximum wait time of GeodeAwaitility
  private static final int EXPIRE_QUICK_MILLIS = 1;

  private InternalCache cache;
  private ExecutionContext executionContext;
  private volatile CacheRuntimeException cacheRuntimeException;
  private volatile QueryExecutionCanceledException queryExecutionCanceledException;

  @Before
  public void before() {
    cache = mock(InternalCache.class);
    executionContext = mock(ExecutionContext.class);
    cacheRuntimeException = null;
    queryExecutionCanceledException = null;
  }

  @Test
  public void setLowMemoryTrueCancelsQueriesImmediately() {

    QueryMonitor queryMonitor = null;

    ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
        new ScheduledThreadPoolExecutor(1);

    try {
      queryMonitor = new QueryMonitor(
          scheduledThreadPoolExecutor,
          cache,
          NEVER_EXPIRE_MILLIS);

      queryMonitor.monitorQueryExecution(executionContext);

      queryMonitor.setLowMemory(true, 1);

      verify(executionContext, times(1))
          .setQueryCanceledException(any(QueryExecutionLowMemoryException.class));

      assertThatThrownBy(QueryMonitor::throwExceptionIfQueryOnCurrentThreadIsCanceled,
          "Expected setLowMemory(true,_) to cancel executionContext immediately, but it didn't.",
          QueryExecutionCanceledException.class);
    } finally {
      if (queryMonitor != null) {
        /*
         * Setting the low-memory state (above) sets it globally. If we fail to reset it here,
         * then subsequent tests, e.g. if we run this test class more than once in succession
         * in the same JVM, as the Geode "stress" test does, will give unexpected results.
         */
        queryMonitor.setLowMemory(false, 1);
      }
    }

    assertThat(scheduledThreadPoolExecutor.getQueue().size()).isZero();
  }

  @Test
  public void monitorQueryExecutionCancelsLongRunningQueriesAndSetsExceptionAndThrowsException() {

    QueryMonitor queryMonitor = new QueryMonitor(
        new ScheduledThreadPoolExecutor(1),
        cache,
        EXPIRE_QUICK_MILLIS);

    final Answer<Void> processSetQueryCanceledException = invocation -> {
      final Object[] args = invocation.getArguments();
      if (args[0] instanceof CacheRuntimeException) {
        cacheRuntimeException = (CacheRuntimeException) args[0];
      } else {
        throw new AssertionError(
            "setQueryCanceledException() received argument that wasn't a CacheRuntimeException.");
      }
      return null;
    };

    doAnswer(processSetQueryCanceledException).when(executionContext)
        .setQueryCanceledException(any(CacheRuntimeException.class));

    startQueryThread(queryMonitor, executionContext);

    GeodeAwaitility.await().until(() -> cacheRuntimeException != null);

    assertThat(cacheRuntimeException)
        .hasMessageContaining("canceled after exceeding max execution time");

    assertThat(queryExecutionCanceledException).isNotNull();
  }

  @Test
  public void monitorMultipleQueryExecutionsThenStopMonitoringNoRemainingCancellationTasksRunning() {
    cache = (InternalCache) new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();
    final DefaultQuery query =
        (DefaultQuery) cache.getQueryService().newQuery("SELECT DISTINCT * FROM /exampleRegion");
    executionContext = new QueryExecutionContext(null, cache, query);
    final ExecutionContext executionContext2 = new QueryExecutionContext(null, cache, query);

    final ScheduledThreadPoolExecutor queryMonitorExecutor = new ScheduledThreadPoolExecutor(1);

    final QueryMonitor queryMonitor = new QueryMonitor(
        queryMonitorExecutor,
        cache,
        NEVER_EXPIRE_MILLIS);

    // We want to ensure isolation of cancellation tasks for different query threads/executions.
    // Here we ensure that if we monitor/unmonitor two executions in different threads that
    // both cancellation tasks are removed from the executor queue.
    final Thread firstClientQueryThread = new Thread(() -> {
      queryMonitor.monitorQueryExecution(executionContext);

      final Thread secondClientQueryThread = new Thread(() -> {
        queryMonitor.monitorQueryExecution(executionContext2);
        queryMonitor.stopMonitoringQueryExecution(executionContext2);
      });

      secondClientQueryThread.start();
      try {
        secondClientQueryThread.join();
      } catch (final InterruptedException ex) {
        Assert.fail("Unexpected exception while executing query. Details:\n"
            + ExceptionUtils.getStackTrace(ex));
      }

      queryMonitor.stopMonitoringQueryExecution(executionContext);
    });

    firstClientQueryThread.start();

    // Both cancellation tasks should have been removed upon stopping monitoring the queries on
    // each thread, so the task queue size should be 0
    GeodeAwaitility.await().until(() -> queryMonitorExecutor.getQueue().size() == 0);
  }

  private void startQueryThread(final QueryMonitor queryMonitor,
      final ExecutionContext executionContext) {

    final Thread queryThread = new Thread(() -> {
      queryMonitor.monitorQueryExecution(executionContext);

      while (true) {
        try {
          QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCanceled();
          Thread.sleep(5 * EXPIRE_QUICK_MILLIS);
        } catch (final QueryExecutionCanceledException e) {
          queryExecutionCanceledException = e;
          break;
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("Simulated executionContext thread unexpectedly interrupted.");
        }
      }
    });

    queryThread.start();
  }
}
