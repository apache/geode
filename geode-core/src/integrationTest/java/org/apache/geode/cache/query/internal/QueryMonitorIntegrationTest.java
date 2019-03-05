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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Test QueryMonitor, integrated with its ScheduledThreadPoolExecutor.
 *
 * Mock DefaultQuery.
 */
public class QueryMonitorIntegrationTest {

  // query expiration duration so long that the query never expires
  private static final int NEVER_EXPIRE_MILLIS = 100000;

  // much much smaller than default maximum wait time of GeodeAwaitility
  private static final int EXPIRE_QUICK_MILLIS = 1;

  private InternalCache cache;
  private DefaultQuery query;
  private volatile QueryExecutionCanceledException queryExecutionCanceledException;

  @Before
  public void before() {
    cache = (InternalCache) new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();
    query =
        (DefaultQuery) cache.getQueryService().newQuery("SELECT DISTINCT * FROM /exampleRegion");
    queryExecutionCanceledException = null;
  }

  @Test
  public void setLowMemoryTrueCancelsQueriesImmediatelyAndCannotMonitorNewQuery() {

    QueryMonitor queryMonitor = null;

    final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
        new ScheduledThreadPoolExecutor(1);

    try {
      queryMonitor = new QueryMonitor(
          scheduledThreadPoolExecutor,
          cache,
          NEVER_EXPIRE_MILLIS);

      queryMonitor.monitorQueryThread(query);

      // Want to verify that all future queries are canceled due to low memory state, so create
      // a new query and monitor it. The query has to be created before the low memory state is set,
      // because the query service doesn't allow creating new queries in the low memory state.
      final DefaultQuery query2 =
          (DefaultQuery) cache.getQueryService().newQuery("SELECT DISTINCT * FROM /exampleRegion");

      // Set low memory and verify handling for the currently monitored exception

      queryMonitor.setLowMemory(true, 1);

      // Need to get a handle on the atomic reference because cancellation state
      // is thread local, and awaitility until() runs in a separate thread.
      final AtomicReference<CacheRuntimeException> cacheRuntimeExceptionAtomicReference =
          query.getQueryCanceledExceptionAtomicReference();

      GeodeAwaitility.await().until(() -> cacheRuntimeExceptionAtomicReference != null);

      assertThatThrownBy(QueryMonitor::throwExceptionIfQueryOnCurrentThreadIsCanceled,
          "Expected setLowMemory(true,_) to cancel query immediately, but it didn't.",
          QueryExecutionCanceledException.class);

      // Need to make query monitor effectively final for use in lambda. We expect this to throw
      // because we cannot monitor a new query once we are in a low memory state.
      final QueryMonitor finalQueryMonitor = queryMonitor;
      assertThatThrownBy(() -> finalQueryMonitor.monitorQueryThread(query2))
          .isExactlyInstanceOf(QueryExecutionLowMemoryException.class);
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
  public void monitorQueryThreadCancelsLongRunningQueriesAndSetsExceptionAndThrowsException() {

    final QueryMonitor queryMonitor = new QueryMonitor(
        new ScheduledThreadPoolExecutor(1),
        cache,
        EXPIRE_QUICK_MILLIS);

    startQueryThread(queryMonitor, query);

    GeodeAwaitility.await().until(() -> queryExecutionCanceledException != null);

    assertThat(queryExecutionCanceledException).isNotNull();
  }

  @Test
  public void monitorQueryThreadThenStopMonitoringNoRemainingCancellationTasksRunning() {
    cache = (InternalCache) new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();
    query =
        (DefaultQuery) cache.getQueryService().newQuery("SELECT DISTINCT * FROM /exampleRegion");

    final ScheduledThreadPoolExecutor queryMonitorExecutor = new ScheduledThreadPoolExecutor(1);

    final QueryMonitor queryMonitor = new QueryMonitor(
        queryMonitorExecutor,
        cache,
        NEVER_EXPIRE_MILLIS);

    final Thread firstClientQueryThread = new Thread(() -> {
      queryMonitor.monitorQueryThread(query);

      final Thread secondClientQueryThread = new Thread(() -> {
        queryMonitor.monitorQueryThread(query);
        queryMonitor.stopMonitoringQueryThread(query);
      });

      secondClientQueryThread.start();
      try {
        secondClientQueryThread.join();
      } catch (final InterruptedException ex) {
        Assert.fail("Unexpected exception while executing query. Details:\n"
            + ExceptionUtils.getStackTrace(ex));
      }

      queryMonitor.stopMonitoringQueryThread(query);
    });

    firstClientQueryThread.start();

    // Both cancellation tasks should have been removed upon stopping monitoring the queries on
    // each thread, so the task queue size should be 0
    GeodeAwaitility.await().until(() -> queryMonitorExecutor.getQueue().size() == 0);
  }

  @Test
  public void monitorQueryThreadTimesOutButCancellationStateIsThreadLocal() {
    cache = (InternalCache) new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();
    query =
        (DefaultQuery) cache.getQueryService().newQuery("SELECT DISTINCT * FROM /exampleRegion");

    final ScheduledThreadPoolExecutor queryMonitorExecutor = new ScheduledThreadPoolExecutor(1);

    final QueryMonitor queryMonitor = new QueryMonitor(
        queryMonitorExecutor,
        cache,
        EXPIRE_QUICK_MILLIS);

    startQueryThread(queryMonitor, query);

    GeodeAwaitility.await().until(() -> queryExecutionCanceledException != null);

    // The cancellation state should be local to the query thread in startQueryThread, so we should
    // be able to execute subsequent queries using the same query object. We are verifying that
    // the cancellation state on the main test thread is still not set here.
    assertThat(query.isCanceled()).isFalse();
  }

  private void startQueryThread(final QueryMonitor queryMonitor,
      final DefaultQuery query) {

    final Thread queryThread = new Thread(() -> {
      queryMonitor.monitorQueryThread(query);

      while (true) {
        try {
          QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCanceled();
          Thread.sleep(5 * EXPIRE_QUICK_MILLIS);
        } catch (final QueryExecutionCanceledException e) {
          queryExecutionCanceledException = e;
          assertThat(query.isCanceled()).isTrue();
          break;
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("Simulated query thread unexpectedly interrupted.");
        }
      }
    });

    queryThread.start();
  }
}
