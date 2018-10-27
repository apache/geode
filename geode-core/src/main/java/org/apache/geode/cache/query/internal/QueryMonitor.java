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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;

/**
 * QueryMonitor class, monitors the query execution time. Instantiated based on the system property
 * MAX_QUERY_EXECUTION_TIME. At most there will be one query monitor-thread that cancels the long
 * running queries.
 *
 * The queries to be monitored is added into the ordered queue, ordered based on its start/arrival
 * time. The first one in the Queue is the older query that will be canceled first.
 *
 * The QueryMonitor cancels a query-execution thread if its taking more than the max time.
 *
 * @since GemFire 6.0
 */
public class QueryMonitor {
  private static final Logger logger = LogService.getLogger();

  private final InternalCache cache;

  private final long defaultMaxQueryExecutionTime;

  private final ScheduledThreadPoolExecutorFactory executorFactory;

  private volatile ScheduledThreadPoolExecutor executor;

  private volatile boolean cancellingDueToLowMemory;

  // Variables for cancelling queries due to low memory
  private static volatile Boolean LOW_MEMORY = Boolean.FALSE;

  private static volatile long LOW_MEMORY_USED_BYTES = 0;

  @FunctionalInterface
  public interface ScheduledThreadPoolExecutorFactory {
    ScheduledThreadPoolExecutor create();
  }

  public QueryMonitor(final ScheduledThreadPoolExecutorFactory executorFactory,
      final InternalCache cache,
      final long defaultMaxQueryExecutionTime) {
    this.cache = cache;
    this.defaultMaxQueryExecutionTime = defaultMaxQueryExecutionTime;

    this.executorFactory = executorFactory;
    this.executor = executorFactory.create();
    this.executor.setRemoveOnCancelPolicy(true);
  }

  /**
   * Add query to be monitored.
   *
   * Must not be called from a thread that is not the query thread,
   * because this class uses a ThreadLocal on the query thread!
   *
   * @param query Query.
   */
  public void monitorQueryThread(final DefaultQuery query) {
    monitorQueryThread(query, defaultMaxQueryExecutionTime);
  }

  /**
   * Each query can have a different maxQueryExecution time. Make this method public to
   * expose that feature to callers.
   *
   * Must not be called from a thread that is not the query thread,
   * because this class uses a ThreadLocal on the query thread!
   */
  private void monitorQueryThread(final DefaultQuery query,
      final long maxQueryExecutionTime) {

    // cq query is not monitored
    if (query.isCqQuery()) {
      return;
    }

    if (LOW_MEMORY) {
      final QueryExecutionLowMemoryException lowMemoryException = createLowMemoryException();
      query.setQueryCanceledException(lowMemoryException);
      throw lowMemoryException;
    }

    query.setExpirationTask(scheduleExpirationTask(query, maxQueryExecutionTime));

    if (logger.isDebugEnabled()) {
      final Thread queryThread = Thread.currentThread();
      logger.debug(
          "Adding thread to QueryMonitor. QueryMonitor size is: {}, Thread (id): {}, Query: {}, Thread is : {}",
          executor.getQueue().size(), queryThread.getId(), query.getQueryString(),
          queryThread);
    }
  }

  /**
   * Stops monitoring the query. Removes the passed thread from QueryMonitor queue.
   *
   * Must not be called from a thread that is not the query thread,
   * because this class uses a ThreadLocal on the query thread!
   */
  public void stopMonitoringQueryThread(final DefaultQuery query) {
    query.getExpirationTask().ifPresent(task -> task.cancel(false));

    if (logger.isDebugEnabled()) {
      final Thread queryThread = Thread.currentThread();
      logger.debug(
          "Query completed before expiration. QueryMonitor size is: {}, Thread ID is: {},  Thread is: {}",
          executor.getQueue().size(), queryThread.getId(), queryThread);
    }
  }

  /**
   * This method is called to check if the query execution is canceled. The QueryMonitor cancels the
   * query execution if it takes more than the max query execution time set or in low memory
   * situations where critical heap percentage has been set on the resource manager
   *
   * The max query execution time is set using the system property
   * gemfire.Cache.MAX_QUERY_EXECUTION_TIME
   */
  public static void throwExceptionIfQueryOnCurrentThreadIsCancelled() {
    if (DefaultQuery.QueryCanceled.get().get()) {
      throw new QueryExecutionCanceledException();
    }
  }

  /**
   * Stops query monitoring. Makes this QueryMonitor unusable for further monitoring.
   */
  public void stopMonitoring() {
    executor.shutdownNow();
  }

  /**
   * Assumes LOW_MEMORY will only be set if query monitor is enabled
   */
  public static boolean isLowMemory() {
    return LOW_MEMORY;
  }

  public static long getMemoryUsedDuringLowMemory() {
    return LOW_MEMORY_USED_BYTES;
  }

  public void setLowMemory(boolean lowMemory, long usedBytes) {
    if (cache != null && !cache.isQueryMonitorDisabledForLowMemory()) {
      QueryMonitor.LOW_MEMORY_USED_BYTES = usedBytes;
      QueryMonitor.LOW_MEMORY = lowMemory;
    }
  }

  public synchronized void cancelAllQueriesDueToMemory() {

    /*
     * An expiration task is actually dual-purpose. Its primary purpose is to cancel
     * a query if the query runs too long. Alternately, if this flag
     * (cancellingDueToLowMemory) is set, the expiration task will cancel the query
     * due to low memory.
     */
    cancellingDueToLowMemory = true;

    try {
      executor.shutdown();
      try {
        final boolean terminatedAllQueries = executor.awaitTermination(1, TimeUnit.SECONDS);
        if (!terminatedAllQueries)
          logger.info("When cancelling queries due to low memory, failed to cancel all queries.");
      } catch (final InterruptedException e) {
        logger.info("When cancelling queries due to low memory, scheduler thread was interrupted.");
        // we rec'd an InterruptedException and are not re-throwing it so we have to interrupt()
        Thread.currentThread().interrupt();
      }
    } finally {
      cancellingDueToLowMemory = false;
      // executor is volatile so other threads will see this modification
      executor = executorFactory.create();
    }

  }

  private ScheduledFuture<?> scheduleExpirationTask(final DefaultQuery query,
      final long timeLimitMillis) {

    // make ThreadLocal QueryCanceled, available to closure
    final AtomicBoolean queryCanceledThreadLocal =
        DefaultQuery.QueryCanceled.get();

    return executor.schedule(() -> {
      final CacheRuntimeException exception = cancellingDueToLowMemory ? createLowMemoryException()
          : createExpirationException(timeLimitMillis);

      query.setQueryCanceledException(exception);
      queryCanceledThreadLocal.set(true);

      logger.info(exception.getMessage());
    }, timeLimitMillis, TimeUnit.MILLISECONDS);
  }

  private QueryExecutionTimeoutException createExpirationException(long timeLimitMillis) {
    return new QueryExecutionTimeoutException(
        String.format(
            "Query execution cancelled after exceeding max execution time %sms.",
            timeLimitMillis));
  }

  private QueryExecutionLowMemoryException createLowMemoryException() {
    return new QueryExecutionLowMemoryException(
        String.format(
            "Query execution canceled due to memory threshold crossed in system, memory used: %s bytes.",
            LOW_MEMORY_USED_BYTES));
  }
}
