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

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;

/**
 * {@link QueryMonitor} class, monitors the query execution time. In typical usage, the maximum
 * query execution time might be set (upon construction) via the system property
 * {@link GemFireCacheImpl#MAX_QUERY_EXECUTION_TIME}. The number of threads allocated to query
 * monitoring is determined by the instance of {@link ScheduledThreadPoolExecutorFactory} passed
 * to the constructor.
 *
 * This class supports a low-memory mode, established by {@link #setLowMemory(boolean, long)}. \
 * In that mode, any attempt to monitor a (new) query will throw an exception.
 *
 * The {@link #monitorQueryThread(DefaultQuery)} method initiates monitoring of a query.
 * {@link #stopMonitoringQueryThread(DefaultQuery)} stops monitoring a query.
 *
 * If the {@link QueryMonitor} determines a query needs to be canceled: either because it is
 * taking too long, or because memory is running low, it does two things:
 *
 * <ul>
 * <li>registers an exception on the query via
 * {@link DefaultQuery#setQueryCanceledException(CacheRuntimeException)}</li>
 * <li>sets the {@link DefaultQuery#queryCanceled} thread-local variable to {@code true}
 * so that subsequent calls to {@link #throwExceptionIfQueryOnCurrentThreadIsCanceled()}
 * will throw an exception</li>
 * </ul>
 *
 * Code outside this class, that wishes to participate in cooperative cancelation of queries
 * calls {@link #throwExceptionIfQueryOnCurrentThreadIsCanceled()} at various yield points.
 * In catch blocks, {@link DefaultQuery#getQueryCanceledException()} is interrogated to learn
 * the cancelation cause.
 *
 * @since GemFire 6.0
 */
public class QueryMonitor {
  private static final Logger logger = LogService.getLogger();

  private final InternalCache cache;

  private final long defaultMaxQueryExecutionTime;

  private final ScheduledThreadPoolExecutorFactory executorFactory;

  private volatile ScheduledThreadPoolExecutor executor;

  private volatile boolean cancelingDueToLowMemory;

  private static volatile Boolean LOW_MEMORY = Boolean.FALSE;

  private static volatile long LOW_MEMORY_USED_BYTES = 0;

  @FunctionalInterface
  public interface ScheduledThreadPoolExecutorFactory {
    ScheduledThreadPoolExecutor create();
  }

  /**
   * This class will call {@link ScheduledThreadPoolExecutor#setRemoveOnCancelPolicy(boolean)}
   * on {@link ScheduledThreadPoolExecutor} instances returned by the
   * {@link ScheduledThreadPoolExecutorFactory} to set that property to {@code true}.
   *
   * The default behavior of a {@link ScheduledThreadPoolExecutor} is to keep canceled
   * tasks in the queue, relying on the timeout processing loop to remove them
   * when their time is up. That behaviour would result in tasks for completed
   * queries to remain in the queue until their timeout deadline was reached,
   * resulting in queue growth.
   *
   * Setting the remove-on-cancel-policy to {@code true} changes that behavior so tasks are
   * removed immediately upon cancelation (via {@link #stopMonitoringQueryThread(DefaultQuery)}).
   *
   * @param executorFactory is called to construct the initial executor. It's called subsequently
   *        every time the QueryMonitor moves out of the low-memory state, to create a new executor.
   * @param cache is interrogated via {@link InternalCache#isQueryMonitorDisabledForLowMemory} at
   *        each low-memory state change
   * @param defaultMaxQueryExecutionTime is the maximum time, in milliseconds, that any query
   *        is allowed to run
   */
  public QueryMonitor(final ScheduledThreadPoolExecutorFactory executorFactory,
      final InternalCache cache,
      final long defaultMaxQueryExecutionTime) {
    Objects.requireNonNull(executorFactory);
    Objects.requireNonNull(cache);

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

    query.setCancelationTask(scheduleCancelationTask(query, maxQueryExecutionTime));

    if (logger.isDebugEnabled()) {
      logDebug(query, "Adding thread to QueryMonitor.");
    }
  }

  /**
   * Stops monitoring the query.
   *
   * Must not be called from a thread that is not the query thread,
   * because this class uses a ThreadLocal on the query thread!
   */
  public void stopMonitoringQueryThread(final DefaultQuery query) {
    query.getCancelationTask().ifPresent(task -> task.cancel(false));

    if (logger.isDebugEnabled()) {
      logDebug(query, "Query completed before cancelation.");
    }
  }

  /**
   * Throw an exception if the query has been canceled. The {@link QueryMonitor} cancels the
   * query if it takes more than the max query execution time or in low memory situations where
   * critical heap percentage has been set on the resource manager.
   *
   * @throws QueryExecutionCanceledException if the query has been canceled
   */
  public static void throwExceptionIfQueryOnCurrentThreadIsCanceled() {
    if (DefaultQuery.queryCanceled.get().get()) {
      throw new QueryExecutionCanceledException();
    }
  }

  /**
   * Stops query monitoring. Makes this {@link QueryMonitor} unusable for further monitoring.
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

  /**
   * Caller should not call this method concurrently from multiple threads. Doing so can
   * result in lost low memory state updates due to lock unfairness.
   */
  public synchronized void setLowMemory(final boolean isLowMemory, final long usedBytes) {
    if (!cache.isQueryMonitorDisabledForLowMemory()) {
      QueryMonitor.LOW_MEMORY_USED_BYTES = usedBytes;
      final boolean memoryStateChanged = isLowMemory != QueryMonitor.LOW_MEMORY;
      if (memoryStateChanged) {
        if (isLowMemory) {
          cancelAllQueriesDueToMemory();
        } else {
          /*
           * Executor was shut down and made permanently unusable when we went into
           * the low-memory state. We have to make a new executor now that we're monitoring
           * queries again.
           */
          executor = executorFactory.create();
        }
      }
      QueryMonitor.LOW_MEMORY = isLowMemory;
    }
  }

  /**
   * Stop accepting new monitoring requests. Run all cancelation tasks with
   * {@link #cancelingDueToLowMemory} set. Leave the executor's task queue empty.
   */
  private synchronized void cancelAllQueriesDueToMemory() {

    /*
     * A cancelation task is dual-purpose. Its primary purpose is to cancel
     * a query if the query runs too long. Alternately, if this flag
     * {@link #cancelingDueToLowMemory} is set, the cancelation task will cancel the query
     * due to low memory.
     */
    cancelingDueToLowMemory = true;

    try {
      /*
       * It's tempting to try to process the list of tasks returned from shutdownNow().
       * Unfortunately, that call leaves the executor in a state that causes the task's
       * run() to cancel the task, instead of actually running it. By calling shutdown()
       * we block new task additions and put the executor in a state that allows the
       * task's run() to actually run the task logic.
       */
      executor.shutdown(); // executor won't accept new work ever again
      final BlockingQueue<Runnable> expirationTaskQueue = executor.getQueue();
      for (final Runnable cancelationTask : expirationTaskQueue) {
        cancelationTask.run();
      }
      expirationTaskQueue.clear();
    } finally {
      cancelingDueToLowMemory = false;
    }

  }

  private ScheduledFuture<?> scheduleCancelationTask(final DefaultQuery query,
      final long timeLimitMillis) {

    // Make ThreadLocal queryCanceled available to closure, which will run in a separate thread
    final AtomicBoolean queryCanceledThreadLocal =
        DefaultQuery.queryCanceled.get();

    return executor.schedule(() -> {
      final CacheRuntimeException exception = cancelingDueToLowMemory ? createLowMemoryException()
          : createExpirationException(timeLimitMillis);

      query.setQueryCanceledException(exception);
      queryCanceledThreadLocal.set(true);

      if (logger.isInfoEnabled() && !cancelingDueToLowMemory) {
        logger.info(String.format("%s %s", exception.getMessage(), query));
      }
    }, timeLimitMillis, TimeUnit.MILLISECONDS);
  }

  private QueryExecutionTimeoutException createExpirationException(final long timeLimitMillis) {
    return new QueryExecutionTimeoutException(
        String.format(
            "Query execution canceled after exceeding max execution time %sms.",
            timeLimitMillis));
  }

  private QueryExecutionLowMemoryException createLowMemoryException() {
    return new QueryExecutionLowMemoryException(
        String.format(
            "Query execution canceled due to memory threshold crossed in system, memory used: %s bytes.",
            LOW_MEMORY_USED_BYTES));
  }

  private void logDebug(final DefaultQuery query, final String message) {
    final Thread queryThread = Thread.currentThread();
    logger.debug(
        message + " QueryMonitor size is: {}, Thread (id): {}, Query: {}, Thread is : {}",
        executor.getQueue().size(), queryThread.getId(), query.getQueryString(),
        queryThread);
  }

}
