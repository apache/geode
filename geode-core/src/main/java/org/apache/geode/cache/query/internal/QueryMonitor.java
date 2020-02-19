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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * {@link QueryMonitor} class, monitors the query execution time. In typical usage, the maximum
 * query execution time might be set (upon construction) via the system property {@link
 * GemFireCacheImpl#MAX_QUERY_EXECUTION_TIME}. The number of threads allocated to query monitoring
 * is determined by the instance of {@link ScheduledThreadPoolExecutor} passed to the
 * constructor.
 *
 * This class supports a low-memory mode, established by {@link #setLowMemory(boolean, long)}
 * with {@code isLowMemory=true}. In that mode, any attempt to monitor a (new) query will
 * throw an exception.
 *
 * The {@link #monitorQueryExecution(ExecutionContext)} method initiates monitoring of a query.
 * {@link
 * #stopMonitoringQueryExecution(ExecutionContext)} stops monitoring a query.
 *
 * If the {@link QueryMonitor} determines a query needs to be canceled: either because it is taking
 * too long, or because memory is running low, it does two things:
 *
 * <ul>
 * <li>registers an exception on the query via
 * {@link ExecutionContext#setQueryCanceledException(CacheRuntimeException)}</li>
 * <li>sets the {@link ExecutionContext#isCanceled} thread-local variable to {@code true}
 * so that subsequent calls to {@link #throwExceptionIfQueryOnCurrentThreadIsCanceled()} will throw
 * an exception</li>
 * </ul>
 *
 * Code outside this class, that wishes to participate in cooperative cancelation of queries calls
 * {@link #throwExceptionIfQueryOnCurrentThreadIsCanceled()} at various yield points. In catch
 * blocks, {@link ExecutionContext#getQueryCanceledException()} is interrogated to learn the
 * cancelation cause.
 *
 * @since GemFire 6.0
 */
public class QueryMonitor {
  private static final Logger logger = LogService.getLogger();

  private final InternalCache cache;

  private final long defaultMaxQueryExecutionTime;

  private final ScheduledThreadPoolExecutor executor;

  @MakeNotStatic
  private static volatile MemoryState memoryState = MemoryStateImpl.HEAP_AVAILABLE;

  @MakeNotStatic
  private static volatile long memoryUsedBytes = 0;

  /**
   * This class will call {@link ScheduledThreadPoolExecutor#setRemoveOnCancelPolicy(boolean)} on
   * {@code executor} to set that property to {@code true}.
   *
   * The default behavior of a {@link ScheduledThreadPoolExecutor} is to keep canceled tasks in the
   * queue, relying on the timeout processing loop to remove them when their time is up. That
   * behaviour would cause tasks for completed queries to remain in the queue until their
   * timeout deadline was reached, resulting in queue growth.
   *
   * Setting the remove-on-cancel-policy to {@code true} changes that behavior so tasks are removed
   * immediately upon cancelation (via {@link #stopMonitoringQueryExecution(ExecutionContext)}).
   *
   * @param executor is responsible for processing scheduled cancelation tasks
   * @param cache is interrogated via {@link InternalCache#isQueryMonitorDisabledForLowMemory} at
   *        each low-memory state change
   * @param defaultMaxQueryExecutionTime is the maximum time, in milliseconds, that any query is
   *        allowed to run
   */
  public QueryMonitor(final ScheduledThreadPoolExecutor executor,
      final InternalCache cache,
      final long defaultMaxQueryExecutionTime) {
    Objects.requireNonNull(executor);
    Objects.requireNonNull(cache);

    this.cache = cache;
    this.defaultMaxQueryExecutionTime = defaultMaxQueryExecutionTime;

    this.executor = executor;
    this.executor.setRemoveOnCancelPolicy(true);
  }

  /**
   * Start monitoring the query.
   *
   * Must not be called from a thread that is not the query thread, because this class uses a
   * ThreadLocal on the query thread!
   */
  public void monitorQueryExecution(final ExecutionContext executionContext) {
    monitorQueryExecution(executionContext, defaultMaxQueryExecutionTime);
  }

  /**
   * Each query can have a different maxQueryExecution time. Make this method public to expose that
   * feature to callers.
   *
   * Must not be called from a thread that is not the query thread, because this class uses a
   * ThreadLocal on the query thread!
   */
  private void monitorQueryExecution(final ExecutionContext executionContext,
      final long maxQueryExecutionTime) {

    // cq query is not monitored
    if (executionContext.isCqQueryContext()) {
      return;
    }

    executionContext
        .setCancellationTask(scheduleCancelationTask(executionContext, maxQueryExecutionTime));

    if (logger.isDebugEnabled()) {
      logDebug(executionContext, "Adding thread to QueryMonitor.");
    }
  }

  /**
   * Stop monitoring the query.
   *
   * Must not be called from a thread that is not the query thread, because this class uses a
   * ThreadLocal on the query thread!
   */
  public void stopMonitoringQueryExecution(final ExecutionContext executionContext) {
    executionContext.getCancellationTask().ifPresent(task -> task.cancel(false));

    if (logger.isDebugEnabled()) {
      logDebug(executionContext, "Query completed before cancelation.");
    }
  }

  /**
   * Throw an exception if the query has been canceled. The {@link QueryMonitor} cancels the query
   * if it takes more than the max query execution time or in low memory situations where critical
   * heap percentage has been set on the resource manager.
   *
   * @throws QueryExecutionCanceledException if the query has been canceled
   */
  public static void throwExceptionIfQueryOnCurrentThreadIsCanceled() {
    if (ExecutionContext.isCanceled.get().get()) {
      throw new QueryExecutionCanceledException();
    }
  }

  /**
   * Stops query monitoring. Makes this {@link QueryMonitor} unusable for further monitoring.
   */
  public void stopMonitoring() {
    executor.shutdownNow();
  }

  public static boolean isLowMemory() {
    return memoryState.isLowMemory();
  }

  public static long getMemoryUsedBytes() {
    return memoryUsedBytes;
  }

  /**
   * Caller must not call this method concurrently from multiple threads.
   * In addition to causing data inconsistency, concurrent calls will result in
   * lost updates e.g. transitions to low-memory status could be missed,
   * resulting in a failure to cancel queries.
   */
  public void setLowMemory(final boolean isLowMemory, final long usedBytes) {
    memoryState.setLowMemory(executor, isLowMemory, usedBytes, cache);
  }

  /**
   * This interface plays the role of the "State" interface in the GoF "State" design pattern.
   * Its implementations embodied in the {@link MemoryStateImpl} enum (an abstract base class,
   * or ABC) and its enum constants (subclasses of the ABC) play the role of "ConcreteState"
   * classes in that design pattern.
   *
   * The "Context" role is fulfilled by the melange of behavior
   * and state embodied in the (static) {@link #isLowMemory()} and
   * {@link #getMemoryUsedBytes()} methods and the {@link #setLowMemory(boolean, long)}
   * method and the static fields they manipulate.
   */
  private interface MemoryState {
    void setLowMemory(ScheduledThreadPoolExecutor executor,
        boolean isLowMemory,
        long usedBytes,
        InternalCache cache);

    ScheduledFuture<?> schedule(Runnable command,
        long delay,
        TimeUnit unit,
        ScheduledExecutorService scheduledExecutorService,
        ExecutionContext executionContext);

    boolean isLowMemory();

    CacheRuntimeException createCancellationException(long timeLimitMillis,
        ExecutionContext executionContext);
  }

  /**
   * This enum (an abstract base class or ABC) and its enum constants (subclasses of the ABC)
   * play the role of "ConcreteState" classes in the GoF "State" pattern.
   *
   * See {@link MemoryState} for details.
   */
  private enum MemoryStateImpl implements MemoryState {
    HEAP_AVAILABLE {
      @Override
      public void _setLowMemory(final ScheduledThreadPoolExecutor executor,
          final boolean isLowMemory,
          final long usedBytes,
          final InternalCache cache) {
        if (isLowMemory) {
          memoryState = HEAP_EXHAUSTED;

          /*
           * We need to already be in the HEAP_EXHAUSTED state because we want the
           * cancelation behavior associated with that state.
           */
          cancelAllQueries(executor);
        }
        // Otherwise, no state change
      }

      @Override
      public boolean isLowMemory() {
        return false;
      }

      @Override
      public ScheduledFuture<?> schedule(final Runnable command, final long delay,
          final TimeUnit unit,
          final ScheduledExecutorService scheduledExecutorService,
          final ExecutionContext executionContext) {
        return scheduledExecutorService.schedule(command, delay, unit);
      }

      @Override
      public CacheRuntimeException createCancellationException(final long timeLimitMillis,
          final ExecutionContext executionContext) {
        final String message = String.format(
            "Query execution canceled after exceeding max execution time %sms.",
            timeLimitMillis);
        if (logger.isInfoEnabled()) {
          logger.info(String.format("%s %s", message, executionContext));
        }
        return new QueryExecutionTimeoutException(message);
      }

      /**
       * Run all cancelation tasks. Leave the executor's task queue empty.
       */
      private void cancelAllQueries(final ScheduledThreadPoolExecutor executor) {
        final BlockingQueue<Runnable> expirationTaskQueue = executor.getQueue();
        for (final Runnable cancelationTask : expirationTaskQueue) {
          if (expirationTaskQueue.remove(cancelationTask)) {
            cancelationTask.run();
          }
        }
      }

    },
    HEAP_EXHAUSTED {
      @Override
      public void _setLowMemory(final ScheduledThreadPoolExecutor executor,
          final boolean isLowMemory,
          final long usedBytes,
          final InternalCache cache) {
        if (!isLowMemory) {
          memoryState = HEAP_AVAILABLE;
        }
        // Otherwise, no state change
      }

      @Override
      public boolean isLowMemory() {
        return true;
      }

      @Override
      public ScheduledFuture<?> schedule(final Runnable command, final long timeLimitMillis,
          final TimeUnit unit,
          final ScheduledExecutorService scheduledExecutorService,
          final ExecutionContext executionContext) {
        final CacheRuntimeException lowMemoryException =
            createCancellationException(timeLimitMillis, executionContext);
        executionContext.setQueryCanceledException(lowMemoryException);
        throw lowMemoryException;
      }

      @Override
      public CacheRuntimeException createCancellationException(final long timeLimitMillis,
          final ExecutionContext executionContext) {
        return new QueryExecutionLowMemoryException(
            String.format(
                "Query execution canceled due to memory threshold crossed in system, memory used: %s bytes.",
                memoryUsedBytes));
      }

    };

    @Override
    public void setLowMemory(final ScheduledThreadPoolExecutor executor,
        final boolean isLowMemory,
        final long usedBytes,
        final InternalCache cache) {
      if (cache.isQueryMonitorDisabledForLowMemory()) {
        return;
      }

      memoryUsedBytes = usedBytes;

      _setLowMemory(executor, isLowMemory, usedBytes, cache);
    }

    void _setLowMemory(final ScheduledThreadPoolExecutor executor,
        final boolean isLowMemory,
        final long usedBytes,
        final InternalCache cache) {
      throw new IllegalStateException("subclass must override");
    }

  }

  private ScheduledFuture<?> scheduleCancelationTask(final ExecutionContext executionContext,
      final long timeLimitMillis) {

    // Make ThreadLocal isCanceled available to closure, which will run in a separate thread
    final AtomicBoolean queryCanceledThreadLocal =
        ExecutionContext.isCanceled.get();

    /*
     * This is where the GoF "State" design pattern comes home to roost.
     *
     * memoryState.schedule() is going to either schedule or throw an exception depending on what
     * state we are _currently_ in. Remember the switching of that state (reference) happens
     * in a separate thread, up in the setLowMemory() method, generally called by the
     * HeapMemoryMonitor.
     *
     * The first line of the lambda/closure, when it _eventually_ runs (in yet another thread--
     * a thread from the executor), will access what is _then_ the current state, through
     * memoryState, to createCancelationException().
     */
    return memoryState.schedule(() -> {
      final CacheRuntimeException exception = memoryState
          .createCancellationException(timeLimitMillis, executionContext);

      executionContext.setQueryCanceledException(exception);
      queryCanceledThreadLocal.set(true);

    }, timeLimitMillis, TimeUnit.MILLISECONDS, executor, executionContext);
  }

  private void logDebug(final ExecutionContext executionContext, final String message) {
    final Thread queryThread = Thread.currentThread();
    logger.debug(
        message + " QueryMonitor size is: {}, Thread (id): {}, Query: {}, Thread is : {}",
        executor.getQueue().size(), queryThread.getId(),
        executionContext.getQuery().getQueryString(),
        queryThread);
  }

}
