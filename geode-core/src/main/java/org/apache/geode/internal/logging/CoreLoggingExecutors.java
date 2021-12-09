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
package org.apache.geode.internal.logging;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.FunctionExecutionPooledExecutor;
import org.apache.geode.distributed.internal.OverflowQueueWithDMStats;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.PooledExecutorWithDMStats;
import org.apache.geode.distributed.internal.QueueStatHelper;
import org.apache.geode.distributed.internal.SerialQueuedExecutorWithDMStats;
import org.apache.geode.internal.ScheduledThreadPoolExecutorWithKeepAlive;
import org.apache.geode.internal.lang.SystemProperty;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory.CommandWrapper;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory.ThreadInitializer;

/**
 * Utility class that creates instances of ExecutorService whose threads will always log uncaught
 * exceptions.
 */
public class CoreLoggingExecutors {

  private static final String IDLE_THREAD_TIMEOUT_MILLIS_PROPERTY = "IDLE_THREAD_TIMEOUT";
  private static final int DEFAULT_IDLE_THREAD_TIMEOUT_MILLIS = 30_000 * 60;

  public static ExecutorService newFixedThreadPoolWithTimeout(int poolSize, long keepAliveTime,
      TimeUnit unit, QueueStatHelper queueStatHelper, String threadName) {
    BlockingQueue<Runnable> workQueue = createWorkQueueWithStatistics(0, queueStatHelper);
    return LoggingExecutors.newFixedThreadPool(poolSize, keepAliveTime, unit, workQueue, threadName,
        true);
  }

  public static ExecutorService newFunctionThreadPoolWithFeedStatistics(int poolSize,
      int workQueueSize, QueueStatHelper queueStatHelper, String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      PoolStatHelper poolStatHelper, ThreadsMonitoring threadsMonitoring) {
    BlockingQueue<Runnable> workQueue =
        createWorkQueueWithStatistics(workQueueSize, queueStatHelper);
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new FunctionExecutionPooledExecutor(poolSize, workQueue, threadFactory, poolStatHelper,
        threadsMonitoring);
  }

  public static ExecutorService newSerialThreadPool(BlockingQueue<Runnable> workQueue,
      String threadName, ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      PoolStatHelper poolStatHelper, ThreadsMonitoring threadsMonitoring) {
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new SerialQueuedExecutorWithDMStats(workQueue, threadFactory, poolStatHelper,
        threadsMonitoring);
  }

  public static ExecutorService newSerialThreadPoolWithFeedStatistics(int workQueueSize,
      QueueStatHelper queueStatHelper, String threadName, ThreadInitializer threadInitializer,
      CommandWrapper commandWrapper, PoolStatHelper poolStatHelper,
      ThreadsMonitoring threadsMonitoring) {
    BlockingQueue<Runnable> workQueue =
        createWorkQueueWithStatistics(workQueueSize, queueStatHelper);
    return newSerialThreadPool(workQueue, threadName, threadInitializer, commandWrapper,
        poolStatHelper, threadsMonitoring);
  }

  public static ScheduledExecutorService newScheduledThreadPool(int poolSize, long keepAliveTime,
      TimeUnit unit, String threadName, ThreadsMonitoring threadsMonitoring) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName);
    ScheduledThreadPoolExecutorWithKeepAlive result =
        new ScheduledThreadPoolExecutorWithKeepAlive(poolSize, keepAliveTime, unit, threadFactory,
            threadsMonitoring);
    result.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    result.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return result;
  }

  public static ExecutorService newThreadPool(int poolSize, BlockingQueue<Runnable> workQueue,
      String threadName, ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      PoolStatHelper poolStatHelper, ThreadsMonitoring threadsMonitoring) {
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new PooledExecutorWithDMStats(poolSize, getIdleThreadTimeoutMillis(), MILLISECONDS,
        workQueue, threadFactory, poolStatHelper, threadsMonitoring);
  }

  public static ExecutorService newThreadPoolWithFixedFeed(int poolSize, long keepAliveTime,
      TimeUnit unit, int workQueueSize, String threadName, CommandWrapper commandWrapper,
      PoolStatHelper poolStatHelper, ThreadsMonitoring threadsMonitoring) {
    ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(workQueueSize);
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, commandWrapper);
    return new PooledExecutorWithDMStats(poolSize, keepAliveTime, unit, workQueue, threadFactory,
        poolStatHelper, threadsMonitoring);
  }

  public static ExecutorService newThreadPoolWithFeedStatistics(int poolSize, int workQueueSize,
      QueueStatHelper queueStatHelper, String threadName, ThreadInitializer threadInitializer,
      CommandWrapper commandWrapper, PoolStatHelper poolStatHelper,
      ThreadsMonitoring threadsMonitoring) {
    BlockingQueue<Runnable> workQueue =
        createWorkQueueWithStatistics(workQueueSize, queueStatHelper);
    return newThreadPool(poolSize, workQueue, threadName, threadInitializer, commandWrapper,
        poolStatHelper, threadsMonitoring);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeed(int poolSize, String threadName,
      CommandWrapper commandWrapper) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, commandWrapper);
    SynchronousQueue<Runnable> workQueue = new SynchronousQueue<>();
    return new PooledExecutorWithDMStats(poolSize, getIdleThreadTimeoutMillis(), MILLISECONDS,
        workQueue, threadFactory, null, null);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeed(int poolSize, long keepAliveTime,
      TimeUnit unit, String threadName, CommandWrapper commandWrapper,
      PoolStatHelper poolStatHelper, ThreadsMonitoring threadsMonitoring) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, commandWrapper);
    SynchronousQueue<Runnable> workQueue = new SynchronousQueue<>();
    return new PooledExecutorWithDMStats(poolSize, keepAliveTime, unit, workQueue, threadFactory,
        poolStatHelper, threadsMonitoring);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeed(int poolSize, long keepAliveTime,
      TimeUnit unit, String threadName, RejectedExecutionHandler rejectionHandler,
      PoolStatHelper poolStatHelper) {
    SynchronousQueue<Runnable> workQueue = new SynchronousQueue<>();
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName);
    return new PooledExecutorWithDMStats(poolSize, keepAliveTime, unit, workQueue, threadFactory,
        rejectionHandler, poolStatHelper, null);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeed(int corePoolSize,
      int maximumPoolSize, long keepAliveTime, TimeUnit unit, String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper) {
    BlockingQueue<Runnable> blockingQueue = new SynchronousQueue<>();
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, blockingQueue,
        threadFactory);
  }

  /**
   * Used for P2P Reader Threads in ConnectionTable
   */
  public static ExecutorService newThreadPoolWithSynchronousFeed(int corePoolSize,
      int maximumPoolSize, long keepAliveTime, TimeUnit unit, String threadName) {
    return newThreadPoolWithSynchronousFeed(corePoolSize, maximumPoolSize, keepAliveTime, unit,
        threadName, null, null);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeedThatHandlesRejection(
      int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper) {
    BlockingQueue<Runnable> blockingQueue = new SynchronousQueue<>();
    RejectedExecutionHandler rejectedExecutionHandler =
        new QueuingRejectedExecutionHandler(blockingQueue);
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit,
        blockingQueue, threadFactory, rejectedExecutionHandler);
  }

  public static ExecutorService newThreadPoolWithUnlimitedFeed(int poolSize, long keepAliveTime,
      TimeUnit unit, String threadName, ThreadInitializer threadInitializer,
      CommandWrapper commandWrapper, PoolStatHelper poolStatHelper,
      ThreadsMonitoring threadsMonitoring) {
    LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new PooledExecutorWithDMStats(poolSize, keepAliveTime, unit, workQueue, threadFactory,
        poolStatHelper, threadsMonitoring);
  }

  private CoreLoggingExecutors() {
    // no instances allowed
  }

  private static BlockingQueue<Runnable> createWorkQueueWithStatistics(int workQueueSize,
      QueueStatHelper queueStatHelper) {
    BlockingQueue<Runnable> workQueue;
    if (workQueueSize == 0) {
      workQueue = new OverflowQueueWithDMStats<>(queueStatHelper);
    } else {
      workQueue = new OverflowQueueWithDMStats<>(workQueueSize, queueStatHelper);
    }
    return workQueue;
  }

  private static int getIdleThreadTimeoutMillis() {
    return getInteger(SystemProperty.GEMFIRE_PREFIX + IDLE_THREAD_TIMEOUT_MILLIS_PROPERTY,
        DEFAULT_IDLE_THREAD_TIMEOUT_MILLIS);
  }

  @VisibleForTesting
  static class QueuingRejectedExecutionHandler implements RejectedExecutionHandler {

    private final BlockingQueue<Runnable> blockingQueue;

    private QueuingRejectedExecutionHandler(BlockingQueue<Runnable> blockingQueue) {
      this.blockingQueue = blockingQueue;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      try {
        blockingQueue.put(r);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt(); // preserve the state
        throw new RejectedExecutionException("interrupted", ex);
      }
    }
  }
}
