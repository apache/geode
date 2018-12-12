/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.geode.distributed.internal.FunctionExecutionPooledExecutor;
import org.apache.geode.distributed.internal.OverflowQueueWithDMStats;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.PooledExecutorWithDMStats;
import org.apache.geode.distributed.internal.QueueStatHelper;
import org.apache.geode.distributed.internal.SerialQueuedExecutorWithDMStats;
import org.apache.geode.internal.ScheduledThreadPoolExecutorWithKeepAlive;
import org.apache.geode.internal.logging.LoggingThreadFactory.CommandWrapper;
import org.apache.geode.internal.logging.LoggingThreadFactory.ThreadInitializer;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

/**
 * Utility class that creates instances of ExecutorService
 * whose threads will always log uncaught exceptions.
 */
public class LoggingExecutors {

  public static ExecutorService newSerialThreadPool(String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      PoolStatHelper stats, ThreadsMonitoring threadsMonitoring, BlockingQueue<Runnable> feed) {
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new SerialQueuedExecutorWithDMStats(feed, stats, threadFactory, threadsMonitoring);
  }

  public static ExecutorService newSerialThreadPoolWithFeedStatistics(String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      PoolStatHelper poolStats,
      ThreadsMonitoring threadsMonitoring, int feedSize, QueueStatHelper feedStats) {
    BlockingQueue<Runnable> feed = createFeedWithStatistics(feedSize, feedStats);
    return newSerialThreadPool(threadName, threadInitializer, commandWrapper, poolStats,
        threadsMonitoring, feed);
  }

  public static ExecutorService newSerialThreadPoolWithUnlimitedFeed(String threadName,
      ThreadInitializer threadInitializer,
      CommandWrapper commandWrapper, PoolStatHelper stats, ThreadsMonitoring threadsMonitoring) {
    LinkedBlockingQueue<Runnable> feed = new LinkedBlockingQueue<>();
    return newSerialThreadPool(threadName, threadInitializer, commandWrapper, stats,
        threadsMonitoring, feed);
  }

  public static ExecutorService newThreadPoolWithUnlimitedFeed(String threadName,
      ThreadInitializer threadInitializer,
      CommandWrapper commandWrapper, int poolSize,
      PoolStatHelper poolStats, int msTimeout, ThreadsMonitoring threadsMonitoring) {
    LinkedBlockingQueue<Runnable> feed = new LinkedBlockingQueue<>();
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new PooledExecutorWithDMStats(feed, poolSize, poolStats, threadFactory, msTimeout,
        threadsMonitoring);
  }

  public static ExecutorService newThreadPoolWithFixedFeed(String threadName,
      CommandWrapper commandWrapper,
      int poolSize, PoolStatHelper poolStats, int msTimeout, ThreadsMonitoring threadsMonitoring,
      int feedSize) {
    ArrayBlockingQueue<Runnable> feed = new ArrayBlockingQueue<>(feedSize);
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, commandWrapper);
    return new PooledExecutorWithDMStats(feed, poolSize, poolStats, threadFactory, msTimeout,
        threadsMonitoring);
  }

  public static ExecutorService newFunctionThreadPoolWithFeedStatistics(String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      int poolSize, PoolStatHelper poolStats,
      ThreadsMonitoring threadsMonitoring, int feedSize, QueueStatHelper feedStats) {
    BlockingQueue<Runnable> feed = createFeedWithStatistics(feedSize, feedStats);
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new FunctionExecutionPooledExecutor(feed, poolSize, poolStats, threadFactory, true,
        threadsMonitoring);
  }

  private static BlockingQueue<Runnable> createFeedWithStatistics(int feedSize,
      QueueStatHelper feedStats) {
    BlockingQueue<Runnable> feed;
    if (feedSize == 0) {
      feed = new OverflowQueueWithDMStats<>(feedStats);
    } else {
      feed = new OverflowQueueWithDMStats<>(feedSize, feedStats);
    }
    return feed;
  }

  public static ExecutorService newThreadPoolWithFeedStatistics(String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      int poolSize, PoolStatHelper poolStats,
      ThreadsMonitoring threadsMonitoring, int feedSize, QueueStatHelper feedStats) {
    BlockingQueue<Runnable> feed = createFeedWithStatistics(feedSize, feedStats);
    return newThreadPool(threadName, threadInitializer, commandWrapper, poolSize, poolStats,
        threadsMonitoring, feed);
  }

  public static ExecutorService newThreadPool(String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      int poolSize, PoolStatHelper poolStats,
      ThreadsMonitoring threadsMonitoring, BlockingQueue<Runnable> feed) {
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new PooledExecutorWithDMStats(feed, poolSize, poolStats, threadFactory,
        threadsMonitoring);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeed(String threadName,
      CommandWrapper commandWrapper,
      int poolSize) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, commandWrapper);
    SynchronousQueue<Runnable> feed = new SynchronousQueue<>();
    return new PooledExecutorWithDMStats(feed, poolSize, threadFactory, null);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeed(String threadName,
      CommandWrapper commandWrapper,
      int poolSize, PoolStatHelper poolStats, int msTimeout, ThreadsMonitoring threadsMonitoring) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, commandWrapper);
    SynchronousQueue<Runnable> feed = new SynchronousQueue<>();
    return new PooledExecutorWithDMStats(feed, poolSize, poolStats, threadFactory, msTimeout,
        threadsMonitoring);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeed(String threadName, int poolSize,
      PoolStatHelper stats, int msTimeout, RejectedExecutionHandler rejectionHandler) {
    final SynchronousQueue<Runnable> feed = new SynchronousQueue<>();
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName);
    return new PooledExecutorWithDMStats(feed, poolSize, stats, threadFactory, msTimeout,
        rejectionHandler, null);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeed(String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      int corePoolSize, int maximumPoolSize, long keepAliveSeconds) {
    final BlockingQueue<Runnable> blockingQueue = new SynchronousQueue<>();
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveSeconds, SECONDS,
        blockingQueue,
        threadFactory);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeed(String threadName,
      int corePoolSize, int maximumPoolSize, long keepAliveSeconds) {
    return newThreadPoolWithSynchronousFeed(threadName, null, null, corePoolSize, maximumPoolSize,
        keepAliveSeconds);
  }

  public static ExecutorService newThreadPoolWithSynchronousFeedThatHandlesRejection(
      String threadName,
      ThreadInitializer threadInitializer, CommandWrapper commandWrapper,
      int corePoolSize, int maximumPoolSize,
      long keepAliveSeconds) {
    final BlockingQueue<Runnable> blockingQueue = new SynchronousQueue<>();
    final RejectedExecutionHandler rejectedExecutionHandler = (r, pool) -> {
      try {
        blockingQueue.put(r);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt(); // preserve the state
        throw new RejectedExecutionException("interrupted", ex);
      }
    };
    ThreadFactory threadFactory =
        new LoggingThreadFactory(threadName, threadInitializer, commandWrapper);
    return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveSeconds, SECONDS,
        blockingQueue,
        threadFactory, rejectedExecutionHandler);
  }

  private static ThreadPoolExecutor newFixedThreadPool(String threadName, boolean isDaemon,
      int poolSize, long keepAliveSeconds, BlockingQueue<Runnable> feed) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, isDaemon);
    return new ThreadPoolExecutor(poolSize, poolSize,
        keepAliveSeconds, SECONDS,
        feed, threadFactory);
  }

  private static ThreadPoolExecutor newFixedThreadPool(String threadName, boolean isDaemon,
      long keepAliveSeconds, int poolSize) {
    LinkedBlockingQueue<Runnable> feed = new LinkedBlockingQueue<>();
    return newFixedThreadPool(threadName, isDaemon, poolSize, keepAliveSeconds, feed);
  }

  public static ExecutorService newFixedThreadPool(String threadName, boolean isDaemon,
      int poolSize) {
    return newFixedThreadPool(threadName, isDaemon, 0L, poolSize);
  }

  public static ExecutorService newFixedThreadPoolWithTimeout(String threadName, int poolSize,
      int keepAliveSeconds) {
    return newFixedThreadPool(threadName, true, keepAliveSeconds, poolSize);
  }

  public static ExecutorService newFixedThreadPoolWithTimeout(String threadName,
      int poolSize, int keepAliveSeconds, QueueStatHelper feedStats) {
    BlockingQueue<Runnable> feed = createFeedWithStatistics(0, feedStats);
    return newFixedThreadPool(threadName, true, poolSize, keepAliveSeconds, feed);
  }

  public static ExecutorService newFixedThreadPoolWithFeedSize(String threadName,
      int poolSize, int feedSize) {
    LinkedBlockingQueue<Runnable> feed = new LinkedBlockingQueue<>(feedSize);
    RejectedExecutionHandler rejectionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, poolSize, 10, SECONDS, feed,
        threadFactory, rejectionHandler);
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  public static ExecutorService newSingleThreadExecutor(String threadName, boolean isDaemon) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, isDaemon);
    return new ThreadPoolExecutor(1, 1, 0L, SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        threadFactory);
  }

  public static ExecutorService newCachedThreadPool(String threadName, boolean isDaemon) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, isDaemon);
    return new ThreadPoolExecutor(0, Integer.MAX_VALUE,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        threadFactory);
  }

  public static ExecutorService newWorkStealingPool(String threadName, int maxParallelThreads) {
    final ForkJoinWorkerThreadFactory factory = pool -> {
      ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
      LoggingUncaughtExceptionHandler.setOnThread(worker);
      worker.setName(threadName + worker.getPoolIndex());
      return worker;
    };
    return new ForkJoinPool(maxParallelThreads, factory, null, true);
  }

  public static Executor newThreadOnEachExecute(String threadName) {
    return command -> new LoggingThread(threadName, command).start();
  }

  public static ScheduledExecutorService newScheduledThreadPool(String threadName, int poolSize) {
    return newScheduledThreadPool(threadName, poolSize, true);
  }

  public static ScheduledExecutorService newScheduledThreadPool(String threadName, int poolSize,
      boolean executeDelayedTasks) {
    ScheduledThreadPoolExecutor result =
        new ScheduledThreadPoolExecutor(poolSize, new LoggingThreadFactory(threadName));
    result.setExecuteExistingDelayedTasksAfterShutdownPolicy(executeDelayedTasks);
    return result;
  }

  public static ScheduledExecutorService newSingleThreadScheduledExecutor(String threadName) {
    return newScheduledThreadPool(threadName, 1);
  }

  public static ScheduledExecutorService newScheduledThreadPool(String threadName, int poolSize,
      int keepAliveMillis, ThreadsMonitoring threadsMonitoring) {
    ScheduledThreadPoolExecutorWithKeepAlive result =
        new ScheduledThreadPoolExecutorWithKeepAlive(poolSize,
            keepAliveMillis, MILLISECONDS, new LoggingThreadFactory(threadName),
            threadsMonitoring);
    result.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    result.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return result;
  }

  private LoggingExecutors() {
    // no instances allowed
  }

}
