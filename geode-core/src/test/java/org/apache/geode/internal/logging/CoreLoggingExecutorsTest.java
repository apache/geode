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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.FunctionExecutionPooledExecutor;
import org.apache.geode.distributed.internal.FunctionExecutionPooledExecutor.FunctionExecutionRejectedExecutionHandler;
import org.apache.geode.distributed.internal.OverflowQueueWithDMStats;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.PooledExecutorWithDMStats;
import org.apache.geode.distributed.internal.QueueStatHelper;
import org.apache.geode.distributed.internal.SerialQueuedExecutorWithDMStats;
import org.apache.geode.internal.ScheduledThreadPoolExecutorWithKeepAlive;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory.CommandWrapper;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory.ThreadInitializer;

public class CoreLoggingExecutorsTest {

  private CommandWrapper commandWrapper;
  private PoolStatHelper poolStatHelper;
  private QueueStatHelper queueStatHelper;
  private Runnable runnable;
  private ThreadInitializer threadInitializer;
  private ThreadsMonitoring threadsMonitoring;

  @Before
  public void setUp() {
    commandWrapper = mock(CommandWrapper.class);
    poolStatHelper = mock(PoolStatHelper.class);
    queueStatHelper = mock(QueueStatHelper.class);
    runnable = mock(Runnable.class);
    threadInitializer = mock(ThreadInitializer.class);
    threadsMonitoring = mock(ThreadsMonitoring.class);
  }

  /**
   * Creates, passes in, and uses OverflowQueueWithDMStats.
   *
   * <p>
   * Uses {@code ThreadPoolExecutor.AbortPolicy}.
   */
  @Test
  public void newFixedThreadPoolWithTimeout() {
    int poolSize = 5;
    int keepAliveTime = 2;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newFixedThreadPoolWithTimeout(poolSize, keepAliveTime, MINUTES, queueStatHelper,
            threadName);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(poolSize);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(OverflowQueueWithDMStats.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    OverflowQueueWithDMStats overflowQueueWithDMStats =
        (OverflowQueueWithDMStats) executor.getQueue();

    assertThat(overflowQueueWithDMStats.getQueueStatHelper()).isSameAs(queueStatHelper);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates and passes in OverflowQueueWithDMStats. FunctionExecutionPooledExecutor then creates
   * and passes up SynchronousQueue, but finally uses OverflowQueueWithDMStats.
   *
   * <p>
   * {@code getBufferQueue()} returns passed-in OverflowQueueWithDMStats. {@code getQueue()} returns
   * internal SynchronousQueue.
   *
   * <p>
   * Uses {@code ThreadPoolExecutor.AbortPolicy}.
   */
  @Test
  public void newFunctionThreadPoolWithFeedStatistics() {
    int poolSize = 5;
    int workQueueSize = 2;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newFunctionThreadPoolWithFeedStatistics(poolSize, workQueueSize, queueStatHelper,
            threadName, threadInitializer, commandWrapper, poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(FunctionExecutionPooledExecutor.class);

    FunctionExecutionPooledExecutor executor = (FunctionExecutionPooledExecutor) executorService;

    assertThat(executor.getBufferQueue()).isInstanceOf(OverflowQueueWithDMStats.class);
    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(30);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(FunctionExecutionRejectedExecutionHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    OverflowQueueWithDMStats overflowQueueWithDMStats =
        (OverflowQueueWithDMStats) executor.getBufferQueue();

    assertThat(overflowQueueWithDMStats.getQueueStatHelper()).isSameAs(queueStatHelper);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Passes in and uses BlockingQueue.
   *
   * <p>
   * Uses {@code PooledExecutorWithDMStats.BlockHandler}.
   */
  @Test
  public void newSerialThreadPool() {
    BlockingQueue<Runnable> workQueue = mock(BlockingQueue.class);
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newSerialThreadPool(workQueue, threadName, threadInitializer, commandWrapper,
            poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(SerialQueuedExecutorWithDMStats.class);

    SerialQueuedExecutorWithDMStats executor = (SerialQueuedExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(1);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(1);
    assertThat(executor.getPoolStatHelper()).isSameAs(poolStatHelper);
    assertThat(executor.getQueue()).isSameAs(workQueue);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BlockHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates, passes in, and uses OverflowQueueWithDMStats.
   *
   * <p>
   * Uses {@code PooledExecutorWithDMStats.BlockHandler}.
   */
  @Test
  public void newSerialThreadPoolWithFeedStatistics() {
    int workQueueSize = 2;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newSerialThreadPoolWithFeedStatistics(workQueueSize, queueStatHelper, threadName,
            threadInitializer, commandWrapper, poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(SerialQueuedExecutorWithDMStats.class);

    SerialQueuedExecutorWithDMStats executor = (SerialQueuedExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(1);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(1);
    assertThat(executor.getPoolStatHelper()).isSameAs(poolStatHelper);
    assertThat(executor.getQueue()).isInstanceOf(OverflowQueueWithDMStats.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BlockHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates, passes up, and uses SynchronousQueue. ScheduledThreadPoolExecutorWithKeepAlive then
   * creates a ThreadPoolExecutor to delegate to.
   *
   * <p>
   * Uses {@code ScheduledThreadPoolExecutorWithKeepAlive.BlockCallerPolicy}.
   */
  @Test
  public void newScheduledThreadPool() {
    int poolSize = 10;
    long keepAliveTime = 5000;
    TimeUnit unit = SECONDS;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newScheduledThreadPool(poolSize, keepAliveTime, unit, threadName, threadsMonitoring);

    assertThat(executorService).isInstanceOf(ScheduledThreadPoolExecutorWithKeepAlive.class);

    ScheduledThreadPoolExecutorWithKeepAlive executor =
        (ScheduledThreadPoolExecutorWithKeepAlive) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getDelegateExecutor()).isInstanceOf(ScheduledThreadPoolExecutor.class);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ScheduledThreadPoolExecutorWithKeepAlive.BlockCallerPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ScheduledThreadPoolExecutor delegate = executor.getDelegateExecutor();

    assertThat(delegate.getContinueExistingPeriodicTasksAfterShutdownPolicy()).isFalse();
    assertThat(delegate.getExecuteExistingDelayedTasksAfterShutdownPolicy()).isFalse();

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates and passes up BlockingQueue. PooledExecutorWithDMStats then creates and passes up
   * SynchronousQueue
   */
  @Test
  public void newThreadPool() {
    int poolSize = 10;
    BlockingQueue<Runnable> workQueue = mock(BlockingQueue.class);
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newThreadPool(poolSize, workQueue, threadName, threadInitializer, commandWrapper,
            poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    PooledExecutorWithDMStats executor = (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(30);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BufferHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates ArrayBlockingQueue but then uses SynchronousQueue
   */
  @Test
  public void newThreadPoolWithFixedFeed() {
    int poolSize = 10;
    long keepAliveTime = 5000;
    TimeUnit unit = SECONDS;
    int workQueueSize = 2;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newThreadPoolWithFixedFeed(poolSize, keepAliveTime, unit, workQueueSize, threadName,
            commandWrapper, poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    PooledExecutorWithDMStats executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BufferHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates and passes in OverflowQueueWithDMStats. PooledExecutorWithDMStats then creates and
   * uses SynchronousQueue.
   */
  @Test
  public void newThreadPoolWithFeedStatistics() {
    int poolSize = 10;
    int workQueueSize = 2;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newThreadPoolWithFeedStatistics(poolSize, workQueueSize, queueStatHelper, threadName,
            threadInitializer, commandWrapper, poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    PooledExecutorWithDMStats executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(30);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BufferHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeed() {
    int poolSize = 10;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        poolSize, threadName, commandWrapper);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    PooledExecutorWithDMStats executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(30);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.CallerRunsPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeed_2() {
    int poolSize = 10;
    long keepAliveTime = 5000;
    TimeUnit unit = SECONDS;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        poolSize, keepAliveTime, unit, threadName, commandWrapper, poolStatHelper,
        threadsMonitoring);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    PooledExecutorWithDMStats executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.CallerRunsPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeed_3() {
    int poolSize = 10;
    long keepAliveTime = 5000;
    TimeUnit unit = SECONDS;
    String threadName = "thread";
    RejectedExecutionHandler rejectedExecutionHandler = mock(RejectedExecutionHandler.class);

    ExecutorService executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        poolSize, keepAliveTime, unit, threadName, rejectedExecutionHandler, poolStatHelper);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    PooledExecutorWithDMStats executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler()).isSameAs(rejectedExecutionHandler);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeed_4() {
    int corePoolSize = 10;
    int maximumPoolSize = 20;
    long keepAliveTime = 5000;
    TimeUnit unit = SECONDS;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        corePoolSize, maximumPoolSize, keepAliveTime, unit, threadName, threadInitializer,
        commandWrapper);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(corePoolSize);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(maximumPoolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Used for P2P Reader Threads in ConnectionTable
   */
  @Test
  public void newThreadPoolWithSynchronousFeed_5() {
    int corePoolSize = 10;
    int maximumPoolSize = 20;
    long keepAliveTime = 5000;
    TimeUnit unit = SECONDS;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        corePoolSize, maximumPoolSize, keepAliveTime, unit, threadName);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(corePoolSize);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(maximumPoolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeedThatHandlesRejection() {
    int corePoolSize = 10;
    int maximumPoolSize = 20;
    long keepAliveTime = 5000;
    TimeUnit unit = SECONDS;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newThreadPoolWithSynchronousFeedThatHandlesRejection(corePoolSize, maximumPoolSize,
            keepAliveTime, unit, threadName, threadInitializer, commandWrapper);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(corePoolSize);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(maximumPoolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(CoreLoggingExecutors.QueuingRejectedExecutionHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithUnlimitedFeed() {
    int poolSize = 10;
    long keepAliveTime = 5000;
    TimeUnit unit = SECONDS;
    String threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors.newThreadPoolWithUnlimitedFeed(
        poolSize, keepAliveTime, unit, threadName, threadInitializer, commandWrapper,
        poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BufferHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    ThreadFactory threadFactory = executor.getThreadFactory();
    Thread thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }
}
