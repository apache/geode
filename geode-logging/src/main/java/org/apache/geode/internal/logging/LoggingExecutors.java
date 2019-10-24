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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LoggingExecutors {
  public static ScheduledExecutorService newScheduledThreadPool(String threadName, int poolSize,
      boolean executeDelayedTasks) {
    ScheduledThreadPoolExecutor result =
        new ScheduledThreadPoolExecutor(poolSize, new LoggingThreadFactory(threadName));
    result.setExecuteExistingDelayedTasksAfterShutdownPolicy(executeDelayedTasks);
    return result;
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
    final ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> {
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

  public static ScheduledExecutorService newSingleThreadScheduledExecutor(String threadName) {
    return newScheduledThreadPool(threadName, 1);
  }

  static ThreadPoolExecutor newFixedThreadPool(String threadName, boolean isDaemon,
      int poolSize, long keepAliveSeconds,
      BlockingQueue<Runnable> feed) {
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
}
