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
package org.apache.geode.logging.internal.executors;

import static java.lang.Integer.MAX_VALUE;
import static java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
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

  public static ExecutorService newCachedThreadPool(String threadName, boolean isDaemon) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, isDaemon);
    SynchronousQueue<Runnable> workQueue = new SynchronousQueue<>();
    return new ThreadPoolExecutor(0, MAX_VALUE, 60, SECONDS, workQueue, threadFactory);
  }

  public static ThreadPoolExecutor newFixedThreadPool(int poolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue, String threadName, boolean isDaemon) {
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, isDaemon);
    return new ThreadPoolExecutor(poolSize, poolSize, keepAliveTime, unit, workQueue,
        threadFactory);
  }

  private static ThreadPoolExecutor newFixedThreadPool(int poolSize, long keepAliveTime,
      TimeUnit unit, String threadName, boolean isDaemon) {
    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    return newFixedThreadPool(poolSize, keepAliveTime, unit, workQueue, threadName, isDaemon);
  }

  public static ExecutorService newFixedThreadPool(int poolSize, String threadName,
      boolean isDaemon) {
    return newFixedThreadPool(poolSize, 0, SECONDS, threadName, isDaemon);
  }

  public static ExecutorService newFixedThreadPoolWithFeedSize(int poolSize, int workQueueSize,
      String threadName) {
    LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(workQueueSize);
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName);
    RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, poolSize, 10, SECONDS, workQueue,
        threadFactory, rejectedExecutionHandler);
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  public static ExecutorService newFixedThreadPoolWithTimeout(int poolSize, int keepAliveTime,
      TimeUnit unit, String threadName) {
    return newFixedThreadPool(poolSize, keepAliveTime, unit, threadName, true);
  }

  public static ScheduledExecutorService newScheduledThreadPool(int poolSize, String threadName) {
    return newScheduledThreadPool(poolSize, threadName, true);
  }

  public static ScheduledExecutorService newScheduledThreadPool(int poolSize, String threadName,
      boolean executeDelayedTasks) {
    LoggingThreadFactory threadFactory = new LoggingThreadFactory(threadName);
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(poolSize, threadFactory);
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(executeDelayedTasks);
    return executor;
  }

  public static ExecutorService newSingleThreadExecutor(String threadName, boolean isDaemon) {
    LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    ThreadFactory threadFactory = new LoggingThreadFactory(threadName, isDaemon);
    return new ThreadPoolExecutor(1, 1, 0, SECONDS, workQueue, threadFactory);
  }

  public static ScheduledExecutorService newSingleThreadScheduledExecutor(String threadName) {
    return newScheduledThreadPool(1, threadName);
  }

  /** Used for P2P Reader Threads in ConnectionTable */
  public static Executor newThreadOnEachExecute(String threadName) {
    return command -> new LoggingThread(threadName, command).start();
  }

  public static ExecutorService newWorkStealingPool(String threadName, int maximumParallelThreads) {
    ForkJoinWorkerThreadFactory factory = pool -> {
      ForkJoinWorkerThread worker = defaultForkJoinWorkerThreadFactory.newThread(pool);
      LoggingUncaughtExceptionHandler.setOnThread(worker);
      worker.setName(threadName + worker.getPoolIndex());
      return worker;
    };
    return new ForkJoinPool(maximumParallelThreads, factory, null, true);
  }
}
