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
package org.apache.geode.internal;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.internal.monitoring.ThreadsMonitoring;

/**
 * A ScheduledThreadPoolExecutor which allows threads to time out after the keep alive time. With
 * the normal ScheduledThreadPoolExecutor, there is no way to configure it such that it only add
 * threads as needed.
 *
 * <p>
 * This executor is not very useful if you only want to have 1 thread. Use the
 * ScheduledThreadPoolExecutor in that case. This class with throw an exception if you try to
 * configure it with one thread.
 */
public class ScheduledThreadPoolExecutorWithKeepAlive
    extends ThreadPoolExecutor
    implements ScheduledExecutorService {

  private final ScheduledThreadPoolExecutor timer;
  private final ThreadsMonitoring threadsMonitoring;

  public ScheduledThreadPoolExecutorWithKeepAlive(int poolSize, long keepAliveTime, TimeUnit unit,
      ThreadFactory threadFactory, ThreadsMonitoring threadsMonitoring) {
    super(0, poolSize - 1, keepAliveTime, unit, new SynchronousQueue<>(), threadFactory,
        new BlockCallerPolicy());

    timer = new ScheduledThreadPoolExecutor(1, threadFactory) {
      @Override
      protected void terminated() {
        super.terminated();
        ScheduledThreadPoolExecutorWithKeepAlive.super.shutdown();
      }
    };

    this.threadsMonitoring = threadsMonitoring;
  }

  @Override
  public void execute(Runnable command) {
    timer.execute(new HandOffTask(command));
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (threadsMonitoring != null) {
      threadsMonitoring.startMonitor(ThreadsMonitoring.Mode.ScheduledThreadExecutor);
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable ex) {
    if (threadsMonitoring != null) {
      threadsMonitoring.endMonitor();
    }
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return schedule(task, 0, NANOSECONDS);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return schedule(task, 0, NANOSECONDS, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return schedule(task, 0, NANOSECONDS);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    DelegatingScheduledFuture<V> future = new DelegatingScheduledFuture<>(callable);
    ScheduledFuture<V> timerFuture =
        uncheckedCast(timer.schedule(new HandOffTask(future), delay, unit));
    future.setDelegate(timerFuture);
    return future;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return schedule(command, delay, unit, null);
  }

  private <V> ScheduledFuture<V> schedule(Runnable command, long delay, TimeUnit unit, V result) {
    DelegatingScheduledFuture<V> future = new DelegatingScheduledFuture<>(command, result);
    ScheduledFuture<V> timerFuture =
        uncheckedCast(timer.schedule(new HandOffTask(future), delay, unit));
    future.setDelegate(timerFuture);
    return future;
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
      TimeUnit unit) {
    DelegatingScheduledFuture future = new DelegatingScheduledFuture<>(command, null, true);
    ScheduledFuture timerFuture =
        timer.scheduleAtFixedRate(new HandOffTask(future), initialDelay, period, unit);
    future.setDelegate(timerFuture);
    return future;
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
      TimeUnit unit) {
    DelegatingScheduledFuture future = new DelegatingScheduledFuture<>(command, null, true);
    ScheduledFuture timerFuture =
        timer.scheduleWithFixedDelay(new HandOffTask(future), initialDelay, delay, unit);
    future.setDelegate(timerFuture);
    return future;
  }

  @Override
  public void shutdown() {
    // note - the timer has a "hook" which will shutdown our
    // worker pool once the timer is shutdown.
    timer.shutdown();
  }

  /**
   * Shutdown the executor immediately, returning a list of tasks that haven't been run. Like
   * ScheduledThreadPoolExecutor, this returns a list of RunnableScheduledFuture objects, instead of
   * the actual tasks submitted. However, these Future objects are even less useful than the ones
   * returned by ScheduledThreadPoolExecutor. In particular, they don't match the future returned by
   * the {{@link #submit(Runnable)} method, and the run method won't do anything useful. This list
   * should only be used as a count of the number of tasks that didn't execute.
   *
   * @see ScheduledThreadPoolExecutor#shutdownNow()
   */
  @Override
  public List<Runnable> shutdownNow() {
    List<Runnable> tasks = timer.shutdownNow();
    super.shutdownNow();
    return tasks;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long start = System.nanoTime();
    if (!timer.awaitTermination(timeout, unit)) {
      return false;
    }
    long elapsed = System.nanoTime() - start;
    long remaining = unit.toNanos(timeout) - elapsed;
    if (remaining < 0) {
      return false;
    }
    return super.awaitTermination(remaining, NANOSECONDS);
  }

  @Override
  public int getCorePoolSize() {
    return super.getCorePoolSize() + 1;
  }

  @Override
  public int getLargestPoolSize() {
    return super.getLargestPoolSize() + 1;
  }

  @Override
  public int getMaximumPoolSize() {
    return super.getMaximumPoolSize() + 1;
  }

  @Override
  public int getPoolSize() {
    return super.getPoolSize() + 1;
  }

  @Override
  public boolean isShutdown() {
    return timer.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return super.isTerminated() && timer.isTerminated();
  }

  public void setContinueExistingPeriodicTasksAfterShutdownPolicy(boolean b) {
    timer.setContinueExistingPeriodicTasksAfterShutdownPolicy(b);
  }

  public void setExecuteExistingDelayedTasksAfterShutdownPolicy(boolean b) {
    timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(b);
  }

  /**
   * A Runnable which we put in the timer which simply hands off the contain task for execution in
   * the thread pool when the timer fires.
   */
  private class HandOffTask implements Runnable {

    private final Runnable task;

    private HandOffTask(Runnable task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        ScheduledThreadPoolExecutorWithKeepAlive.super.execute(task);
      } catch (RejectedExecutionException e) {
        // do nothing, we'll only get this if we're shutting down.
      }
    }
  }


  /**
   * The future returned by the schedule* methods on this class. This future will not return a value
   * until the task has actually executed in the thread pool, but it allows us to cancel associated
   * timer task.
   */
  private static class DelegatingScheduledFuture<V>
      extends FutureTask<V>
      implements ScheduledFuture<V> {

    private final AtomicReference<ScheduledFuture<V>> delegate = new AtomicReference<>();
    private final boolean periodic;

    private DelegatingScheduledFuture(Runnable runnable, V result) {
      this(runnable, result, false);
    }

    private DelegatingScheduledFuture(Callable<V> callable) {
      this(callable, false);
    }

    private DelegatingScheduledFuture(Runnable runnable, V result, boolean periodic) {
      super(runnable, result);
      this.periodic = periodic;
    }

    private DelegatingScheduledFuture(Callable<V> callable, boolean periodic) {
      super(callable);
      this.periodic = periodic;
    }

    @Override
    public void run() {
      if (periodic) {
        runAndReset();
      } else {
        super.run();
      }
    }

    public void setDelegate(ScheduledFuture<V> future) {
      delegate.set(future);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      delegate.get().cancel(true);
      return super.cancel(mayInterruptIfRunning);
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return delegate.get().getDelay(unit);
    }

    @Override
    public int compareTo(Delayed o) {
      return delegate.get().compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
      return delegate.get().equals(o);
    }

    @Override
    public int hashCode() {
      return delegate.get().hashCode();
    }
  }

  /**
   * A RejectedExecutionHandler which causes the caller to block until there is space in the queue
   * for the task.
   */
  protected static class BlockCallerPolicy implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException("executor has been shutdown");
      }
      try {
        executor.getQueue().put(r);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException("interrupted", ie);
      }
    }
  }
}
