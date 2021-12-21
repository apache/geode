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

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.geode.internal.monitoring.ThreadsMonitoring;

/**
 * A decorator for a ScheduledExecutorService which tries to make sure that there is only one task
 * in the queue for the executor service that has been submitted through this decorator.
 *
 * This class is useful if you have a task that you want to make sure runs at least once after an
 * event, but if the event happens repeatedly before the task runs, the task should still only run
 * once.
 *
 * In many cases it might make sense to have multiple decorators, or to submit one kind of task
 * through the decorator and other tasks directly through the executor.
 *
 * For example, a task that recovers redundancy for all buckets of a PR when a member crashes only
 * needs to be run once, no matter how many members crash before the task starts. But after the task
 * has started, we want to make sure we schedule another execution for the next crash.
 *
 * If the a new task is scheduled to run sooner than the task that is currently in the queue, the
 * currently queued task will be canceled and the new task will be submitted to the queue with the
 * new time.
 *
 * @since GemFire 6.0
 */
@SuppressWarnings("synthetic-access")
public class OneTaskOnlyExecutor {

  private final ThreadsMonitoring threadMonitoring;
  private final ScheduledExecutorService ex;
  private ScheduledFuture<?> future = null;
  private final ConflatedTaskListener listener;

  public OneTaskOnlyExecutor(ScheduledExecutorService ex, ThreadsMonitoring tMonitoring) {
    this(ex, new ConflatedTaskListenerAdapter(), tMonitoring);
  }

  public OneTaskOnlyExecutor(ScheduledExecutorService ex, ConflatedTaskListener listener,
      ThreadsMonitoring tMonitoring) {
    this.ex = ex;
    this.listener = listener;
    threadMonitoring = tMonitoring;
  }

  /**
   * Schedule an execution of a task. This will either add the task to the execution service, or if
   * a task has already been scheduled through this decorator and is still pending execution it will
   * return the future associated with the previously scheduled task.
   *
   * @param runnable a runnable to execution
   * @param delay the time to delay before execution
   * @param unit the time unit
   * @return The future associated with this task, or with a previously scheduled task if that task
   *         has not yet been run.
   * @see ScheduledExecutorService#schedule(Runnable, long, TimeUnit)
   */
  public ScheduledFuture<?> schedule(Runnable runnable, long delay, TimeUnit unit) {
    synchronized (this) {
      if (future == null || future.isCancelled() || future.isDone()
          || future.getDelay(unit) > delay) {
        if (future != null && !future.isDone()) {
          future.cancel(false);
          listener.taskDropped();
        }
        future = ex.schedule(new DelegatingRunnable(runnable), delay, unit);
      } else {
        listener.taskDropped();
      }
    }
    return future;
  }

  /**
   * Schedule an execution of a task. This will either add the task to the execution service, or if
   * a task has already been scheduled through this decorator and is still pending execution it will
   * return the future associated with the previously scheduled task.
   *
   * @param callable a callable to execute
   * @param delay the time to delay before execution
   * @param unit the time unit
   * @return The future associated with this task, or with a previously scheduled task if that task
   *         has not yet been run.
   * @see ScheduledExecutorService#schedule(Runnable, long, TimeUnit)
   */
  public <T> ScheduledFuture<?> schedule(Callable<T> callable, long delay, TimeUnit unit) {
    synchronized (this) {
      if (future == null || future.isCancelled() || future.isDone()
          || future.getDelay(unit) > delay) {
        if (future != null && !future.isDone()) {
          future.cancel(false);
          listener.taskDropped();
        }
        future = ex.schedule(new DelegatingCallable<>(callable), delay, unit);
      } else {
        listener.taskDropped();
      }
    }
    return future;
  }

  /**
   * Removes the canceled tasks from the executor queue.
   */
  public void purge() {
    ((ScheduledThreadPoolExecutor) ex).purge();
  }

  private class DelegatingRunnable implements Runnable {
    private final Runnable runnable;

    public DelegatingRunnable(Runnable runnable) {
      this.runnable = runnable;
    }

    @Override
    public void run() {
      synchronized (OneTaskOnlyExecutor.this) {
        future = null;
      }
      beforeExecute();
      try {
        runnable.run();
      } finally {
        afterExecute();
      }
    }
  }

  private class DelegatingCallable<T> implements Callable<T> {
    private final Callable<T> callable;

    public DelegatingCallable(Callable<T> callable) {
      this.callable = callable;
    }

    @Override
    public T call() throws Exception {
      synchronized (OneTaskOnlyExecutor.this) {
        future = null;
      }
      return callable.call();
    }
  }

  public interface ConflatedTaskListener {
    void taskDropped();
  }

  public static class ConflatedTaskListenerAdapter implements ConflatedTaskListener {
    @Override
    public void taskDropped() {

    }
  }

  protected void beforeExecute() {
    if (threadMonitoring != null) {
      threadMonitoring.startMonitor(ThreadsMonitoring.Mode.OneTaskOnlyExecutor);
    }
  }

  protected void afterExecute() {
    if (threadMonitoring != null) {
      threadMonitoring.endMonitor();
    }
  }
}
