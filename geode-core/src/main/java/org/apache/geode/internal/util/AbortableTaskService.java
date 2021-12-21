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
package org.apache.geode.internal.util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps an executor with a task queue so that currently executing tasks can be aborted.
 */
public class AbortableTaskService {
  /** the executor */
  private final Executor exec;

  /** the queue of executing tasks */
  private final Queue<AbortingRunnable> tasks;

  /**
   * Provides an executable interface that can be aborted during execution.
   */
  public interface AbortableTask {
    /**
     * Invoked to execute the task. The task implementation should periodically check the aborted
     * flag and take appropriate action.
     *
     * @param aborted set to true when the task has been aborted
     */
    void runOrAbort(AtomicBoolean aborted);

    /**
     * Invoked when a task is aborted prior to execution.
     */
    void abortBeforeRun();
  }

  public AbortableTaskService(Executor exec) {
    this.exec = exec;
    tasks = new ConcurrentLinkedQueue<AbortingRunnable>();
  }

  /**
   * Executes the task using the embedded executor.
   *
   * @param task the task to execute
   */
  public void execute(AbortableTask task) {
    AbortingRunnable ar = new AbortingRunnable(task);
    tasks.add(ar);

    try {
      exec.execute(ar);

    } catch (RejectedExecutionException e) {
      tasks.remove(ar);
      throw e;
    }
  }

  /**
   * Aborts all executing tasks.
   */
  public void abortAll() {
    for (AbortingRunnable ar : tasks) {
      ar.abort();
    }
  }

  /**
   * Waits for all currently executing tasks to complete.
   */
  public void waitForCompletion() {
    boolean interrupted = false;
    for (AbortingRunnable ar : tasks) {
      try {
        ar.waitForCompletion();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns true if all tasks are done or aborted.
   */
  public boolean isCompleted() {
    for (AbortingRunnable ar : tasks) {
      synchronized (ar) {
        if (!ar.done) {
          return false;
        }
      }
    }
    return true;
  }

  private class AbortingRunnable implements Runnable {
    /** the task to execute */
    private final AbortableTask task;

    /** true if the task is aborted */
    private final AtomicBoolean aborted;

    /** true if the task has begun */
    private final AtomicBoolean hasStarted;

    /** true if the task is complete */
    private boolean done;

    public AbortingRunnable(AbortableTask task) {
      this.task = task;

      aborted = new AtomicBoolean(false);
      hasStarted = new AtomicBoolean(false);

      done = false;
    }

    private synchronized void waitForCompletion() throws InterruptedException {
      while (!done) {
        wait();
      }
    }

    private synchronized void signalDone() {
      done = true;
      notifyAll();
    }

    private void abort() {
      aborted.set(true);
      if (hasStarted.compareAndSet(false, true)) {
        try {
          task.abortBeforeRun();
        } finally {
          tasks.remove(this);
          signalDone();
        }
      }
    }

    @Override
    public void run() {
      if (hasStarted.compareAndSet(false, true)) {
        try {
          task.runOrAbort(aborted);
        } finally {
          tasks.remove(this);
          signalDone();
        }
      }
    }
  }
}
