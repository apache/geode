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

package org.apache.geode.redis;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ConcurrentLoopingThreads {
  private final int iterationCount;
  private final Consumer<Integer>[] functions;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private List<Future<?>> loopingFutures;
  private Throwable actionThrowable = null;

  @SafeVarargs
  public ConcurrentLoopingThreads(int iterationCount,
      Consumer<Integer>... functions) {
    this.iterationCount = iterationCount;
    this.functions = functions;
  }

  /**
   * Start the operations asynchronously. Use {@link #await()} to wait for completion.
   */
  public ConcurrentLoopingThreads start() {
    return start(false, null);
  }

  private ConcurrentLoopingThreads start(boolean lockstep, Runnable barrierAction) {
    CyclicBarrier barrier = new CyclicBarrier(functions.length, barrierAction);

    loopingFutures = Arrays
        .stream(functions)
        .map(r -> new LoopingThread(r, iterationCount, barrier, lockstep))
        .map(t -> executorService.submit(t))
        .collect(Collectors.toList());

    return this;
  }

  /**
   * Wait for all operations to complete. Will propagate the first exception thrown by any of the
   * operations.
   */
  public void await() {
    boolean timeOutExceptionThrown;
    do {
      timeOutExceptionThrown = false;
      for (Future<?> loopingThread : loopingFutures) {
        try {
          loopingThread.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          timeOutExceptionThrown = true;
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    } while (timeOutExceptionThrown);
  }

  /**
   * Start operations and only return once all are complete.
   */
  public void run() {
    start(false, null);
    await();
  }

  /**
   * Start operations and run each iteration in lockstep
   */
  public void runInLockstep() {
    start(true, null);
    await();
  }

  /**
   * Start operations and provide an action to be performed at the end of every iteration. This
   * implies running in lockstep. This would typically be used to provide some form of validation.
   */
  public void runWithAction(Runnable action) {
    Runnable innerRunnable = () -> {
      try {
        action.run();
      } catch (Throwable e) {
        actionThrowable = e;
        throw e;
      }
    };

    start(true, innerRunnable);

    try {
      await();
    } catch (Throwable e) {
      if (actionThrowable != null) {
        // This will ensure that AssertionErrors are clearly apparent
        if (actionThrowable instanceof Error) {
          throw (Error) actionThrowable;
        }
        throw new RuntimeException(actionThrowable);
      } else {
        throw e;
      }
    }
  }

  private static class LoopingRunnable implements Runnable {
    private final Consumer<Integer> runnable;
    private final int iterationCount;
    private final CyclicBarrier barrier;
    private final boolean lockstep;

    public LoopingRunnable(Consumer<Integer> runnable, int iterationCount,
        CyclicBarrier barrier, boolean lockstep) {
      this.runnable = runnable;
      this.iterationCount = iterationCount;
      this.barrier = barrier;
      this.lockstep = lockstep;
    }

    @Override
    public void run() {
      if (!lockstep) {
        waitForBarrier();
      }
      for (int i = 0; i < iterationCount; i++) {
        runnable.accept(i);
        if (lockstep) {
          waitForBarrier();
        }
      }
    }

    private void waitForBarrier() {
      try {
        barrier.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class LoopingThread extends Thread {
    public LoopingThread(Consumer<Integer> runnable, int iterationCount,
        CyclicBarrier barrier, boolean lockstep) {
      super(new LoopingRunnable(runnable, iterationCount, barrier, lockstep));
    }
  }
}
