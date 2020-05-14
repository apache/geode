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
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ConcurrentLoopingThreads {
  private final int iterationCount;
  private final Consumer<Integer>[] functions;

  @SafeVarargs
  public ConcurrentLoopingThreads(int iterationCount,
      Consumer<Integer>... functions) {
    this.iterationCount = iterationCount;
    this.functions = functions;
  }

  public void run() {
    CyclicBarrier latch = new CyclicBarrier(functions.length);
    List<LoopingThread> loopingThreadStream = Arrays
        .stream(functions)
        .map((r) -> new LoopingThread(r, iterationCount, latch))
        .peek(Thread::start)
        .collect(Collectors.toList());

    loopingThreadStream.forEach(loopingThread -> {
      try {
        loopingThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

  }

  private static class LoopingRunnable implements Runnable {
    private final Consumer<Integer> runnable;
    private final int iterationCount;
    private final CyclicBarrier barrier;

    public LoopingRunnable(Consumer<Integer> runnable, int iterationCount,
        CyclicBarrier barrier) {
      this.runnable = runnable;
      this.iterationCount = iterationCount;
      this.barrier = barrier;
    }

    @Override
    public void run() {
      try {
        barrier.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
      for (int i = 0; i < iterationCount; i++) {
        runnable.accept(i);
        Thread.yield();
      }
    }
  }

  private static class LoopingThread extends Thread {
    public LoopingThread(Consumer<Integer> runnable, int iterationCount,
        CyclicBarrier barrier) {
      super(new LoopingRunnable(runnable, iterationCount, barrier));
    }
  }
}
