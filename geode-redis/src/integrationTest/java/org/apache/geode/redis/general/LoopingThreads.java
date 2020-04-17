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

package org.apache.geode.redis.general;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Stream;

public class LoopingThreads {
  private final int iterationCount;
  private final Function<Integer, Object>[] functions;

  public LoopingThreads(int iterationCount,
      Function<Integer, Object>... functions) {
    this.iterationCount = iterationCount;
    this.functions = functions;
  }

  public void run() {
    CountDownLatch latch = new CountDownLatch(1);
    Stream<LoopingThread> loopingThreadStream = Arrays
        .stream(functions)
        .map((r) -> new LoopingThread(r, iterationCount, latch))
        .map((t) -> {
          t.start();
          return t;
        });

    latch.countDown();

    loopingThreadStream.forEach(loopingThread -> {
      try {
        loopingThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

  }

  private class LoopingRunnable implements Runnable {
    private final Function<Integer, Object> runnable;
    private final int iterationCount;
    private CountDownLatch startLatch;

    public LoopingRunnable(Function<Integer, Object> runnable, int iterationCount,
        CountDownLatch startLatch) {
      this.runnable = runnable;
      this.iterationCount = iterationCount;
      this.startLatch = startLatch;
    }

    @Override
    public void run() {
      try {
        startLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      for (int i = 0; i < iterationCount; i++) {
        runnable.apply(i);
        Thread.yield();
      }
    }
  }

  private class LoopingThread extends Thread {
    public LoopingThread(Function<Integer, Object> runnable, int iterationCount,
        CountDownLatch latch) {
      super(new LoopingRunnable(runnable, iterationCount, latch));
    }
  }
}
