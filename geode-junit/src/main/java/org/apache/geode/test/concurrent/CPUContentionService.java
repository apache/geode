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
package org.apache.geode.test.concurrent;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Keeps CPUs busy by searching for probable primes.
 *
 * <h3>Lifecycle</h3>
 *
 * 1: Initializing the contention service
 *
 * <p>
 * The constructor creates an executor service with the specified number of threads. Initially, the
 * threads are idle.
 *
 * <p>
 * The executor service remains active until you call {@code shutDown()}.
 *
 * <p>
 * 2: Starting contention
 *
 * <p>
 * Each of these methods starts all threads executing, each contending for CPUs.
 * <ul>
 * <li>{@link #runFor(Duration)}</li>
 * <li>{@link #runUntil(Instant)}</li>
 * <li>{@link #runUntil(BooleanSupplier)}</li>
 * <li>{@link #runUntilStopped()}</li>
 * </ul>
 *
 * <p>
 * 3: Stopping contention
 *
 * <p>
 * If you call {@code runUntilStopped()}, the threads remain active until you call {@code stop()}.
 *
 * <p>
 * Each of the other methods will stop the threads when the specified condition is satisfied.
 *
 * <p>
 * Stopping the threads does not shut down the underlying thread pool executor.
 *
 * <p>
 * 4: Shutting down the contention service
 *
 * <p>
 * When you're done with this service, you must call {@code shutDown(duration)} to close the thread
 * pool.
 */
public class CPUContentionService {
  private final List<Future<BigInteger>> tasks = new ArrayList<>();
  private final AtomicReference<Future<?>> stopWatcher = new AtomicReference<>();
  private final ExecutorService executor;
  private final int threadCount;

  public CPUContentionService(int numberOfThreads) {
    executor = newFixedThreadPool(numberOfThreads);
    threadCount = numberOfThreads;
  }

  public void runUntil(Instant stopTime) {
    runUntil(() -> Instant.now().isAfter(stopTime));
  }

  public void runFor(Duration duration) {
    runUntil(Instant.now().plus(duration));
  }

  /**
   * Runs all threads until {@code stop()} or {@code shutDown()} are called.
   */
  public void runUntilStopped() {
    runUntil(() -> false);
  }

  public synchronized void runUntil(BooleanSupplier stopCondition) {
    if (!tasks.isEmpty()) {
      throw new IllegalStateException("Only one " + getClass().getSimpleName()
          + " can be in progress at a time");
    }
    stopWatcher.set(executor.submit(watchForStop(stopCondition)));
    for (int i = 1; i < threadCount; i++) {
      tasks.add(executor.submit(generateProbablePrimes()));
    }
  }

  /**
   * Cancels all current running threads.
   * The thread pool remains open, and this contention service can be reused.
   */
  public synchronized void stop() {
    tasks.forEach(task -> task.cancel(true));
    tasks.clear();
    stopWatcher.getAndSet(null).cancel(true);
  }

  /**
   * Cancels current running threads and closes the thread pool.
   * This contention service can no longer be used.
   */
  public synchronized void shutDown(Duration duration) throws InterruptedException {
    executor.shutdownNow();
    executor.awaitTermination(duration.toMillis(), MILLISECONDS);
    tasks.clear();
  }

  private static Callable<BigInteger> generateProbablePrimes() {
    return () -> {
      Random random = new Random();
      // To keep the CPU busy, we must prevent the Java and JIT compilers from optimizing this loop
      // out of existence. To do that, we save the the most recently generated prime, and return it
      // when the task is cancelled. Our assumption is that the compilers can't predict that the
      // return value is unused, and is therefore forced to calculate a new prime on each iteration.
      BigInteger prime = BigInteger.valueOf(0);
      while (!Thread.currentThread().isInterrupted()) {
        prime = BigInteger.probablePrime(1000, random);
      }
      return prime;
    };
  }

  private Runnable watchForStop(BooleanSupplier stopCondition) {
    return () -> {
      while (!Thread.currentThread().isInterrupted() && !stopCondition.getAsBoolean()) {
      }
      stop();
    };
  }
}
