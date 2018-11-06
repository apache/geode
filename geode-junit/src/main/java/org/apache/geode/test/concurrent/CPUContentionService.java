package org.apache.geode.test.concurrent;

import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
  private final List<Future<?>> tasks = new ArrayList<>();
  private final AtomicReference<Future<?>> stopWatcher = new AtomicReference<>();
  private final ExecutorService executor;
  private final int threadCount;
  private final Set<BigInteger> primes = synchronizedSet(new HashSet<>());

  public CPUContentionService(int numberOfThreads) {
    this.executor = newFixedThreadPool(numberOfThreads);
    this.threadCount = numberOfThreads;
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
      tasks.add(executor.submit(generatePrimes(primes)));
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

  private static Runnable generatePrimes(Set<BigInteger> primes) {
    return () -> {
      Random random = new Random();
      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          return;
        }
        primes.add(BigInteger.probablePrime(1000, random));
      }
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
