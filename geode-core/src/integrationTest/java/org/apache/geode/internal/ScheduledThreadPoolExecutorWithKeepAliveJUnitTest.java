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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;


public class ScheduledThreadPoolExecutorWithKeepAliveJUnitTest {

  ScheduledThreadPoolExecutorWithKeepAlive ex;

  @After
  public void tearDown() throws Exception {
    ex.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    ex.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    ex.shutdownNow();
    assertTrue(ex.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testFuture() throws InterruptedException, ExecutionException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(5, 60, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);
    final AtomicBoolean done = new AtomicBoolean();
    Future f = ex.submit(new Runnable() {
      public void run() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
          fail("interrupted");
        }
        done.set(true);
      }
    });
    f.get();
    assertTrue("Task did not complete", done.get());

    Thread.sleep(2000); // let the thread finish with the task

    f = ex.submit(new Callable() {
      public Object call() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
        return Boolean.TRUE;
      }
    });
    assertTrue("Task did not complete", ((Boolean) f.get()).booleanValue());

    assertEquals(2, ex.getLargestPoolSize());
  }

  @Test
  public void testConcurrentExecutionAndExpiration()
      throws InterruptedException, ExecutionException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(50, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);

    Runnable waitForABit = new Runnable() {
      public void run() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    };

    Future[] futures = new Future[50];
    for (int i = 0; i < 50; i++) {
      futures[i] = ex.submit(waitForABit);
    }
    long start = System.nanoTime();

    for (int i = 0; i < 50; i++) {
      futures[i].get();
    }
    long end = System.nanoTime();

    assertTrue("Tasks executed in parallel", TimeUnit.NANOSECONDS.toSeconds(end - start) < 50);

    assertEquals(50, ex.getLargestPoolSize());

    // now make sure we expire back down.
    Thread.sleep(5000);
    assertEquals(1, ex.getPoolSize());
  }

  @Test
  public void testConcurrentRepeatedTasks() throws InterruptedException, ExecutionException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(50, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);

    final AtomicInteger counter = new AtomicInteger();
    Runnable waitForABit = new Runnable() {
      public void run() {
        try {
          counter.incrementAndGet();
          Thread.sleep(500);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    };

    Future[] futures = new Future[50];
    for (int i = 0; i < 50; i++) {
      futures[i] = ex.scheduleAtFixedRate(waitForABit, 0, 1, TimeUnit.SECONDS);
    }

    Thread.sleep(10000);

    for (int i = 0; i < 50; i++) {
      futures[i].cancel(true);
    }

    System.err.println("Counter = " + counter);
    assertTrue("Tasks did not execute in parallel. Expected more than 300 executions, got "
        + counter.get(), counter.get() > 300);

    assertEquals(50, ex.getLargestPoolSize());

    // now make sure we expire back down.
    Thread.sleep(5000);
    assertEquals(1, ex.getPoolSize());
  }

  /**
   * time, in nanoseconds, that we should tolerate as slop (evidently needed for windows)
   */
  private static final long SLOP = TimeUnit.MILLISECONDS.toNanos(20);

  @Test
  public void testDelayedExcecution() throws InterruptedException, ExecutionException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(50, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);
    long start = System.nanoTime();
    Future f = ex.schedule(new Runnable() {
      public void run() {}
    }, 10, TimeUnit.SECONDS);
    f.get();
    long end = System.nanoTime();
    assertTrue("Execution was not delayed 10 seconds, only " + (end - start),
        TimeUnit.SECONDS.toNanos(10) <= end - start + SLOP);
  }

  @Test
  public void testRepeatedExecution() throws InterruptedException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(50, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);

    final AtomicInteger counter = new AtomicInteger();
    Runnable run = new Runnable() {
      public void run() {
        counter.incrementAndGet();
      }
    };
    ScheduledFuture f = ex.scheduleAtFixedRate(run, 0, 1, TimeUnit.SECONDS);
    await()
        .untilAsserted(
            () -> assertEquals("Task was not executed repeatedly", true, counter.get() > 1));
    await()
        .untilAsserted(() -> assertEquals("The task could not be cancelled", true, f.cancel(true)));
    await()
        .untilAsserted(
            () -> assertEquals("Task was not cancelled within 30 sec", true, f.isCancelled()));
    int oldValue = counter.get();
    Thread.sleep(5000);
    assertEquals("Task was not cancelled", oldValue, counter.get());
  }

  @Test
  public void testShutdown() throws InterruptedException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(50, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);
    ex.schedule(new Runnable() {
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    }, 2, TimeUnit.SECONDS);
    ex.shutdown();
    long start = System.nanoTime();
    assertTrue(ex.awaitTermination(10, TimeUnit.SECONDS));
    long elapsed = System.nanoTime() - start;
    assertTrue("Shutdown did not wait to task to complete. Only waited "
        + TimeUnit.NANOSECONDS.toMillis(elapsed), TimeUnit.SECONDS.toNanos(4) < elapsed + SLOP);
  }

  @Test
  public void testShutdown2() throws InterruptedException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(50, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);
    ex.submit(new Runnable() {
      public void run() {
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    });
    // give it a chance to get in the worker pool
    Thread.sleep(500);
    ex.shutdown();
    long start = System.nanoTime();
    assertTrue(ex.awaitTermination(10, TimeUnit.SECONDS));
    long elapsed = System.nanoTime() - start;
    assertTrue("Shutdown did not wait to task to complete. Only waited "
        + TimeUnit.NANOSECONDS.toMillis(elapsed), TimeUnit.SECONDS.toNanos(2) < elapsed);
  }

  @Test
  public void testShutdownNow() throws InterruptedException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(50, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);
    ex.schedule(new Runnable() {
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    }, 2, TimeUnit.SECONDS);
    ex.shutdownNow();
    long start = System.nanoTime();
    assertTrue(ex.awaitTermination(1, TimeUnit.SECONDS));
    long elapsed = System.nanoTime() - start;
    assertTrue(
        "ShutdownNow should not have waited. Waited " + TimeUnit.NANOSECONDS.toMillis(elapsed),
        TimeUnit.SECONDS.toNanos(2) > elapsed);
  }

  @Test
  public void testShutdownNow2() throws InterruptedException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(50, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);
    ex.submit(new Runnable() {
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    });
    // give it a chance to get in the worker pool.
    Thread.sleep(500);
    ex.shutdownNow();
    long start = System.nanoTime();
    assertTrue(ex.awaitTermination(1, TimeUnit.SECONDS));
    long elapsed = System.nanoTime() - start;
    assertTrue(
        "ShutdownNow should not have waited. Waited " + TimeUnit.NANOSECONDS.toMillis(elapsed),
        TimeUnit.SECONDS.toNanos(2) > elapsed);
  }

  @Test
  public void testShutdownDelayedTasks() throws InterruptedException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(50, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);
    ex.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    ex.schedule(new Runnable() {
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    }, 5000, TimeUnit.MILLISECONDS);
    ex.shutdown();
    long start = System.nanoTime();
    assertTrue(ex.awaitTermination(30, TimeUnit.SECONDS));
    long elapsed = System.nanoTime() - start;
    assertTrue("Shutdown should not have waited. Waited " + TimeUnit.NANOSECONDS.toMillis(elapsed),
        TimeUnit.SECONDS.toNanos(2) > elapsed);
  }

  @Test
  public void testAllWorkersActive() throws InterruptedException {
    ex = new ScheduledThreadPoolExecutorWithKeepAlive(6, 1, TimeUnit.SECONDS,
        Executors.defaultThreadFactory(), null);
    final AtomicInteger counter = new AtomicInteger();

    long start = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      ex.submit(new Runnable() {
        public void run() {
          try {
            Thread.sleep(500);
            counter.incrementAndGet();
          } catch (InterruptedException e) {
            fail("interrupted");
          }
        }
      });
    }

    long elapsed = System.nanoTime() - start;
    assertTrue("calling ex.submit blocked the caller", TimeUnit.SECONDS.toNanos(1) > elapsed);

    Thread.sleep(20 * 500 + 1000);

    assertEquals(100, counter.get());
    assertEquals(6, ex.getMaximumPoolSize());
  }
}
