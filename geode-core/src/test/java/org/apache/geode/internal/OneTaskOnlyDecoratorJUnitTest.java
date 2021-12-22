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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;


public class OneTaskOnlyDecoratorJUnitTest {

  /**
   * Test to make sure we only execute the task once no matter how many times we schedule it.
   */
  @Test
  public void testExecuteOnlyOnce() throws Exception {
    ScheduledExecutorService ex = Executors.newScheduledThreadPool(1);

    MyConflationListener listener = new MyConflationListener();
    OneTaskOnlyExecutor decorator = new OneTaskOnlyExecutor(ex, listener, null);

    final CountDownLatch latch = new CountDownLatch(1);
    ex.submit((Callable) () -> {
      latch.await();
      return null;
    });

    final AtomicInteger counter = new AtomicInteger();

    Runnable increment = () -> counter.incrementAndGet();

    for (int i = 0; i < 50; i++) {
      decorator.schedule(increment, 0, TimeUnit.SECONDS);
    }

    assertEquals(0, counter.get());
    latch.countDown();
    ex.shutdown();
    ex.awaitTermination(60, TimeUnit.SECONDS);
    assertEquals(1, counter.get());
    assertEquals(49, listener.getDropCount());
  }

  /**
   * Test to make sure we reschedule the task for execution if it has already in progress.
   */
  @Test
  public void testReschedule() throws Exception {
    ScheduledExecutorService ex = Executors.newScheduledThreadPool(1);
    OneTaskOnlyExecutor decorator = new OneTaskOnlyExecutor(ex, null);

    final CountDownLatch taskRunning = new CountDownLatch(1);
    final CountDownLatch continueTask = new CountDownLatch(1);
    final AtomicInteger counter = new AtomicInteger();

    Callable waitForLatch = () -> {
      taskRunning.countDown();
      continueTask.await();
      counter.incrementAndGet();
      return null;
    };

    Runnable increment = () -> counter.incrementAndGet();

    decorator.schedule(waitForLatch, 0, TimeUnit.SECONDS);
    taskRunning.await();
    decorator.schedule(increment, 0, TimeUnit.SECONDS);

    assertEquals(0, counter.get());
    continueTask.countDown();

    ex.shutdown();
    ex.awaitTermination(60, TimeUnit.SECONDS);
    assertEquals(2, counter.get());
  }

  /**
   * Test to make sure we reschedule the task for execution if the new requested execution is
   * earlier than the previous one
   */
  @Test
  public void testRescheduleForEarlierTime() throws Exception {
    ScheduledExecutorService ex = Executors.newScheduledThreadPool(1);
    MyConflationListener listener = new MyConflationListener();
    OneTaskOnlyExecutor decorator = new OneTaskOnlyExecutor(ex, listener, null);

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger counter = new AtomicInteger();

    Runnable increment = () -> counter.incrementAndGet();

    decorator.schedule(increment, 120, TimeUnit.SECONDS);
    decorator.schedule(increment, 10, TimeUnit.MILLISECONDS);

    long start = System.nanoTime();

    ex.shutdown();
    ex.awaitTermination(60, TimeUnit.SECONDS);
    long elapsed = System.nanoTime() - start;
    assertEquals(1, counter.get());
    assertEquals(1, listener.getDropCount());
    assertTrue(elapsed < TimeUnit.SECONDS.toNanos(120));
  }

  private static class MyConflationListener
      extends OneTaskOnlyExecutor.ConflatedTaskListenerAdapter {
    private int dropCount;

    @Override
    public void taskDropped() {
      dropCount++;
    }

    public int getDropCount() {
      return dropCount;
    }
  }
}
