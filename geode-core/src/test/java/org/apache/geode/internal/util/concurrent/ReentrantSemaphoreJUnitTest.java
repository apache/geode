/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.util.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ReentrantSemaphoreJUnitTest {

  private static final long OPERATION_TIMEOUT_MILLIS = 10 * 1000;

  private CountDownLatch done;
  private CountDownLatch acquired;

  @Rule
  public Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

  @Before
  public void setUp() throws Exception {
    done = new CountDownLatch(1);
    acquired = new CountDownLatch(2);
  }

  @After
  public void tearDown() throws Exception {
    acquired.countDown();
    done.countDown();
  }

  @Test
  public void testOneThread() throws Exception {
    final ReentrantSemaphore semaphore = new ReentrantSemaphore(2);
    semaphore.acquire();
    semaphore.acquire();
    assertEquals(1, semaphore.availablePermits());
    semaphore.release();
    semaphore.release();
    assertEquals(2, semaphore.availablePermits());
  }

  @Test
  public void testMultipleThreads() throws Exception {
    final ReentrantSemaphore sem = new ReentrantSemaphore(2);

    final AtomicReference<Throwable> failure = new AtomicReference<>();

    Thread t1 = new Thread() {
      public void run() {
        try {
          sem.acquire();
          sem.acquire();
          sem.acquire();
          acquired.countDown();
          assertTrue(done.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
          sem.release();
          sem.release();
          sem.release();
        } catch(Exception e) {
          failure.compareAndSet(null, e);
        }
      }
    };
    t1.start();
    
    Thread t2 = new Thread() {
      public void run() {
        try {
          sem.acquire();
          sem.acquire();
          sem.acquire();
          acquired.countDown();
          assertTrue(done.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
          sem.release();
          sem.release();
          sem.release();
        } catch(Exception e) {
          failure.compareAndSet(null, e);
        }
      }
    };
    t2.start();
    
    Thread t3 = new Thread() {
      public void run() {
        try {
          assertTrue(acquired.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
          assertEquals(0, sem.availablePermits());
          assertFalse(sem.tryAcquire(1, TimeUnit.SECONDS));
        } catch(Exception e) {
          failure.compareAndSet(null, e);
        }
      }
    };
    t3.start();
    
    t3.join(OPERATION_TIMEOUT_MILLIS);
    assertFalse(t3.isAlive());

    done.countDown();

    t2.join(OPERATION_TIMEOUT_MILLIS);
    assertFalse(t3.isAlive());

    t1.join(OPERATION_TIMEOUT_MILLIS);
    assertFalse(t1.isAlive());

    if (failure.get() != null) {
      throw new AssertionError(failure.get());
    }
    
    assertEquals(2, sem.availablePermits());
  }

}
