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
package com.gemstone.gemfire.internal.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class ReentrantSemaphoreJUnitTest extends TestCase {
  
  public void test() throws Throwable {
    final ReentrantSemaphore sem = new ReentrantSemaphore(2);
    
    sem.acquire();
    sem.acquire();
    assertEquals(1, sem.availablePermits());
    sem.release();
    sem.release();
    assertEquals(2, sem.availablePermits());
    
    final CountDownLatch testDone = new CountDownLatch(1);
    final CountDownLatch semsAquired = new CountDownLatch(2);
    
    final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
    
    Thread t1 = new Thread() {
      public void run() {
        try { 
          sem.acquire();
          sem.acquire();
          sem.acquire();
          semsAquired.countDown();
          testDone.await();
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
          semsAquired.countDown();
          testDone.await();
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
          semsAquired.await();
          assertEquals(0, sem.availablePermits());
          assertFalse(sem.tryAcquire(1, TimeUnit.SECONDS));
        } catch(Exception e) {
          failure.compareAndSet(null, e);
        }
      }
    };
    t3.start();
    
    t3.join();
    testDone.countDown();
    t2.join();
    t1.join();
    
    if(failure.get() != null) {
      throw failure.get();
    }
    
    assertEquals(2, sem.availablePermits());
  }

}
