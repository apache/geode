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
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.util.concurrent.CountDownLatch;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;

/**
 * @author dsmith
 * 
 */
@Category(UnitTest.class)
public class UnsafeThreadLocalJUnitTest extends TestCase {

  /**
   * Test that we can get the value of a thread local from another thread.
   * 
   * @throws InterruptedException
   */
  public void test() throws InterruptedException {
    final UnsafeThreadLocal<String> utl = new UnsafeThreadLocal<String>();
    final CountDownLatch localSet = new CountDownLatch(1);

    Thread test = new Thread() {
      public void run() {
        utl.set("hello");
        localSet.countDown();
        try {
          Thread.sleep(100 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    test.setDaemon(true);
    test.start();
    localSet.await();
    assertEquals("hello", utl.get(test));
    assertEquals(null, utl.get(Thread.currentThread()));
  }

}
