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
package org.apache.geode.internal.cache.ha;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Test runs all tests of HARegionQueueJUnitTest using BlockingHARegionQueue instead of
 * HARegionQueue.
 * 
 * 
 */
@Category(IntegrationTest.class)
public class BlockingHARegionQueueJUnitTest extends HARegionQueueJUnitTest {

  /**
   * Creates Blocking HA region-queue object
   * 
   * @return Blocking HA region-queue object
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws CacheException
   * @throws InterruptedException
   */
  protected HARegionQueue createHARegionQueue(String name)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    HARegionQueue regionqueue =
        HARegionQueue.getHARegionQueueInstance(name, cache, HARegionQueue.BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

  /**
   * Creates Blocking HA region-queue object
   * 
   * @return Blocking HA region-queue object
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws CacheException
   * @throws InterruptedException
   */
  protected HARegionQueue createHARegionQueue(String name, HARegionQueueAttributes attrs)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name, cache, attrs,
        HARegionQueue.BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

  /**
   * Tests the effect of a put which is blocked because of capacity constraint & subsequent passage
   * because of take operation
   * 
   */
  @Test
  public void testBlockingPutAndTake()
      throws InterruptedException, IOException, ClassNotFoundException {
    HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
    hrqa.setBlockingQueueCapacity(1);
    final HARegionQueue hrq = this.createHARegionQueue("testBlockingPutAndTake", hrqa);
    hrq.setPrimary(true);// fix for 40314 - capacity constraint is checked for primary only.
    EventID id1 = new EventID(new byte[] {1}, 1, 1);
    hrq.put(new ConflatableObject("key1", "val1", id1, false, "testing"));
    Thread t1 = new Thread(new Runnable() {
      public void run() {
        try {
          EventID id2 = new EventID(new byte[] {1}, 1, 2);
          hrq.put(new ConflatableObject("key1", "val2", id2, false, "testing"));
        } catch (Exception e) {
          encounteredException = true;
        }
      }
    });
    t1.start();
    Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() -> t1.isAlive());
    Conflatable conf = (Conflatable) hrq.take();
    assertNotNull(conf);
    Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() -> !t1.isAlive());
  }

  /**
   * Test Scenario : BlockingQueue capacity is 1. The first put should be successful. The second put
   * should block till a peek/remove happens.
   * 
   */
  @Test
  public void testBlockingPutAndPeekRemove()
      throws InterruptedException, IOException, ClassNotFoundException {
    HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
    hrqa.setBlockingQueueCapacity(1);
    final HARegionQueue hrq = this.createHARegionQueue("testBlockingPutAndPeekRemove", hrqa);
    hrq.setPrimary(true);// fix for 40314 - capacity constraint is checked for primary only.
    EventID id1 = new EventID(new byte[] {1}, 1, 1);
    hrq.put(new ConflatableObject("key1", "val1", id1, false, "testing"));
    Thread t1 = new Thread(new Runnable() {
      public void run() {
        try {
          EventID id2 = new EventID(new byte[] {1}, 1, 2);
          hrq.put(new ConflatableObject("key1", "val2", id2, false, "testing"));
        } catch (Exception e) {
          encounteredException = true;
        }
      }
    });
    t1.start();
    Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() -> t1.isAlive());
    Conflatable conf = (Conflatable) hrq.peek();
    assertNotNull(conf);
    hrq.remove();
    Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() -> !t1.isAlive());
    assertFalse("Exception occured in put-thread", encounteredException);

  }

  /**
   * Test Scenario :Blocking Queue capacity is 1. The first put should be successful.The second put
   * should block till the first put expires.
   * 
   */
  // fix for 40314 - capacity constraint is checked for primary only and
  // expiry is not applicable on primary so marking this test as invalid.
  @Ignore
  @Test
  public void testBlockingPutAndExpiry()
      throws InterruptedException, IOException, ClassNotFoundException {
    HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
    hrqa.setBlockingQueueCapacity(1);
    hrqa.setExpiryTime(1);
    final HARegionQueue hrq = this.createHARegionQueue("testBlockingPutAndExpiry", hrqa);

    EventID id1 = new EventID(new byte[] {1}, 1, 1);
    long start = System.currentTimeMillis();
    hrq.put(new ConflatableObject("key1", "val1", id1, false, "testing"));
    Thread t1 = new Thread(new Runnable() {
      public void run() {
        try {
          EventID id2 = new EventID(new byte[] {1}, 1, 2);
          hrq.put(new ConflatableObject("key1", "val2", id2, false, "testing"));
        } catch (Exception e) {
          encounteredException = true;
        }
      }
    });
    t1.start();
    Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() -> t1.isAlive());
    waitAtLeast(1000, start, () -> {
      assertFalse("Put-thread blocked unexpectedly", t1.isAlive());
    });
    assertFalse("Exception occured in put-thread", encounteredException);
  }
}
