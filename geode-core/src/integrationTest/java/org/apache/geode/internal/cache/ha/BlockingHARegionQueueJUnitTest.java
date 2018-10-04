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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Test runs all tests of HARegionQueueJUnitTest using BlockingHARegionQueue instead of
 * HARegionQueue.
 */
@Category({ClientSubscriptionTest.class})
public class BlockingHARegionQueueJUnitTest extends HARegionQueueJUnitTest {

  @Override
  protected int queueType() {
    return HARegionQueue.BLOCKING_HA_QUEUE;
  }

  /**
   * Tests the effect of a put which is blocked because of capacity constraint & subsequent passage
   * because of take operation
   */
  @Test
  public void testBlockingPutAndTake() throws Exception {
    HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
    hrqa.setBlockingQueueCapacity(1);

    HARegionQueue hrq = createHARegionQueue(testName.getMethodName(), hrqa);
    hrq.setPrimary(true); // fix for 40314 - capacity constraint is checked for primary only.

    EventID id1 = new EventID(new byte[] {1}, 1, 1);
    hrq.put(new ConflatableObject("key1", "val1", id1, false, "testing"));

    AtomicBoolean threadStarted = new AtomicBoolean(false);

    Thread thread = new Thread(() -> {
      try {
        threadStarted.set(true);
        EventID id2 = new EventID(new byte[] {1}, 1, 2);
        hrq.put(new ConflatableObject("key1", "val2", id2, false, "testing"));
      } catch (InterruptedException e) {
        errorCollector.addError(e);
      }
    });
    thread.start();

    await().until(() -> threadStarted.get());

    Conflatable conf = (Conflatable) hrq.take();
    assertThat(conf, notNullValue());

    await().until(() -> !thread.isAlive());
  }

  /**
   * Test Scenario : BlockingQueue capacity is 1. The first put should be successful. The second put
   * should block till a peek/remove happens.
   */
  @Test
  public void testBlockingPutAndPeekRemove() throws Exception {
    HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
    hrqa.setBlockingQueueCapacity(1);

    HARegionQueue hrq = createHARegionQueue(testName.getMethodName(), hrqa);
    hrq.setPrimary(true);// fix for 40314 - capacity constraint is checked for primary only.

    EventID id1 = new EventID(new byte[] {1}, 1, 1);
    hrq.put(new ConflatableObject("key1", "val1", id1, false, "testing"));

    AtomicBoolean threadStarted = new AtomicBoolean(false);

    Thread thread = new Thread(() -> {
      try {
        threadStarted.set(true);
        EventID id2 = new EventID(new byte[] {1}, 1, 2);
        hrq.put(new ConflatableObject("key1", "val2", id2, false, "testing"));
      } catch (Exception e) {
        errorCollector.addError(e);
      }
    });
    thread.start();

    await().until(() -> threadStarted.get());

    Conflatable conf = (Conflatable) hrq.peek();
    assertThat(conf, notNullValue());

    hrq.remove();

    await().until(() -> !thread.isAlive());
  }

  /**
   * Test Scenario :Blocking Queue capacity is 1. The first put should be successful.The second put
   * should block till the first put expires.
   * <p>
   * fix for 40314 - capacity constraint is checked for primary only and expiry is not applicable on
   * primary so marking this test as invalid.
   */
  @Test
  public void testBlockingPutAndExpiry() throws Exception {
    HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
    hrqa.setBlockingQueueCapacity(1);
    hrqa.setExpiryTime(1);

    HARegionQueue hrq = createHARegionQueue(testName.getMethodName(), hrqa);

    EventID id1 = new EventID(new byte[] {1}, 1, 1);

    hrq.put(new ConflatableObject("key1", "val1", id1, false, "testing"));

    AtomicBoolean threadStarted = new AtomicBoolean(false);

    Thread thread = new Thread(() -> {
      try {
        threadStarted.set(true);
        EventID id2 = new EventID(new byte[] {1}, 1, 2);
        hrq.put(new ConflatableObject("key1", "val2", id2, false, "testing"));
      } catch (Exception e) {
        errorCollector.addError(e);
      }
    });
    thread.start();

    await().until(() -> threadStarted.get());

    await().until(() -> !thread.isAlive());
  }
}
