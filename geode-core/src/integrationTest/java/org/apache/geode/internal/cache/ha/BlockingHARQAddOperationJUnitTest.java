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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Test runs all tests of HARQAddOperationJUnitTest using BlockingHARegionQueue instead of
 * HARegionQueue
 *
 *
 */
@Category({ClientSubscriptionTest.class})
public class BlockingHARQAddOperationJUnitTest extends HARQAddOperationJUnitTest {

  /**
   * Creates Blocking HA region-queue object
   *
   * @return Blocking HA region-queue object
   */
  @Override
  protected HARegionQueue createHARegionQueue(String name)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    HARegionQueue regionqueue =
        HARegionQueue.getHARegionQueueInstance(name, cache, HARegionQueue.BLOCKING_HA_QUEUE, false,
            disabledClock());
    return regionqueue;
  }

  /**
   * Creates Blocking HA region-queue object
   *
   * @return Blocking HA region-queue object
   */
  @Override
  protected HARegionQueue createHARegionQueue(String name, HARegionQueueAttributes attrs)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name, cache, attrs,
        HARegionQueue.BLOCKING_HA_QUEUE, false, disabledClock());
    return regionqueue;
  }

  /**
   * Tests the take() functionality of <code>BlockingHARegionQueue<code> with conflation disabled.
   *
   */
  @Test
  public void testBlockingTakeConflationDisabled() throws Exception {
    logWriter.info("HARQAddOperationJUnitTest : testBlockingTakeConflationDisabled BEGIN");

    doBlockingTake(false);
    logWriter.info("HARQAddOperationJUnitTest : testBlockingTakeConflationDisabled END");
  }

  /**
   * Tests the take() functionality of <code>BlockingHARegionQueue<code> with conflation enabled.
   *
   *
   */
  @Test
  public void testBlockingTakeConflationEnabled() throws Exception {
    logWriter.info("HARQAddOperationJUnitTest : testBlockingTakeConflationEnabled BEGIN");

    doBlockingTake(true);
    logWriter.info("HARQAddOperationJUnitTest : testBlockingTakeConflationEnabled END");
  }

  /**
   * This method performs the following steps :<br>
   * 1)Create a blocking queue and start a thread which does take() on it. 2)Verify after
   * significant time that the thread is still alive as it should be blocked on take() since there
   * are no events in the queue.<br>
   * 3)Do a put into the queue and verify that the take thread returns with the same object.
   *
   * @param conflationEnabled - whether conflation is enabled or not
   *
   */
  public void doBlockingTake(boolean conflationEnabled) throws Exception {

    testFailed = false;
    message = null;
    final HARegionQueue rq = createHARegionQueue("testBlockingTake");
    final List takenObjects = new ArrayList();
    Thread takeThread = new Thread() {
      @Override
      public void run() {
        try {
          takenObjects.add(rq.take());
        } catch (Exception e) {
          testFailed = true;
          message.append("Exception while performing take operation " + e.getStackTrace());
        }
      }
    };

    takeThread.start();
    Wait.pause(20 * 1000);
    if (!takeThread.isAlive()) {
      fail("take() thread died ");
    }
    EventID id1 = new EventID(new byte[] {1}, 1, 1);
    ConflatableObject c1 = new ConflatableObject(KEY1, VALUE1, id1, conflationEnabled, "region1");
    rq.put(c1);
    ThreadUtils.join(takeThread, 20 * 1000);
    assertEquals(1, takenObjects.size());
    Conflatable obj = (Conflatable) takenObjects.get(0);
    assertNotNull(obj);
    assertEquals(id1, obj.getEventId());

    if (testFailed) {
      fail("Test failed due to " + message);
    }
  }

  /**
   * This test performs the following steps :<br>
   * 1)Create a blocking queue.<br>
   * 2) Start two threads which does take() on it and add the return object to a list.<br>
   * 3)Put two object into the queue. <br>
   * 4)Verify both both take() threads return with an object by ensuring that the size of the list
   * containing return objects is two.<br>
   *
   *
   */
  @Test
  public void testConcurrentBlockingTake() throws Exception {
    logWriter.info("HARQAddOperationJUnitTest : testConcurrentBlockingTake BEGIN");

    testFailed = false;
    message = null;
    final HARegionQueue rq = createHARegionQueue("testBlockingTake");
    final List takenObjects = new Vector();
    final int totalTakeThreads = 2;
    Thread[] takeThreads = new Thread[totalTakeThreads];
    for (int i = 0; i < totalTakeThreads; i++) {
      takeThreads[i] = new Thread() {
        @Override
        public void run() {
          try {
            takenObjects.add(rq.take());
          } catch (Exception e) {
            testFailed = true;
            message.append("Exception while performing take operation " + e.getStackTrace());
          }
        }
      };
      takeThreads[i].start();
    }

    Conflatable c = null;
    EventID id = null;
    for (int i = 0; i < totalTakeThreads; i++) {
      id = new EventID(new byte[] {1}, 1, i);
      c = new ConflatableObject("k" + i, "v" + i, id, true, "region1");
      rq.put(c);
    }
    for (int i = 0; i < totalTakeThreads; i++) {
      ThreadUtils.join(takeThreads[i], 20 * 1000);
    }

    assertEquals(totalTakeThreads, takenObjects.size());

    for (int i = 0; i < totalTakeThreads; i++) {
      c = (Conflatable) takenObjects.get(i);
      assertNotNull(c);
    }

    if (testFailed) {
      fail("Test failed due to " + message);
    }
    logWriter.info("HARQAddOperationJUnitTest : testConcurrentBlockingTake END");
  }
}
