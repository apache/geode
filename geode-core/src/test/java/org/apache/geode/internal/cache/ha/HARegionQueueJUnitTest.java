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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.number.OrderingComparison.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * This is a test for the APIs of a HARegionQueue and verifies that the head, tail and size counters
 * are updated properly.
 *
 * TODO: need to rewrite a bunch of tests in HARegionQueueJUnitTest
 */
@Category({IntegrationTest.class, ClientSubscriptionTest.class})
public class HARegionQueueJUnitTest {

  /** total number of threads doing put operations */
  private static final int TOTAL_PUT_THREADS = 10;

  private static HARegionQueue hrqForTestSafeConflationRemoval;
  private static List list1;

  protected InternalCache cache;
  private HARegionQueue haRegionQueue;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    this.cache = createCache();
  }

  @After
  public void tearDown() throws Exception {
    this.cache.close();
    hrqForTestSafeConflationRemoval = null;
  }

  /**
   * This test does the following :<br>
   * 1)Create producer threads to add objects to queue <br>
   * 2)Start all the producers so that they complete their puts <br>
   * 3)Wait till all put-threads complete their job <br>
   * 4)verify that the size of the queue is equal to the total number of puts done by all producers
   */
  @Test
  public void testQueuePutWithoutConflation() throws Exception {
    this.haRegionQueue = createHARegionQueue(this.testName.getMethodName());
    int putPerProducer = 20;
    createAndRunProducers(false, false, false, putPerProducer);
    assertThat(this.haRegionQueue.size(), is(putPerProducer * TOTAL_PUT_THREADS));
  }

  /**
   * This test does the following :<br>
   * 1)Create producer threads to add objects to queue <br>
   * 2)Start all the producers,all of them will do puts against same set of keys <br>
   * 3)Wait till all put-threads complete their job <br>
   * 4)verify that the size of the queue is equal to the total number of puts done by one thread (as
   * rest of them will conflate)
   */
  @Test
  public void testQueuePutWithConflation() throws Exception {
    this.haRegionQueue = createHARegionQueue(this.testName.getMethodName());
    int putPerProducer = 20;
    createAndRunProducers(true, false, true, putPerProducer);
    assertThat(this.haRegionQueue.size(), is(putPerProducer));
  }

  /**
   * This test does the following :<br>
   * 1)Create producer threads to add objects to queue <br>
   * 2)Start all the producers,all of them will do puts against same set of ids <br>
   * 3)Wait till all put-threads complete their job <br>
   * 4)verify that the size of the queue is equal to the total number of puts done by one thread (as
   * rest of them will be duplicates and hence will be replaced)
   */
  @Test
  public void testQueuePutWithDuplicates() throws Exception {
    this.haRegionQueue = createHARegionQueue(this.testName.getMethodName());
    int putPerProducer = 20;
    createAndRunProducers(false, false, true, putPerProducer);
    assertThat(this.haRegionQueue.size(), is(putPerProducer * TOTAL_PUT_THREADS));
  }

  /*
   * Test method for 'org.apache.geode.internal.cache.ha.HARegionQueue.addDispatchedMessage(Object)'
   */
  @Test
  public void testAddDispatchedMessageObject() throws Exception {
    this.haRegionQueue = createHARegionQueue(this.testName.getMethodName());
    assertThat(HARegionQueue.getDispatchedMessagesMapForTesting().isEmpty(), is(true));

    this.haRegionQueue.addDispatchedMessage(new ThreadIdentifier(new byte[1], 1), 1);
    this.haRegionQueue.addDispatchedMessage(new ThreadIdentifier(new byte[1], 2), 2);

    assertThat(!HARegionQueue.getDispatchedMessagesMapForTesting().isEmpty(), is(true));
  }

  /**
   * tests the blocking peek functionality of BlockingHARegionQueue
   */
  @Test
  public void testBlockQueue() throws Exception {
    HARegionQueue regionQueue = HARegionQueue.getHARegionQueueInstance(
        this.testName.getMethodName(), this.cache, HARegionQueue.BLOCKING_HA_QUEUE, false);
    Thread[] threads = new Thread[10];
    int threadsLength = threads.length;
    CyclicBarrier barrier = new CyclicBarrier(threadsLength + 1);

    for (int i = 0; i < threadsLength; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            barrier.await();
            long startTime = System.currentTimeMillis();
            Object obj = regionQueue.peek();
            if (obj == null) {
              errorCollector.addError(new AssertionError(
                  "Failed :  failed since object was null and was not expected to be null"));
            }
            long totalTime = System.currentTimeMillis() - startTime;

            if (totalTime < 2000) {
              errorCollector.addError(new AssertionError(
                  " Failed :  Expected time to be greater than 2000 but it is not so "));
            }
          } catch (Exception e) {
            errorCollector.addError(e);
          }
        }
      };
    }

    for (Thread thread1 : threads) {
      thread1.start();
    }

    barrier.await();

    Thread.sleep(5000);

    EventID id = new EventID(new byte[] {1}, 1, 1);
    regionQueue
        .put(new ConflatableObject("key", "value", id, false, this.testName.getMethodName()));

    long startTime = System.currentTimeMillis();
    for (Thread thread : threads) {
      ThreadUtils.join(thread, 60 * 1000);
    }

    long totalTime = System.currentTimeMillis() - startTime;

    if (totalTime >= 60000) {
      fail(" Test taken too long ");
    }
  }

  /**
   * tests whether expiry of entry in the region queue occurs as expected
   */
  @Test
  public void testExpiryPositive() throws Exception {
    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    haa.setExpiryTime(1);

    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName(), haa);
    long start = System.currentTimeMillis();

    regionQueue.put(new ConflatableObject("key", "value", new EventID(new byte[] {1}, 1, 1), true,
        this.testName.getMethodName()));

    Map map = (Map) regionQueue.getConflationMapForTesting().get(this.testName.getMethodName());
    waitAtLeast(1000, start, () -> {
      assertThat(map, is(Collections.emptyMap()));
      assertThat(regionQueue.getRegion().keys(), is(Collections.emptySet()));
    });
  }

  /**
   * tests whether expiry of a conflated entry in the region queue occurs as expected
   */
  @Test
  public void testExpiryPositiveWithConflation() throws Exception {
    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    haa.setExpiryTime(1);

    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName(), haa);
    long start = System.currentTimeMillis();

    regionQueue.put(new ConflatableObject("key", "value", new EventID(new byte[] {1}, 1, 1), true,
        this.testName.getMethodName()));

    regionQueue.put(new ConflatableObject("key", "newValue", new EventID(new byte[] {1}, 1, 2),
        true, this.testName.getMethodName()));

    assertThat(
        " Expected region size not to be zero since expiry time has not been exceeded but it is not so ",
        !regionQueue.isEmpty(), is(true));
    assertThat(
        " Expected the available id's size not  to be zero since expiry time has not  been exceeded but it is not so ",
        !regionQueue.getAvalaibleIds().isEmpty(), is(true));
    assertThat(
        " Expected conflation map size not  to be zero since expiry time has not been exceeded but it is not so "
            + ((Map) regionQueue.getConflationMapForTesting().get(this.testName.getMethodName()))
                .get("key"),
        ((Map) regionQueue.getConflationMapForTesting().get(this.testName.getMethodName()))
            .get("key"),
        not(sameInstance(null)));
    assertThat(
        " Expected eventID map size not to be zero since expiry time has not been exceeded but it is not so ",
        !regionQueue.getEventsMapForTesting().isEmpty(), is(true));

    waitAtLeast(1000, start, () -> {
      assertThat(regionQueue.getRegion().keys(), is(Collections.emptySet()));
      assertThat(regionQueue.getAvalaibleIds(), is(Collections.emptySet()));
      assertThat(regionQueue.getConflationMapForTesting().get(this.testName.getMethodName()),
          is(Collections.emptyMap()));
      assertThat(regionQueue.getEventsMapForTesting(), is(Collections.emptyMap()));
    });
  }

  /**
   * tests a ThreadId not being expired if it was updated
   */
  @Test
  public void testNoExpiryOfThreadId() throws Exception {
    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    haa.setExpiryTime(45);

    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName(), haa);
    EventID ev1 = new EventID(new byte[] {1}, 1, 1);
    EventID ev2 = new EventID(new byte[] {1}, 1, 2);
    Conflatable cf1 =
        new ConflatableObject("key", "value", ev1, true, this.testName.getMethodName());
    Conflatable cf2 =
        new ConflatableObject("key", "value2", ev2, true, this.testName.getMethodName());

    regionQueue.put(cf1);
    long tailKey = regionQueue.tailKey.get();
    regionQueue.put(cf2);

    // Invalidate will trigger the expiration of the entry
    // See HARegionQueue.createCacheListenerForHARegion
    regionQueue.getRegion().invalidate(tailKey);

    assertThat(
        " Expected region size not to be zero since expiry time has not been exceeded but it is not so ",
        !regionQueue.isEmpty(), is(true));
    assertThat(" Expected the available id's size not  to have counter 1 but it has ",
        !regionQueue.getAvalaibleIds().contains(1L), is(true));
    assertThat(" Expected the available id's size to have counter 2 but it does not have ",
        regionQueue.getAvalaibleIds().contains(2L), is(true));
    assertThat(" Expected eventID map not to have the first event, but it has",
        !regionQueue.getCurrentCounterSet(ev1).contains(1L), is(true));
    assertThat(" Expected eventID map to have the second event, but it does not",
        regionQueue.getCurrentCounterSet(ev2).contains(2L), is(true));
  }

  /**
   * Tests a QueueRemovalMessage coming before a localPut(). The localPut() should result in no data
   * being put in the queue
   */
  @Test
  public void testQRMComingBeforeLocalPut() throws Exception {
    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName());
    EventID id = new EventID(new byte[] {1}, 1, 1);

    regionQueue.removeDispatchedEvents(id);
    regionQueue.put(new ConflatableObject("key", "value", id, true, this.testName.getMethodName()));

    assertThat(" Expected key to be null since QRM for the message id had already arrived ",
        !regionQueue.getRegion().containsKey(1L), is(true));
  }

  /**
   * test verifies correct expiry of ThreadIdentifier in the HARQ if no corresponding put comes
   */
  @Test
  public void testOnlyQRMComing() throws Exception {
    HARegionQueueAttributes harqAttr = new HARegionQueueAttributes();
    harqAttr.setExpiryTime(1);

    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName(), harqAttr);
    EventID id = new EventID(new byte[] {1}, 1, 1);
    long start = System.currentTimeMillis();

    regionQueue.removeDispatchedEvents(id);

    assertThat(" Expected testingID to be present since only QRM achieved ",
        regionQueue.getRegion().containsKey(new ThreadIdentifier(new byte[] {1}, 1)), is(true));

    waitAtLeast(1000, start,
        () -> assertThat(
            " Expected testingID not to be present since it should have expired after 2.5 seconds",
            !regionQueue.getRegion().containsKey(new ThreadIdentifier(new byte[] {1}, 1)),
            is(true)));
  }

  /**
   * test all relevant data structures are updated on a local put
   */
  @Test
  public void testPutPath() throws Exception {
    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName());
    Conflatable cf = new ConflatableObject("key", "value", new EventID(new byte[] {1}, 1, 1), true,
        this.testName.getMethodName());

    regionQueue.put(cf);

    assertThat(" Expected region peek to return cf but it is not so ", regionQueue.peek(), is(cf));
    assertThat(
        " Expected the available id's size not  to be zero since expiry time has not  been exceeded but it is not so ",
        !regionQueue.getAvalaibleIds().isEmpty(), is(true));
    assertThat(
        " Expected conflation map to have entry for this key since expiry time has not been exceeded but it is not so ",
        ((Map) regionQueue.getConflationMapForTesting().get(this.testName.getMethodName()))
            .get("key"),
        is(1L));
    assertThat(
        " Expected eventID map size not to be zero since expiry time has not been exceeded but it is not so ",
        !regionQueue.getEventsMapForTesting().isEmpty(), is(true));
  }

  /**
   * - adds 10 items - sets last dispatched as 5th - verify no data pertaining to the first five is
   * there - verify the next five entries and their relevant data is present
   */
  @Test
  public void testQRMDispatch() throws Exception {
    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName());
    Conflatable[] cf = new Conflatable[10];

    // put 10 conflatable objects
    for (int i = 0; i < 10; i++) {
      cf[i] = new ConflatableObject("key" + i, "value", new EventID(new byte[] {1}, 1, i), true,
          this.testName.getMethodName());
      regionQueue.put(cf[i]);
    }

    // remove the first 5 by giving the right sequence id
    regionQueue.removeDispatchedEvents(new EventID(new byte[] {1}, 1, 4));

    // verify 1-5 not in region
    for (int i = 1; i < 6; i++) {
      assertThat(!regionQueue.getRegion().containsKey((long) i), is(true));
    }

    // verify 6-10 still in region queue
    for (int i = 6; i < 11; i++) {
      assertThat(regionQueue.getRegion().containsKey((long) i), is(true));
    }

    // verify 1-5 not in conflation map
    for (int i = 0; i < 5; i++) {
      assertThat(
          !((Map) regionQueue.getConflationMapForTesting().get(this.testName.getMethodName()))
              .containsKey("key" + i),
          is(true));
    }

    // verify 6-10 in conflation map
    for (int i = 5; i < 10; i++) {
      assertThat(((Map) regionQueue.getConflationMapForTesting().get(this.testName.getMethodName()))
          .containsKey("key" + i), is(true));
    }

    EventID eid = new EventID(new byte[] {1}, 1, 6);

    // verify 1-5 not in eventMap
    for (int i = 1; i < 6; i++) {
      assertThat(!regionQueue.getCurrentCounterSet(eid).contains((long) i), is(true));
    }

    // verify 6-10 in event Map
    for (int i = 6; i < 11; i++) {
      assertThat(regionQueue.getCurrentCounterSet(eid).contains((long) i), is(true));
    }

    // verify 1-5 not in available Id's map
    for (int i = 1; i < 6; i++) {
      assertThat(!regionQueue.getAvalaibleIds().contains((long) i), is(true));
    }

    // verify 6-10 in available id's map
    for (int i = 6; i < 11; i++) {
      assertThat(regionQueue.getAvalaibleIds().contains((long) i), is(true));
    }
  }

  /**
   * - send Dispatch message for sequence id 7 - put from sequence id 1 - id 10 - verify data for
   * 1-7 not there - verify data for 8-10 is there
   */
  @Test
  public void testQRMBeforePut() throws Exception {
    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName());

    EventID[] ids = new EventID[10];

    for (int i = 0; i < 10; i++) {
      ids[i] = new EventID(new byte[] {1}, 1, i);
    }

    // first get the qrm message for the seventh id
    regionQueue.removeDispatchedEvents(ids[6]);
    Conflatable[] cf = new Conflatable[10];

    // put 10 conflatable objects
    for (int i = 0; i < 10; i++) {
      cf[i] =
          new ConflatableObject("key" + i, "value", ids[i], true, this.testName.getMethodName());
      regionQueue.put(cf[i]);
    }

    // verify 1-7 not in region
    Set values = (Set) regionQueue.getRegion().values();

    for (int i = 0; i < 7; i++) {
      System.out.println(i);
      assertThat(!values.contains(cf[i]), is(true));
    }

    // verify 8-10 still in region queue
    for (int i = 7; i < 10; i++) {
      System.out.println(i);
      assertThat(values.contains(cf[i]), is(true));
    }

    // verify 1-8 not in conflation map
    for (int i = 0; i < 7; i++) {
      assertThat(
          !((Map) regionQueue.getConflationMapForTesting().get(this.testName.getMethodName()))
              .containsKey("key" + i),
          is(true));
    }

    // verify 8-10 in conflation map
    for (int i = 7; i < 10; i++) {
      assertThat(((Map) regionQueue.getConflationMapForTesting().get(this.testName.getMethodName()))
          .containsKey("key" + i), is(true));
    }

    EventID eid = new EventID(new byte[] {1}, 1, 6);

    // verify 1-7 not in eventMap
    for (int i = 4; i < 11; i++) {
      assertThat(!regionQueue.getCurrentCounterSet(eid).contains((long) i), is(true));
    }

    // verify 8-10 in event Map
    for (int i = 1; i < 4; i++) {
      assertThat(regionQueue.getCurrentCounterSet(eid).contains((long) i), is(true));
    }

    // verify 1-7 not in available Id's map
    for (int i = 4; i < 11; i++) {
      assertThat(!regionQueue.getAvalaibleIds().contains((long) i), is(true));
    }

    // verify 8-10 in available id's map
    for (int i = 1; i < 4; i++) {
      assertThat(regionQueue.getAvalaibleIds().contains((long) i), is(true));
    }
  }

  /**
   * test to verify conflation happens as expected
   */
  @Test
  public void testConflation() throws Exception {
    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName());
    EventID ev1 = new EventID(new byte[] {1}, 1, 1);
    EventID ev2 = new EventID(new byte[] {1}, 2, 2);
    Conflatable cf1 =
        new ConflatableObject("key", "value", ev1, true, this.testName.getMethodName());
    Conflatable cf2 =
        new ConflatableObject("key", "value2", ev2, true, this.testName.getMethodName());
    regionQueue.put(cf1);

    Map conflationMap = regionQueue.getConflationMapForTesting();
    assertThat(((Map) conflationMap.get(this.testName.getMethodName())).get("key"), is(1L));

    regionQueue.put(cf2);

    // verify the conflation map has recorded the new key
    assertThat(((Map) conflationMap.get(this.testName.getMethodName())).get("key"), is(2L));
    // the old key should not be present
    assertThat(!regionQueue.getRegion().containsKey(1L), is(true));
    // available ids should not contain the old id (the old position)
    assertThat(!regionQueue.getAvalaibleIds().contains(1L), is(true));
    // available id should have the new id (the new position)
    assertThat(regionQueue.getAvalaibleIds().contains(2L), is(true));
    // events map should not contain the old position
    assertThat(regionQueue.getCurrentCounterSet(ev1).isEmpty(), is(true));
    // events map should contain the new position
    assertThat(regionQueue.getCurrentCounterSet(ev2).contains(2L), is(true));
  }

  /**
   * Tests whether the QRM message removes the events correctly from the DACE & Conflation Map. The
   * events which are of ID greater than that contained in QRM should stay
   */
  @Test
  public void testQRM() throws Exception {
    RegionQueue regionqueue = createHARegionQueue(this.testName.getMethodName());

    for (int i = 0; i < 10; ++i) {
      regionqueue.put(new ConflatableObject("key" + (i + 1), "value",
          new EventID(new byte[] {1}, 1, i + 1), true, this.testName.getMethodName()));
    }

    EventID qrmID = new EventID(new byte[] {1}, 1, 5);
    ((HARegionQueue) regionqueue).removeDispatchedEvents(qrmID);
    Map conflationMap = ((HARegionQueue) regionqueue).getConflationMapForTesting();
    assertThat(((Map) conflationMap.get(this.testName.getMethodName())).size(), is(5));

    Set availableIDs = ((HARegionQueue) regionqueue).getAvalaibleIds();
    Set counters = ((HARegionQueue) regionqueue).getCurrentCounterSet(qrmID);

    assertThat(availableIDs.size(), is(5));
    assertThat(counters.size(), is(5));

    for (int i = 5; i < 10; ++i) {
      assertThat(
          ((Map) (conflationMap.get(this.testName.getMethodName()))).containsKey("key" + (i + 1)),
          is(true));
      assertThat(availableIDs.contains((long) (i + 1)), is(true));
      assertThat(counters.contains((long) (i + 1)), is(true));
    }

    Region rgn = ((HARegionQueue) regionqueue).getRegion();
    assertThat(rgn.keySet().size(), is(6));
  }

  /**
   * This test tests safe removal from the conflation map. i.e operations should only remove old
   * values and not the latest value
   */
  @Test
  public void testSafeConflationRemoval() throws Exception {
    hrqForTestSafeConflationRemoval = new HARQTestClass("testSafeConflationRemoval", this.cache);
    Conflatable cf1 = new ConflatableObject("key1", "value", new EventID(new byte[] {1}, 1, 1),
        true, "testSafeConflationRemoval");

    hrqForTestSafeConflationRemoval.put(cf1);
    hrqForTestSafeConflationRemoval.removeDispatchedEvents(new EventID(new byte[] {1}, 1, 1));

    Map map = (Map) hrqForTestSafeConflationRemoval.getConflationMapForTesting()
        .get("testSafeConflationRemoval");

    assertThat(
        "Expected the counter to be 2 since it should not have been deleted but it is not so ",
        map.get("key1"), is(2L));
  }

  /**
   * This test tests remove operation is causing the insertion of sequence ID for existing
   * ThreadIdentifier object and concurrently the QRM thread is iterating over the Map to form the
   * Data Set for dispatch. There should not be any Data Loss
   * 
   * In this test, first we add x number of events for unique thread id for the same region then we
   * start two concurrent threads. One which does add to dispatched events map with sequence id's
   * greater than x but the same ThreadIds as previous. The second thread does createMessageList
   * (which removes from the dispatched events map) and stores the list
   * 
   * After the two threads have operated, createMessageList is called again and the data is stored
   * in another list.
   * 
   * The data in the list is populated on a map against the threadId.
   * 
   * It is then verified to see that all the sequence should be greater than x
   */
  @Test
  public void testConcurrentDispatcherAndRemovalForSameRegionSameThreadId() throws Exception {
    long numberOfIterations = 1000;
    HARegionQueue hrq = createHARegionQueue(this.testName.getMethodName());
    HARegionQueue.stopQRMThread();
    ThreadIdentifier[] ids = new ThreadIdentifier[(int) numberOfIterations];

    for (int i = 0; i < numberOfIterations; i++) {
      ids[i] = new ThreadIdentifier(new byte[] {1}, i);
      hrq.addDispatchedMessage(ids[i], i);
    }

    Thread thread1 = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(600);
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
        list1 = HARegionQueue.createMessageListForTesting();
      }
    };

    Thread thread2 = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(480);
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
        for (int i = 0; i < numberOfIterations; i++) {
          hrq.addDispatchedMessage(ids[i], i + numberOfIterations);
        }
      }
    };

    thread1.start();
    thread2.start();
    ThreadUtils.join(thread1, 30 * 1000);
    ThreadUtils.join(thread2, 30 * 1000);
    List list2 = HARegionQueue.createMessageListForTesting();
    Iterator iterator = list1.iterator();
    boolean doOnce = false;
    EventID id;
    Map map = new HashMap();

    while (iterator.hasNext()) {
      if (!doOnce) {
        iterator.next();
        iterator.next();
        doOnce = true;
      } else {
        id = (EventID) iterator.next();
        map.put(new Long(id.getThreadID()), id.getSequenceID());
      }
    }

    iterator = list2.iterator();
    doOnce = false;

    while (iterator.hasNext()) {
      if (!doOnce) {
        iterator.next();
        iterator.next();
        doOnce = true;
      } else {
        id = (EventID) iterator.next();
        map.put(id.getThreadID(), id.getSequenceID());
      }
    }

    iterator = map.values().iterator();
    Long max = numberOfIterations;
    while (iterator.hasNext()) {
      Long next = (Long) iterator.next();
      assertThat(
          " Expected all the sequence ID's to be greater than " + max
              + " but it is not so. Got sequence id " + next,
          next.compareTo(max), greaterThanOrEqualTo(0));
    }
  }

  /**
   * This test remove operation is updating the sequence ID for a ThreadIdentifier and concurrently
   * the QRM thread is iterating over the Map to form the Data Set. There should not be any DataLoss
   *
   * In this test, first we add x number of events for unique thread id for the same region then we
   * start two concurrent threads. One which does add to dispatched events map with sequence id's
   * greater than x and the second one which does createMessageList (which removes from the
   * dispatched events map) and stores the list
   * 
   * After the two threads have operated, createMessageList is called again and the data is stored
   * in another list.
   * 
   * The data in the list is populated on a map against the threadId.
   * 
   * It is then verified to see that the map size should be 2x
   */
  @Test
  public void testConcurrentDispatcherAndRemovalForSameRegionDifferentThreadId() throws Exception {
    int numberOfIterations = 1000;
    HARegionQueue hrq = createHARegionQueue(this.testName.getMethodName());
    HARegionQueue.stopQRMThread();
    ThreadIdentifier[] ids = new ThreadIdentifier[(int) numberOfIterations];

    for (int i = 0; i < numberOfIterations; i++) {
      ids[i] = new ThreadIdentifier(new byte[] {1}, i);
      hrq.addDispatchedMessage(ids[i], i);
    }

    Thread thread1 = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(600);
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
        list1 = HARegionQueue.createMessageListForTesting();
      }
    };

    Thread thread2 = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(480);
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
        for (int i = 0; i < numberOfIterations; i++) {
          ids[i] = new ThreadIdentifier(new byte[] {1}, i + numberOfIterations);
          hrq.addDispatchedMessage(ids[i], i + numberOfIterations);
        }
      }
    };

    thread1.start();
    thread2.start();
    ThreadUtils.join(thread1, 30 * 1000);
    ThreadUtils.join(thread2, 30 * 1000);
    List list2 = HARegionQueue.createMessageListForTesting();
    Iterator iterator = list1.iterator();
    boolean doOnce = false;
    EventID id;
    Map map = new HashMap();

    while (iterator.hasNext()) {
      if (!doOnce) {
        iterator.next();
        iterator.next();
        doOnce = true;
      } else {
        id = (EventID) iterator.next();
        map.put(id.getThreadID(), id.getSequenceID());
      }
    }

    iterator = list2.iterator();
    doOnce = false;

    while (iterator.hasNext()) {
      if (!doOnce) {
        iterator.next();
        iterator.next();
        doOnce = true;
      } else {
        id = (EventID) iterator.next();
        map.put(id.getThreadID(), id.getSequenceID());
      }
    }
    assertThat(
        " Expected the map size to be " + 2 * numberOfIterations + " but it is " + map.size(),
        map.size(), is(2 * numberOfIterations));
  }

  /**
   * This test tests remove operation is causing the insertion of a sequence ID for a new HA Region
   * and concurrently the QRM thread is iterating over the Map to form the Data Set for dispatch.
   * There should not be any Data Loss
   *
   * In this test, first we add x number of events for unique thread id for the 2 regions then we
   * start two concurrent threads. One which does add to dispatched events map to 3 new regions
   * 
   * the second thread does createMessageList (which removes from the dispatched events map) and
   * stores the list
   * 
   * After the two threads have operated, createMessageList is called again and the data is stored
   * in another list.
   * 
   * The data in the list is populated on a map against the threadId.
   * 
   * It is then verified to see that a total of x entries are present in the map
   */
  @Test
  public void testConcurrentDispatcherAndRemovalForMultipleRegionsSameThreadId() throws Exception {
    int numberOfIterations = 10000;
    HARegionQueue hrq1 = createHARegionQueue(this.testName.getMethodName() + "-1");
    HARegionQueue hrq2 = createHARegionQueue(this.testName.getMethodName() + "-2");
    HARegionQueue hrq3 = createHARegionQueue(this.testName.getMethodName() + "-3");
    HARegionQueue hrq4 = createHARegionQueue(this.testName.getMethodName() + "-4");
    HARegionQueue hrq5 = createHARegionQueue(this.testName.getMethodName() + "-5");

    HARegionQueue.stopQRMThread();

    ThreadIdentifier[] ids = new ThreadIdentifier[(int) numberOfIterations];

    for (int i = 0; i < numberOfIterations; i++) {
      ids[i] = new ThreadIdentifier(new byte[] {1}, i);
      hrq1.addDispatchedMessage(ids[i], i);
      hrq2.addDispatchedMessage(ids[i], i);

    }

    Thread thread1 = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(600);
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
        list1 = HARegionQueue.createMessageListForTesting();
      }
    };

    Thread thread2 = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(480);
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
        for (int i = 0; i < numberOfIterations; i++) {
          hrq3.addDispatchedMessage(ids[i], i);
          hrq4.addDispatchedMessage(ids[i], i);
          hrq5.addDispatchedMessage(ids[i], i);
        }
      }
    };

    thread1.start();
    thread2.start();
    ThreadUtils.join(thread1, 30 * 1000);
    ThreadUtils.join(thread2, 30 * 1000);
    List list2 = HARegionQueue.createMessageListForTesting();
    Iterator iterator = list1.iterator();
    boolean doOnce = true;
    EventID id;
    Map map = new HashMap();

    while (iterator.hasNext()) {
      if (!doOnce) {
        iterator.next(); // read the total message size
        doOnce = true;
      } else {
        iterator.next();// region name;
        int size = (Integer) iterator.next();
        for (int i = 0; i < size; i++) {
          id = (EventID) iterator.next();
          map.put(new ThreadIdentifier(id.getMembershipID(), id.getThreadID()), id.getSequenceID());
        }
      }
    }

    iterator = list2.iterator();
    doOnce = true;

    while (iterator.hasNext()) {
      if (!doOnce) {
        iterator.next(); // read the total message size
        doOnce = true;
      } else {
        iterator.next();// region name;
        int size = (Integer) iterator.next();
        for (int i = 0; i < size; i++) {
          id = (EventID) iterator.next();
          map.put(new ThreadIdentifier(id.getMembershipID(), id.getThreadID()), id.getSequenceID());
        }
      }
    }

    assertThat(" Expected the map size to be " + numberOfIterations + " but it is " + map.size(),
        map.size(), is(numberOfIterations));
  }

  /**
   * This test tests remove operation is causing the insertion of a sequence ID for a new
   * ThreadIdentifier Object ( that is the HA Regio name exists but ThreadIdentifier object is
   * appearing 1st time) and concurrently the QRM thread is iterating over the Map to form the Data
   * Set for dispatch. There should not be any DataLoss
   * 
   * In this test, first we add x number of events for unique thread id for the multiples regions
   * then we start two concurrent threads. One which does add to dispatched events map with sequence
   * id's greater than x and new ThreadIdentifiers
   * 
   * the second thread does createMessageList (which removes from the dispatched events map) and
   * stores the list
   * 
   * After the two threads have operated, createMessageList is called again and the data is stored
   * in another list.
   * 
   * The data in the list is populated on a map against the threadId.
   * 
   * It is then verified to see that the map size should be 2x * number of regions
   */
  @Test
  public void testConcurrentDispatcherAndRemovalForMultipleRegionsDifferentThreadId()
      throws Exception {
    int numberOfIterations = 1000;
    HARegionQueue hrq1 = createHARegionQueue(this.testName.getMethodName() + "-1");
    HARegionQueue hrq2 = createHARegionQueue(this.testName.getMethodName() + "-2");
    HARegionQueue hrq3 = createHARegionQueue(this.testName.getMethodName() + "-3");
    HARegionQueue hrq4 = createHARegionQueue(this.testName.getMethodName() + "-4");
    HARegionQueue hrq5 = createHARegionQueue(this.testName.getMethodName() + "-5");

    HARegionQueue.stopQRMThread();

    ThreadIdentifier[] ids1 = new ThreadIdentifier[(int) numberOfIterations];
    ThreadIdentifier[] ids2 = new ThreadIdentifier[(int) numberOfIterations];
    ThreadIdentifier[] ids3 = new ThreadIdentifier[(int) numberOfIterations];
    ThreadIdentifier[] ids4 = new ThreadIdentifier[(int) numberOfIterations];
    ThreadIdentifier[] ids5 = new ThreadIdentifier[(int) numberOfIterations];

    for (int i = 0; i < numberOfIterations; i++) {
      ids1[i] = new ThreadIdentifier(new byte[] {1}, i);
      ids2[i] = new ThreadIdentifier(new byte[] {2}, i);
      ids3[i] = new ThreadIdentifier(new byte[] {3}, i);
      ids4[i] = new ThreadIdentifier(new byte[] {4}, i);
      ids5[i] = new ThreadIdentifier(new byte[] {5}, i);
      hrq1.addDispatchedMessage(ids1[i], i);
      hrq2.addDispatchedMessage(ids2[i], i);
      hrq3.addDispatchedMessage(ids3[i], i);
      hrq4.addDispatchedMessage(ids4[i], i);
      hrq5.addDispatchedMessage(ids5[i], i);
    }

    Thread thread1 = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(600);
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
        list1 = HARegionQueue.createMessageListForTesting();
      }
    };

    Thread thread2 = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(480);
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
        for (int i = 0; i < numberOfIterations; i++) {
          ids1[i] = new ThreadIdentifier(new byte[] {1}, i + numberOfIterations);
          ids2[i] = new ThreadIdentifier(new byte[] {2}, i + numberOfIterations);
          ids3[i] = new ThreadIdentifier(new byte[] {3}, i + numberOfIterations);
          ids4[i] = new ThreadIdentifier(new byte[] {4}, i + numberOfIterations);
          ids5[i] = new ThreadIdentifier(new byte[] {5}, i + numberOfIterations);

          hrq1.addDispatchedMessage(ids1[i], i + numberOfIterations);
          hrq2.addDispatchedMessage(ids2[i], i + numberOfIterations);
          hrq3.addDispatchedMessage(ids3[i], i + numberOfIterations);
          hrq4.addDispatchedMessage(ids4[i], i + numberOfIterations);
          hrq5.addDispatchedMessage(ids5[i], i + numberOfIterations);
        }
      }
    };

    thread1.start();
    thread2.start();
    ThreadUtils.join(thread1, 30 * 1000);
    ThreadUtils.join(thread2, 30 * 1000);
    List list2 = HARegionQueue.createMessageListForTesting();
    Iterator iterator = list1.iterator();
    boolean doOnce = true;
    EventID id = null;
    Map map = new HashMap();

    while (iterator.hasNext()) {
      if (!doOnce) {
        iterator.next(); // read the total message size
        doOnce = true;
      } else {
        iterator.next(); // region name;
        int size = (Integer) iterator.next();
        System.out.println(" size of list 1 iteration x " + size);
        for (int i = 0; i < size; i++) {
          id = (EventID) iterator.next();
          map.put(new ThreadIdentifier(id.getMembershipID(), id.getThreadID()), id.getSequenceID());
        }
      }
    }

    iterator = list2.iterator();
    doOnce = true;

    while (iterator.hasNext()) {
      if (!doOnce) {
        iterator.next(); // read the total message size
        doOnce = true;
      } else {
        iterator.next(); // region name;
        int size = (Integer) iterator.next();
        System.out.println(" size of list 2 iteration x " + size);
        for (int i = 0; i < size; i++) {
          id = (EventID) iterator.next();
          map.put(new ThreadIdentifier(id.getMembershipID(), id.getThreadID()), id.getSequenceID());
        }
      }
    }

    assertThat(
        " Expected the map size to be " + numberOfIterations * 2 * 5 + " but it is " + map.size(),
        map.size(), is(numberOfIterations * 2 * 5));
  }

  /**
   * Concurrent Peek on Blocking Queue waiting with for a Put . If concurrent take is also happening
   * such that the object is removed first then the peek should block & not return with null.
   */
  @Test
  public void testBlockingQueueForConcurrentPeekAndTake() throws Exception {
    TestBlockingHARegionQueue regionQueue =
        new TestBlockingHARegionQueue("testBlockQueueForConcurrentPeekAndTake", this.cache);
    Thread[] threads = new Thread[3];

    for (int i = 0; i < 3; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            long startTime = System.currentTimeMillis();
            Object obj = regionQueue.peek();
            if (obj == null) {
              errorCollector.addError(new AssertionError(
                  "Failed :  failed since object was null and was not expected to be null"));
            }
            long totalTime = System.currentTimeMillis() - startTime;

            if (totalTime < 4000) {
              errorCollector.addError(new AssertionError(
                  "Failed :  Expected time to be greater than 4000 but it is not so"));
            }
          } catch (Exception e) {
            errorCollector.addError(e);
          }
        }
      };
    }

    for (int k = 0; k < 3; k++) {
      threads[k].start();
    }

    Thread.sleep(4000);

    EventID id = new EventID(new byte[] {1}, 1, 1);
    EventID id1 = new EventID(new byte[] {1}, 1, 2);

    regionQueue.takeFirst = true;
    regionQueue.put(new ConflatableObject("key", "value", id, true, this.testName.getMethodName()));

    Thread.sleep(2000);

    regionQueue
        .put(new ConflatableObject("key1", "value1", id1, true, this.testName.getMethodName()));

    long startTime = System.currentTimeMillis();
    for (int k = 0; k < 3; k++) {
      ThreadUtils.join(threads[k], 180 * 1000);
    }

    long totalTime = System.currentTimeMillis() - startTime;
    if (totalTime >= 180000) {
      fail(" Test taken too long ");
    }
  }

  /**
   * Peek on a blocking queue is in progress & the single element is removed either by take or by
   * QRM thread , the peek should block correctly.
   */
  @Test
  public void testBlockingQueueForTakeWhenPeekInProgress() throws Exception {
    TestBlockingHARegionQueue regionQueue =
        new TestBlockingHARegionQueue("testBlockQueueForTakeWhenPeekInProgress", this.cache);
    Thread[] threads = new Thread[3];

    for (int i = 0; i < 3; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            long startTime = System.currentTimeMillis();
            Object obj = regionQueue.peek();
            if (obj == null) {
              errorCollector.addError(new AssertionError(
                  "Failed :  failed since object was null and was not expected to be null"));
            }
            long totalTime = System.currentTimeMillis() - startTime;

            if (totalTime < 4000) {
              errorCollector.addError(new AssertionError(
                  "Failed :  Expected time to be greater than 4000 but it is not so"));
            }
          } catch (Exception e) {
            errorCollector.addError(e);
          }
        }
      };
    }

    for (int k = 0; k < 3; k++) {
      threads[k].start();
    }

    Thread.sleep(4000);

    EventID id = new EventID(new byte[] {1}, 1, 1);
    EventID id1 = new EventID(new byte[] {1}, 1, 2);

    regionQueue.takeWhenPeekInProgress = true;
    regionQueue.put(new ConflatableObject("key", "value", id, true, this.testName.getMethodName()));

    Thread.sleep(2000);

    regionQueue
        .put(new ConflatableObject("key1", "value1", id1, true, this.testName.getMethodName()));

    long startTime = System.currentTimeMillis();
    for (int k = 0; k < 3; k++) {
      ThreadUtils.join(threads[k], 60 * 1000);
    }

    long totalTime = System.currentTimeMillis() - startTime;
    if (totalTime >= 60000) {
      fail(" Test taken too long ");
    }
  }

  /**
   * The basis of HARegionQueue's Add & remove operations etc , is that the event being added first
   * goes into DACE , Region etc and finally it is published by adding into the available IDs Set.
   * Similarly if an event is to be removed it should first be removed from availableIDs set & then
   * from behind the scenes. It will be the responsibility of the thread removing from available IDs
   * successfully which will remove the entry from all other places. Now if the expiry task makes
   * the event from underlying null before removing from available IDs , there is a potential
   * violation. This test will validate that behaviour
   */
  @Test
  public void testConcurrentEventExpiryAndTake() throws Exception {
    AtomicBoolean complete = new AtomicBoolean(false);
    AtomicBoolean expiryCalled = new AtomicBoolean(false);
    AtomicBoolean allowExpiryToProceed = new AtomicBoolean(false);

    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    haa.setExpiryTime(3);

    RegionQueue regionqueue =
        new HARegionQueue.TestOnlyHARegionQueue(this.testName.getMethodName(), this.cache, haa) {
          @Override
          CacheListener createCacheListenerForHARegion() {

            return new CacheListenerAdapter() {

              @Override
              public void afterInvalidate(EntryEvent event) {

                if (event.getKey() instanceof Long) {
                  synchronized (HARegionQueueJUnitTest.this) {
                    expiryCalled.set(true);
                    HARegionQueueJUnitTest.this.notifyAll();
                  }

                  Thread.yield();

                  synchronized (HARegionQueueJUnitTest.this) {
                    while (!allowExpiryToProceed.get()) {
                      try {
                        HARegionQueueJUnitTest.this.wait();
                      } catch (InterruptedException e) {
                        errorCollector.addError(e);
                        break;
                      }
                    }
                  }

                  try {
                    expireTheEventOrThreadIdentifier(event);
                  } catch (CacheException e) {
                    errorCollector.addError(e);
                  } finally {
                    synchronized (HARegionQueueJUnitTest.this) {
                      complete.set(true);
                      HARegionQueueJUnitTest.this.notifyAll();
                    }
                  }
                }
              }
            };
          }
        };

    EventID ev1 = new EventID(new byte[] {1}, 1, 1);
    Conflatable cf1 =
        new ConflatableObject("key", "value", ev1, true, this.testName.getMethodName());
    regionqueue.put(cf1);

    synchronized (this) {
      while (!expiryCalled.get()) {
        wait();
      }
    }

    try {
      Object o = regionqueue.take();
      assertThat(o, nullValue());

    } catch (Exception e) {
      throw new AssertionError("Test failed due to exception ", e);

    } finally {
      synchronized (this) {
        allowExpiryToProceed.set(true);
        notifyAll();
      }
    }

    synchronized (this) {
      while (!complete.get()) {
        wait();
      }
    }
  }

  /**
   * Tests the functionality of batch peek & remove with blocking & non blocking HARegionQueue
   */
  @Test
  public void testBatchPeekWithRemoveForNonBlockingQueue() throws Exception {
    testBatchPeekWithRemove(false);
  }

  /**
   * Tests the functionality of batch peek & remove with blocking & non blocking HARegionQueue
   */
  @Test
  public void testBatchPeekWithRemoveForBlockingQueue() throws Exception {
    testBatchPeekWithRemove(true);
  }

  private void testBatchPeekWithRemove(boolean createBlockingQueue)
      throws InterruptedException, IOException, ClassNotFoundException {
    HARegionQueue regionQueue = createHARegionQueue(createBlockingQueue);

    for (int i = 0; i < 10; ++i) {
      EventID ev1 = new EventID(new byte[] {1}, 1, i);
      Conflatable cf1 =
          new ConflatableObject("key", "value", ev1, false, this.testName.getMethodName());
      regionQueue.put(cf1);
    }

    List objs = regionQueue.peek(10, 5000);
    assertThat(objs.size(), is(10));
    Iterator itr = objs.iterator();
    int j = 0;

    while (itr.hasNext()) {
      Conflatable conf = (Conflatable) itr.next();
      assertThat(conf, notNullValue());
      assertThat("The sequence ID of the objects in the queue are not as expected",
          conf.getEventId().getSequenceID(), is((long) j++));
    }

    regionQueue.remove();
    assertThat(regionQueue.size(), is(0));
  }

  private HARegionQueue createHARegionQueue(boolean createBlockingQueue)
      throws InterruptedException, IOException, ClassNotFoundException {
    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    haa.setExpiryTime(300);

    if (createBlockingQueue) {
      return HARegionQueue.getHARegionQueueInstance(this.testName.getMethodName(), this.cache, haa,
          HARegionQueue.BLOCKING_HA_QUEUE, false);
    } else {
      return HARegionQueue.getHARegionQueueInstance(this.testName.getMethodName(), this.cache, haa,
          HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    }
  }

  /**
   * tests whether expiry of entry in the region queue occurs as expected using system property to
   * set expiry
   */
  @Test
  public void testExpiryUsingSystemProperty() throws Exception {
    System.setProperty(HARegionQueue.REGION_ENTRY_EXPIRY_TIME, "1");

    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    HARegionQueue regionQueue = createHARegionQueue(this.testName.getMethodName(), haa);
    long start = System.currentTimeMillis();

    regionQueue.put(new ConflatableObject("key", "value", new EventID(new byte[] {1}, 1, 1), true,
        this.testName.getMethodName()));

    Map map = (Map) regionQueue.getConflationMapForTesting().get(this.testName.getMethodName());
    assertThat(!map.isEmpty(), is(true));

    waitAtLeast(1000, start, () -> {
      assertThat(map, is(Collections.emptyMap()));
      assertThat(regionQueue.getRegion().keys(), is(Collections.emptySet()));
    });
  }

  /**
   * This tests whether the messageSyncInterval for QueueRemovalThread is refreshed properly when
   * set/updated using cache's setter API
   */
  @Test
  public void testUpdateOfMessageSyncInterval() throws Exception {
    int initialMessageSyncInterval = 5;
    this.cache.setMessageSyncInterval(initialMessageSyncInterval);
    createHARegionQueue(this.testName.getMethodName());

    assertThat("messageSyncInterval not set properly", HARegionQueue.getMessageSyncInterval(),
        is(initialMessageSyncInterval));

    int updatedMessageSyncInterval = 10;
    this.cache.setMessageSyncInterval(updatedMessageSyncInterval);

    Awaitility.await().atMost(1, TimeUnit.MINUTES)
        .until(() -> assertThat("messageSyncInterval not updated.",
            HARegionQueue.getMessageSyncInterval(), is(updatedMessageSyncInterval)));
  }

  /**
   * Wait until a given runnable stops throwing exceptions. It should take at least
   * minimumElapsedTime after the supplied start time to happen.
   *
   * This is useful for validating that an entry doesn't expire until a certain amount of time has
   * passed
   */
  private void waitAtLeast(final int minimumElapsedTime, final long start,
      final Runnable runnable) {
    Awaitility.await().atMost(1, TimeUnit.MINUTES).until(runnable);
    long elapsed = System.currentTimeMillis() - start;
    assertThat(elapsed >= minimumElapsedTime, is(true));
  }

  /**
   * Creates and runs the put threads which will create the conflatable objects and add them to the
   * queue
   *
   * @param generateSameKeys - if all the producers need to put objects with same set of keys
   *        (needed for conflation testing)
   * @param generateSameIds - if all the producers need to put objects with same set of ids (needed
   *        for duplicates testing)
   * @param conflationEnabled - true if all producers need to put objects with conflation enabled,
   *        false otherwise.
   * @param putPerProducer - number of objects offered to the queue by each producer
   */
  private void createAndRunProducers(boolean generateSameKeys, boolean generateSameIds,
      boolean conflationEnabled, int putPerProducer) {
    Producer[] putThreads = new Producer[TOTAL_PUT_THREADS];

    // Create the put-threads, each generating same/different set of ids/keys as
    // per the parameters
    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      String keyPrefix;
      long startId;
      if (generateSameKeys) {
        keyPrefix = "key";
      } else {
        keyPrefix = i + "key";
      }
      if (generateSameIds) {
        startId = 1;
      } else {
        startId = i * 100000;
      }
      putThreads[i] =
          new Producer("Producer-" + i, keyPrefix, startId, putPerProducer, conflationEnabled);
    }

    // start the put-threads
    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      putThreads[i].start();
    }

    // call join on the put-threads so that this thread waits till they complete
    // before doing verification
    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      ThreadUtils.join(putThreads[i], 30 * 1000);
    }
  }

  /**
   * Creates the cache instance for the test
   */
  private InternalCache createCache() throws RegionExistsException {
    return (InternalCache) new CacheFactory().set(MCAST_PORT, "0").create();
  }

  protected int queueType() {
    return HARegionQueue.NON_BLOCKING_HA_QUEUE;
  }

  /**
   * Creates HA region-queue object
   */
  private HARegionQueue createHARegionQueue(String name)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    return HARegionQueue.getHARegionQueueInstance(name, this.cache, queueType(), false);
  }

  /**
   * Creates region-queue object
   */
  HARegionQueue createHARegionQueue(String name, HARegionQueueAttributes attrs)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    return HARegionQueue.getHARegionQueueInstance(name, this.cache, attrs, queueType(), false);
  }

  /**
   * Used to override the remove method for testSafeConflationRemoval
   */
  static class ConcHashMap extends ConcurrentHashMap implements ConcurrentMap {

    @Override
    public boolean remove(Object arg0, Object arg1) {
      Conflatable cf2 = new ConflatableObject("key1", "value2", new EventID(new byte[] {1}, 1, 2),
          true, "testSafeConflationRemoval");
      try {
        hrqForTestSafeConflationRemoval.put(cf2);
      } catch (Exception e) {
        throw new AssertionError("Exception occurred in trying to put ", e);
      }
      return super.remove(arg0, arg1);
    }
  }

  /**
   * Extends HARegionQueue for testing purposes. used by testSafeConflationRemoval
   */
  static class HARQTestClass extends HARegionQueue.TestOnlyHARegionQueue {

    HARQTestClass(String regionName, InternalCache cache)
        throws IOException, ClassNotFoundException, CacheException, InterruptedException {
      super(regionName, cache);
    }

    @Override
    ConcurrentMap createConcurrentMap() {
      return new ConcHashMap();
    }
  }

  /**
   * Thread to perform PUTs into the queue
   */
  class Producer extends Thread {

    /** sleep between successive puts */
    private static final long sleepTime = 10;

    /** total number of puts by this thread */
    private long totalPuts = 0;

    /** prefix to keys of all objects put by this thread */
    private final String keyPrefix;

    /** startingId for sequence-ids of all objects put by this thread */
    private final long startingId;

    /** name of this producer thread */
    private String producerName;

    /**
     * boolean to indicate whether this thread should create conflation enabled entries
     */
    private final boolean createConflatables;

    /**
     * @param name - name for this thread
     * @param keyPrefix - prefix to keys of all objects put by this thread
     * @param startingId - startingId for sequence-ids of all objects put by this thread
     * @param totalPuts total number of puts by this thread
     * @param createConflatableEvents - boolean to indicate whether this thread should create
     *        conflation enabled entries
     */
    Producer(String name, String keyPrefix, long startingId, long totalPuts,
        boolean createConflatableEvents) {
      super(name);
      this.producerName = name;
      this.keyPrefix = keyPrefix;
      this.startingId = startingId;
      this.totalPuts = totalPuts;
      this.createConflatables = createConflatableEvents;
      setDaemon(true);
    }

    /** Create Conflatable objects and put them into the Queue. */
    @Override
    public void run() {
      if (this.producerName == null) {
        this.producerName = Thread.currentThread().getName();
      }
      for (long i = 0; i < this.totalPuts; i++) {
        try {
          String regionName = "test";
          ConflatableObject event = new ConflatableObject(this.keyPrefix + i, "val" + i,
              new EventID(new byte[] {1}, this.startingId, this.startingId + i),
              this.createConflatables, regionName);

          haRegionQueue.put(event);
          Thread.sleep(sleepTime);

        } catch (Exception e) {
          errorCollector.addError(e);
          break;
        }
      }
    }
  }

}
