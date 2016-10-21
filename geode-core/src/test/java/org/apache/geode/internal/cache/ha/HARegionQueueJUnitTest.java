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
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * This is a test for the APIs of a HARegionQueue and verifies that the head, tail and size counters
 * are updated properly.
 */
@Category(IntegrationTest.class)
public class HARegionQueueJUnitTest {

  /** The cache instance */
  protected Cache cache = null;

  /** Logger for this test */
  protected LogWriter logger;

  /** The <code>RegionQueue</code> instance */
  protected HARegionQueue rq;

  /** total number of threads doing put operations */
  private static final int TOTAL_PUT_THREADS = 10;

  boolean expiryCalled = false;

  volatile boolean encounteredException = false;
  boolean allowExpiryToProceed = false;
  boolean complete = false;

  @Before
  public void setUp() throws Exception {
    cache = createCache();
    logger = cache.getLogger();
    encounteredException = false;
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  /**
   * Creates the cache instance for the test
   */
  private Cache createCache() throws CacheException {
    return new CacheFactory().set(MCAST_PORT, "0").create();
  }

  /**
   * Creates HA region-queue object
   */
  private HARegionQueue createHARegionQueue(String name)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name, cache,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

  /**
   * Creates region-queue object
   */
  private HARegionQueue createHARegionQueue(String name, HARegionQueueAttributes attrs)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name, cache, attrs,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    return regionqueue;
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
    logger.info("HARegionQueueJUnitTest : testQueuePutWithoutConflation BEGIN");

    rq = createHARegionQueue("testOfferNoConflation");
    int putPerProducer = 20;
    createAndRunProducers(false, false, false, putPerProducer);
    assertEquals(putPerProducer * TOTAL_PUT_THREADS, rq.size());

    logger.info("HARegionQueueJUnitTest : testQueuePutWithoutConflation END");
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
    logger.info("HARegionQueueJUnitTest : testQueuePutWithConflation BEGIN");

    rq = createHARegionQueue("testOfferConflation");
    int putPerProducer = 20;
    createAndRunProducers(true, false, true, putPerProducer);
    assertEquals(putPerProducer, rq.size());

    logger.info("HARegionQueueJUnitTest : testQueuePutWithConflation END");
  }

  /**
   * This test does the following :<br>
   * 1)Create producer threads to add objects to queue <br>
   * 2)Start all the producers,all of them will do puts against same set of ids <br>
   * 3)Wait till all put-threads complete their job <br>
   * 4)verify that the size of the queue is equal to the total number of puts done by one thread (as
   * rest of them will be duplicates and hence will be replaced)
   *
   * TODO:Dinesh : Work on optimizing the handling of receiving duplicate events
   */
  @Test
  public void testQueuePutWithDuplicates() throws Exception {
    logger.info("HARegionQueueJUnitTest : testQueuePutWithDuplicates BEGIN");

    rq = createHARegionQueue("testQueuePutWithDuplicates");
    int putPerProducer = 20;
    // createAndRunProducers(false, true, true, putPerProducer);
    /* Suyog: Only one thread can enter DACE at a time */
    createAndRunProducers(false, false, true, putPerProducer);
    assertEquals(putPerProducer * TOTAL_PUT_THREADS, rq.size());

    logger.info("HARegionQueueJUnitTest : testQueuePutWithDuplicates END");
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
   * @throws Exception - thrown if any problem occurs in test execution
   */
  private void createAndRunProducers(boolean generateSameKeys, boolean generateSameIds,
      boolean conflationEnabled, int putPerProducer) throws Exception {
    Producer[] putThreads = new Producer[TOTAL_PUT_THREADS];

    int i = 0;

    // Create the put-threads, each generating same/different set of ids/keys as
    // per the parameters
    for (i = 0; i < TOTAL_PUT_THREADS; i++) {
      String keyPrefix = null;
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
    for (i = 0; i < TOTAL_PUT_THREADS; i++) {
      putThreads[i].start();
    }

    // call join on the put-threads so that this thread waits till they complete
    // before doing verfication
    for (i = 0; i < TOTAL_PUT_THREADS; i++) {
      ThreadUtils.join(putThreads[i], 30 * 1000);
    }
    assertFalse(encounteredException);
  }

  /*
   * Test method for 'org.apache.geode.internal.cache.ha.HARegionQueue.addDispatchedMessage(Object)'
   */
  @Test
  public void testAddDispatchedMessageObject() {
    try {
      // HARegionQueue haRegionQueue = new HARegionQueue("testing", cache);
      HARegionQueue haRegionQueue = createHARegionQueue("testing");
      assertTrue(HARegionQueue.getDispatchedMessagesMapForTesting().isEmpty());
      // TODO:

      haRegionQueue.addDispatchedMessage(new ThreadIdentifier(new byte[1], 1), 1);
      haRegionQueue.addDispatchedMessage(new ThreadIdentifier(new byte[1], 2), 2);

      assertTrue(!HARegionQueue.getDispatchedMessagesMapForTesting().isEmpty());
      // HARegionQueue.getDispatchedMessagesMapForTesting().clear();

    } catch (Exception e) {
      throw new AssertionError("Test encountered an exception due to ", e);
    }
  }

  /**
   * tests the blocking peek functionality of BlockingHARegionQueue
   */
  @Test
  public void testBlockQueue() {
    exceptionInThread = false;
    testFailed = false;
    try {
      final HARegionQueue bQ = HARegionQueue.getHARegionQueueInstance("testing", cache,
          HARegionQueue.BLOCKING_HA_QUEUE, false);
      Thread[] threads = new Thread[10];
      final CyclicBarrier barrier = new CyclicBarrier(threads.length + 1);
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Thread() {
          public void run() {
            try {
              barrier.await();
              long startTime = System.currentTimeMillis();
              Object obj = bQ.peek();
              if (obj == null) {
                testFailed = true;
                message.append(
                    " Failed :  failed since object was null and was not expected to be null \n");
              }
              long totalTime = System.currentTimeMillis() - startTime;

              if (totalTime < 2000) {
                testFailed = true;
                message
                    .append(" Failed :  Expected time to be greater than 2000 but it is not so ");
              }
            } catch (Exception e) {
              exceptionInThread = true;
              exception = e;
            }
          }
        };

      }

      for (int k = 0; k < threads.length; k++) {
        threads[k].start();
      }
      barrier.await();
      Thread.sleep(5000);

      EventID id = new EventID(new byte[] {1}, 1, 1);
      bQ.put(new ConflatableObject("key", "value", id, false, "testing"));

      long startTime = System.currentTimeMillis();
      for (int k = 0; k < threads.length; k++) {
        ThreadUtils.join(threads[k], 60 * 1000);
      }

      long totalTime = System.currentTimeMillis() - startTime;

      if (totalTime >= 60000) {
        fail(" Test taken too long ");
      }

      if (testFailed) {
        fail(" test failed due to " + message);
      }

    } catch (Exception e) {
      throw new AssertionError(" Test failed due to ", e);
    }
  }

  private static volatile int counter = 0;

  protected boolean exceptionInThread = false;

  protected boolean testFailed = false;

  protected StringBuffer message = new StringBuffer();

  protected Exception exception = null;

  private synchronized int getCounter() {
    return ++counter;
  }

  /**
   * Thread to perform PUTs into the queue
   */
  class Producer extends Thread {
    /** total number of puts by this thread */
    long totalPuts = 0;

    /** sleep between successive puts */
    long sleeptime = 10;

    /** prefix to keys of all objects put by this thread */
    String keyPrefix;

    /** startingId for sequence-ids of all objects put by this thread */
    long startingId;

    /** name of this producer thread */
    String producerName;

    /**
     * boolean to indicate whether this thread should create conflation enabled entries
     */
    boolean createConflatables;

    /**
     * Constructor
     * 
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
      if (producerName == null) {
        producerName = Thread.currentThread().getName();
      }
      for (long i = 0; i < totalPuts; i++) {
        String REGION_NAME = "test";
        try {
          ConflatableObject event = new ConflatableObject(keyPrefix + i, "val" + i,
              new EventID(new byte[] {1}, startingId, startingId + i), createConflatables,
              REGION_NAME);

          logger.fine("putting for key =  " + keyPrefix + i);
          rq.put(event);
          Thread.sleep(sleeptime);
        } catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        } catch (Throwable e) {
          logger.severe("Exception while running Producer;continue running.", e);
          encounteredException = true;
          break;
        }
      }
      logger.info(producerName + " :  Puts completed");
    }
  }

  /**
   * tests whether expiry of entry in the regin queue occurs as expected
   */
  @Test
  public void testExpiryPositive()
      throws InterruptedException, IOException, ClassNotFoundException {
    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    haa.setExpiryTime(1);
    HARegionQueue regionqueue = createHARegionQueue("testing", haa);
    long start = System.currentTimeMillis();
    regionqueue.put(
        new ConflatableObject("key", "value", new EventID(new byte[] {1}, 1, 1), true, "testing"));
    Map map = (Map) regionqueue.getConflationMapForTesting().get("testing");
    waitAtLeast(1000, start, () -> {
      assertEquals(Collections.EMPTY_MAP, map);
      assertEquals(Collections.EMPTY_SET, regionqueue.getRegion().keys());
    });
  }

  /**
   * Wait until a given runnable stops throwing exceptions. It should take at least
   * minimumElapsedTime after the supplied start time to happen.
   *
   * This is useful for validating that an entry doesn't expire until a certain amount of time has
   * passed
   */
  protected void waitAtLeast(final int minimumElapsedTIme, final long start,
      final Runnable runnable) {
    Awaitility.await().atMost(1, TimeUnit.MINUTES).until(runnable);
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed >= minimumElapsedTIme);
  }

  /**
   * tests whether expiry of a conflated entry in the region queue occurs as expected
   */
  @Test
  public void testExpiryPositiveWithConflation()
      throws InterruptedException, IOException, ClassNotFoundException {
    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    haa.setExpiryTime(1);
    HARegionQueue regionqueue = createHARegionQueue("testing", haa);
    long start = System.currentTimeMillis();
    regionqueue.put(
        new ConflatableObject("key", "value", new EventID(new byte[] {1}, 1, 1), true, "testing"));
    regionqueue.put(new ConflatableObject("key", "newValue", new EventID(new byte[] {1}, 1, 2),
        true, "testing"));
    assertTrue(
        " Expected region size not to be zero since expiry time has not been exceeded but it is not so ",
        !(regionqueue.size() == 0));
    assertTrue(
        " Expected the available id's size not  to be zero since expiry time has not  been exceeded but it is not so ",
        !(regionqueue.getAvalaibleIds().size() == 0));
    assertTrue(
        " Expected conflation map size not  to be zero since expiry time has not been exceeded but it is not so "
            + ((((Map) (regionqueue.getConflationMapForTesting().get("testing"))).get("key"))),
        !((((Map) (regionqueue.getConflationMapForTesting().get("testing"))).get("key")) == null));
    assertTrue(
        " Expected eventID map size not to be zero since expiry time has not been exceeded but it is not so ",
        !(regionqueue.getEventsMapForTesting().size() == 0));

    waitAtLeast(1000, start, () -> {
      assertEquals(Collections.EMPTY_SET, regionqueue.getRegion().keys());
      assertEquals(Collections.EMPTY_SET, regionqueue.getAvalaibleIds());
      assertEquals(Collections.EMPTY_MAP, regionqueue.getConflationMapForTesting().get("testing"));
      assertEquals(Collections.EMPTY_MAP, regionqueue.getEventsMapForTesting());
    });
  }

  /**
   * tests a ThreadId not being expired if it was updated
   */
  @Test
  public void testNoExpiryOfThreadId() {
    try {
      HARegionQueueAttributes haa = new HARegionQueueAttributes();
      haa.setExpiryTime(45);
      // RegionQueue regionqueue = new HARegionQueue("testing", cache, haa);
      HARegionQueue regionqueue = createHARegionQueue("testing", haa);
      EventID ev1 = new EventID(new byte[] {1}, 1, 1);
      EventID ev2 = new EventID(new byte[] {1}, 1, 2);
      Conflatable cf1 = new ConflatableObject("key", "value", ev1, true, "testing");
      Conflatable cf2 = new ConflatableObject("key", "value2", ev2, true, "testing");
      regionqueue.put(cf1);
      final long tailKey = regionqueue.tailKey.get();
      regionqueue.put(cf2);
      // Invalidate will trigger the expiration of the entry
      // See HARegionQueue.createCacheListenerForHARegion
      regionqueue.getRegion().invalidate(tailKey);
      assertTrue(
          " Expected region size not to be zero since expiry time has not been exceeded but it is not so ",
          !(regionqueue.size() == 0));
      assertTrue(" Expected the available id's size not  to have counter 1 but it has ",
          !(regionqueue.getAvalaibleIds().contains(new Long(1))));
      assertTrue(" Expected the available id's size to have counter 2 but it does not have ",
          (regionqueue.getAvalaibleIds().contains(new Long(2))));
      assertTrue(" Expected eventID map not to have the first event, but it has",
          !(regionqueue.getCurrentCounterSet(ev1).contains(new Long(1))));
      assertTrue(" Expected eventID map to have the second event, but it does not",
          (regionqueue.getCurrentCounterSet(ev2).contains(new Long(2))));
    }

    catch (Exception e) {
      throw new AssertionError("test failed due to ", e);
    }
  }

  /**
   * Tests a QueueRemovalMessage coming before a localPut(). The localPut() should result in no data
   * being put in the queue
   */
  @Test
  public void testQRMComingBeforeLocalPut() {
    try {
      // RegionQueue regionqueue = new HARegionQueue("testing", cache);
      HARegionQueue regionqueue = createHARegionQueue("testing");
      EventID id = new EventID(new byte[] {1}, 1, 1);
      regionqueue.removeDispatchedEvents(id);
      regionqueue.put(new ConflatableObject("key", "value", id, true, "testing"));
      assertTrue(" Expected key to be null since QRM for the message id had already arrived ",
          !regionqueue.getRegion().containsKey(new Long(1)));
    } catch (Exception e) {
      throw new AssertionError("test failed due to ", e);
    }
  }

  /**
   * test verifies correct expiry of ThreadIdentifier in the HARQ if no corresponding put comes
   */
  @Test
  public void testOnlyQRMComing() throws InterruptedException, IOException, ClassNotFoundException {
    HARegionQueueAttributes harqAttr = new HARegionQueueAttributes();
    harqAttr.setExpiryTime(1);
    // RegionQueue regionqueue = new HARegionQueue("testing", cache, harqAttr);
    HARegionQueue regionqueue = createHARegionQueue("testing", harqAttr);
    EventID id = new EventID(new byte[] {1}, 1, 1);
    long start = System.currentTimeMillis();
    regionqueue.removeDispatchedEvents(id);
    assertTrue(" Expected testingID to be present since only QRM achieved ",
        regionqueue.getRegion().containsKey(new ThreadIdentifier(new byte[] {1}, 1)));
    waitAtLeast(1000, start,
        () -> assertTrue(
            " Expected testingID not to be present since it should have expired after 2.5 seconds",
            !regionqueue.getRegion().containsKey(new ThreadIdentifier(new byte[] {1}, 1))));
  }

  /**
   * test all relevant data structures are updated on a local put
   */
  @Test
  public void testPutPath() {
    try {
      HARegionQueue regionqueue = createHARegionQueue("testing");
      Conflatable cf =
          new ConflatableObject("key", "value", new EventID(new byte[] {1}, 1, 1), true, "testing");
      regionqueue.put(cf);
      assertTrue(" Expected region peek to return cf but it is not so ",
          (regionqueue.peek().equals(cf)));
      assertTrue(
          " Expected the available id's size not  to be zero since expiry time has not  been exceeded but it is not so ",
          !(regionqueue.getAvalaibleIds().size() == 0));
      assertTrue(
          " Expected conflation map to have entry for this key since expiry time has not been exceeded but it is not so ",
          ((((Map) (regionqueue.getConflationMapForTesting().get("testing"))).get("key"))
              .equals(new Long(1))));
      assertTrue(
          " Expected eventID map size not to be zero since expiry time has not been exceeded but it is not so ",
          !(regionqueue.getEventsMapForTesting().size() == 0));

    } catch (Exception e) {
      throw new AssertionError("Exception occured in test due to ", e);
    }
  }

  /**
   * - adds 10 items - sets last dispatched as 5th - verify no data pertaining to the first five is
   * there - verify the next five entries and their relevant data is present
   */
  @Test
  public void testQRMDispatch() {
    try {
      HARegionQueue regionqueue = createHARegionQueue("testing");
      Conflatable[] cf = new Conflatable[10];
      // put 10 conflatable objects
      for (int i = 0; i < 10; i++) {
        cf[i] = new ConflatableObject("key" + i, "value", new EventID(new byte[] {1}, 1, i), true,
            "testing");
        regionqueue.put(cf[i]);
      }
      // remove the first 5 by giving the right sequence id
      regionqueue.removeDispatchedEvents(new EventID(new byte[] {1}, 1, 4));
      // verify 1-5 not in region
      for (long i = 1; i < 6; i++) {
        assertTrue(!regionqueue.getRegion().containsKey(new Long(i)));
      }
      // verify 6-10 still in region queue
      for (long i = 6; i < 11; i++) {
        assertTrue(regionqueue.getRegion().containsKey(new Long(i)));
      }
      // verify 1-5 not in conflation map
      for (long i = 0; i < 5; i++) {
        assertTrue(!((Map) regionqueue.getConflationMapForTesting().get("testing"))
            .containsKey("key" + i));
      }
      // verify 6-10 in conflation map
      for (long i = 5; i < 10; i++) {
        assertTrue(
            ((Map) regionqueue.getConflationMapForTesting().get("testing")).containsKey("key" + i));
      }

      EventID eid = new EventID(new byte[] {1}, 1, 6);
      // verify 1-5 not in eventMap
      for (long i = 1; i < 6; i++) {
        assertTrue(!regionqueue.getCurrentCounterSet(eid).contains(new Long(i)));
      }
      // verify 6-10 in event Map
      for (long i = 6; i < 11; i++) {
        assertTrue(regionqueue.getCurrentCounterSet(eid).contains(new Long(i)));
      }

      // verify 1-5 not in available Id's map
      for (long i = 1; i < 6; i++) {
        assertTrue(!regionqueue.getAvalaibleIds().contains(new Long(i)));
      }

      // verify 6-10 in available id's map
      for (long i = 6; i < 11; i++) {
        assertTrue(regionqueue.getAvalaibleIds().contains(new Long(i)));
      }
    } catch (Exception e) {
      throw new AssertionError("Exception occurred in test due to ", e);
    }
  }

  /**
   * - send Dispatch message for sequence id 7 - put from sequence id 1 - id 10 - verify data for
   * 1-7 not there - verify data for 8-10 is there
   */
  @Test
  public void testQRMBeforePut() {
    try {
      HARegionQueue regionqueue = createHARegionQueue("testing");

      EventID[] ids = new EventID[10];

      for (int i = 0; i < 10; i++) {
        ids[i] = new EventID(new byte[] {1}, 1, i);
      }

      // first get the qrm message for the seventh id
      regionqueue.removeDispatchedEvents(ids[6]);
      Conflatable[] cf = new Conflatable[10];
      // put 10 conflatable objects
      for (int i = 0; i < 10; i++) {
        cf[i] = new ConflatableObject("key" + i, "value", ids[i], true, "testing");
        regionqueue.put(cf[i]);
      }

      // verify 1-7 not in region
      Set values = (Set) regionqueue.getRegion().values();
      for (int i = 0; i < 7; i++) {
        System.out.println(i);
        assertTrue(!values.contains(cf[i]));
      }
      // verify 8-10 still in region queue
      for (int i = 7; i < 10; i++) {
        System.out.println(i);
        assertTrue(values.contains(cf[i]));
      }
      // verify 1-8 not in conflation map
      for (long i = 0; i < 7; i++) {
        assertTrue(!((Map) regionqueue.getConflationMapForTesting().get("testing"))
            .containsKey("key" + i));
      }
      // verify 8-10 in conflation map
      for (long i = 7; i < 10; i++) {
        assertTrue(
            ((Map) regionqueue.getConflationMapForTesting().get("testing")).containsKey("key" + i));
      }

      EventID eid = new EventID(new byte[] {1}, 1, 6);
      // verify 1-7 not in eventMap
      for (long i = 4; i < 11; i++) {
        assertTrue(!regionqueue.getCurrentCounterSet(eid).contains(new Long(i)));
      }
      // verify 8-10 in event Map
      for (long i = 1; i < 4; i++) {
        assertTrue(regionqueue.getCurrentCounterSet(eid).contains(new Long(i)));
      }

      // verify 1-7 not in available Id's map
      for (long i = 4; i < 11; i++) {
        assertTrue(!regionqueue.getAvalaibleIds().contains(new Long(i)));
      }

      // verify 8-10 in available id's map
      for (long i = 1; i < 4; i++) {
        assertTrue(regionqueue.getAvalaibleIds().contains(new Long(i)));
      }
    } catch (Exception e) {
      throw new AssertionError("Exception occurred in test due to ", e);
    }
  }

  /**
   * test to verify conflation happens as expected
   */
  @Test
  public void testConflation() {
    try {
      HARegionQueue regionqueue = createHARegionQueue("testing");
      EventID ev1 = new EventID(new byte[] {1}, 1, 1);
      EventID ev2 = new EventID(new byte[] {1}, 2, 2);
      Conflatable cf1 = new ConflatableObject("key", "value", ev1, true, "testing");
      Conflatable cf2 = new ConflatableObject("key", "value2", ev2, true, "testing");
      regionqueue.put(cf1);
      Map conflationMap = regionqueue.getConflationMapForTesting();
      assertTrue(((Map) (conflationMap.get("testing"))).get("key").equals(new Long(1)));
      regionqueue.put(cf2);
      // verify the conflation map has recorded the new key
      assertTrue(((Map) (conflationMap.get("testing"))).get("key").equals(new Long(2)));
      // the old key should not be present
      assertTrue(!regionqueue.getRegion().containsKey(new Long(1)));
      // available ids should not contain the old id (the old position)
      assertTrue(!regionqueue.getAvalaibleIds().contains(new Long(1)));
      // available id should have the new id (the new position)
      assertTrue(regionqueue.getAvalaibleIds().contains(new Long(2)));
      // events map should not contain the old position
      assertTrue(regionqueue.getCurrentCounterSet(ev1).isEmpty());
      // events map should contain the new position
      assertTrue(regionqueue.getCurrentCounterSet(ev2).contains(new Long(2)));

    } catch (Exception e) {
      throw new AssertionError("Exception occurred in test due to ", e);
    }
  }

  /**
   * Tests whether the QRM message removes the events correctly from the DACE & Conflation Map. The
   * events which are of ID greater than that contained in QRM should stay
   */
  @Test
  public void testQRM() {
    try {
      RegionQueue regionqueue = createHARegionQueue("testing");
      for (int i = 0; i < 10; ++i) {
        regionqueue.put(new ConflatableObject("key" + (i + 1), "value",
            new EventID(new byte[] {1}, 1, i + 1), true, "testing"));
      }
      EventID qrmID = new EventID(new byte[] {1}, 1, 5);
      ((HARegionQueue) regionqueue).removeDispatchedEvents(qrmID);
      Map conflationMap = ((HARegionQueue) regionqueue).getConflationMapForTesting();
      assertTrue(((Map) (conflationMap.get("testing"))).size() == 5);

      Set availableIDs = ((HARegionQueue) regionqueue).getAvalaibleIds();
      Set counters = ((HARegionQueue) regionqueue).getCurrentCounterSet(qrmID);
      assertTrue(availableIDs.size() == 5);
      assertTrue(counters.size() == 5);
      for (int i = 5; i < 10; ++i) {
        assertTrue(((Map) (conflationMap.get("testing"))).containsKey("key" + (i + 1)));
        assertTrue(availableIDs.contains(new Long((i + 1))));
        assertTrue(counters.contains(new Long((i + 1))));
      }
      Region rgn = ((HARegionQueue) regionqueue).getRegion();
      assertTrue(rgn.keys().size() == 6);

    } catch (Exception e) {
      throw new AssertionError("Exception occurred in test due to ", e);
    }
  }

  protected static HARegionQueue hrqFortestSafeConflationRemoval;

  /**
   * This test tests safe removal from the conflation map. i.e operations should only remove old
   * values and not the latest value
   */
  @Test
  public void testSafeConflationRemoval() {
    try {
      hrqFortestSafeConflationRemoval = new HARQTestClass("testSafeConflationRemoval",

          cache, this);
      Conflatable cf1 = new ConflatableObject("key1", "value", new EventID(new byte[] {1}, 1, 1),
          true, "testSafeConflationRemoval");
      hrqFortestSafeConflationRemoval.put(cf1);
      hrqFortestSafeConflationRemoval.removeDispatchedEvents(new EventID(new byte[] {1}, 1, 1));
      Map map =

          (Map) hrqFortestSafeConflationRemoval.getConflationMapForTesting()
              .get("testSafeConflationRemoval");
      assertTrue(
          "Expected the counter to be 2 since it should not have been deleted but it is not so ",
          map.get("key1").equals(new Long(2)));
      hrqFortestSafeConflationRemoval = null;
    } catch (Exception e) {
      throw new AssertionError("Test failed due to ", e);
    }
  }

  /**
   * Extends HARegionQueue for testing purposes. used by testSafeConflationRemoval
   */
  static class HARQTestClass extends HARegionQueue.TestOnlyHARegionQueue {

    public HARQTestClass(String REGION_NAME, Cache cache, HARegionQueueJUnitTest test)
        throws IOException, ClassNotFoundException, CacheException, InterruptedException {
      super(REGION_NAME, cache);
    }

    ConcurrentMap createConcurrentMap() {
      return new ConcHashMap();
    }
  }

  /**
   * Used to override the remove method for testSafeConflationRemoval
   */
  static class ConcHashMap extends ConcurrentHashMap implements ConcurrentMap {
    public boolean remove(Object arg0, Object arg1) {
      Conflatable cf2 = new ConflatableObject("key1", "value2", new EventID(new byte[] {1}, 1, 2),
          true, "testSafeConflationRemoval");
      try {
        hrqFortestSafeConflationRemoval.put(cf2);
      } catch (Exception e) {
        throw new AssertionError("Exception occurred in trying to put ", e);
      }
      return super.remove(arg0, arg1);
    }
  }

  static List list1;

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
  public void testConcurrentDispatcherAndRemovalForSameRegionSameThreadId() {
    try {
      final long numberOfIterations = 1000;
      final HARegionQueue hrq = createHARegionQueue("testConcurrentDispatcherAndRemoval");
      HARegionQueue.stopQRMThread();
      final ThreadIdentifier[] ids = new ThreadIdentifier[(int) numberOfIterations];
      for (int i = 0; i < numberOfIterations; i++) {
        ids[i] = new ThreadIdentifier(new byte[] {1}, i);
        hrq.addDispatchedMessage(ids[i], i);
      }
      Thread thread1 = new Thread() {
        public void run() {
          try {
            Thread.sleep(600);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          list1 = HARegionQueue.createMessageListForTesting();
        };
      };
      Thread thread2 = new Thread() {
        public void run() {
          try {
            Thread.sleep(480);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          for (int i = 0; i < numberOfIterations; i++) {
            hrq.addDispatchedMessage(ids[i], i + numberOfIterations);
          }
        };
      };
      thread1.start();
      thread2.start();
      ThreadUtils.join(thread1, 30 * 1000);
      ThreadUtils.join(thread2, 30 * 1000);
      List list2 = HARegionQueue.createMessageListForTesting();
      Iterator iterator = list1.iterator();
      boolean doOnce = false;
      EventID id = null;
      Map map = new HashMap();
      while (iterator.hasNext()) {
        if (!doOnce) {
          iterator.next();
          iterator.next();
          doOnce = true;
        } else {
          id = (EventID) iterator.next();
          map.put(new Long(id.getThreadID()), new Long(id.getSequenceID()));
        }
      }
      iterator = list2.iterator();
      doOnce = false;
      id = null;
      while (iterator.hasNext()) {
        if (!doOnce) {
          iterator.next();
          iterator.next();
          doOnce = true;
        } else {
          id = (EventID) iterator.next();
          map.put(new Long(id.getThreadID()), new Long(id.getSequenceID()));
        }
      }
      iterator = map.values().iterator();
      Long max = new Long(numberOfIterations);
      Long next;
      while (iterator.hasNext()) {
        next = ((Long) iterator.next());
        assertTrue(" Expected all the sequence ID's to be greater than " + max
            + " but it is not so. Got sequence id " + next, next.compareTo(max) >= 0);
      }
    } catch (Exception e) {
      throw new AssertionError("Test failed due to : ", e);
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
  public void testConcurrentDispatcherAndRemovalForSameRegionDifferentThreadId() {
    try {
      final long numberOfIterations = 1000;
      final HARegionQueue hrq = createHARegionQueue("testConcurrentDispatcherAndRemoval");
      HARegionQueue.stopQRMThread();
      final ThreadIdentifier[] ids = new ThreadIdentifier[(int) numberOfIterations];
      for (int i = 0; i < numberOfIterations; i++) {
        ids[i] = new ThreadIdentifier(new byte[] {1}, i);
        hrq.addDispatchedMessage(ids[i], i);
      }
      Thread thread1 = new Thread() {
        public void run() {
          try {
            Thread.sleep(600);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          list1 = HARegionQueue.createMessageListForTesting();
        };
      };
      Thread thread2 = new Thread() {
        public void run() {
          try {
            Thread.sleep(480);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          for (int i = 0; i < numberOfIterations; i++) {
            ids[i] = new ThreadIdentifier(new byte[] {1}, i + numberOfIterations);
            hrq.addDispatchedMessage(ids[i], i + numberOfIterations);
          }
        };
      };
      thread1.start();
      thread2.start();
      ThreadUtils.join(thread1, 30 * 1000);
      ThreadUtils.join(thread2, 30 * 1000);
      List list2 = HARegionQueue.createMessageListForTesting();
      Iterator iterator = list1.iterator();
      boolean doOnce = false;
      EventID id = null;
      Map map = new HashMap();
      while (iterator.hasNext()) {
        if (!doOnce) {
          iterator.next();
          iterator.next();
          doOnce = true;
        } else {
          id = (EventID) iterator.next();
          map.put(new Long(id.getThreadID()), new Long(id.getSequenceID()));
        }
      }
      iterator = list2.iterator();
      doOnce = false;
      id = null;
      while (iterator.hasNext()) {
        if (!doOnce) {
          iterator.next();
          iterator.next();
          doOnce = true;
        } else {
          id = (EventID) iterator.next();
          map.put(new Long(id.getThreadID()), new Long(id.getSequenceID()));
        }
      }
      assertTrue(
          " Expected the map size to be " + (2 * numberOfIterations) + " but it is " + map.size(),
          map.size() == (2 * numberOfIterations));
    } catch (Exception e) {
      throw new AssertionError("Test failed due to an unexpected exception : ", e);
    }
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
  public void testConcurrentDispatcherAndRemovalForMultipleRegionsSameThreadId() {
    try {
      final long numberOfIterations = 10000;
      final HARegionQueue hrq1 = createHARegionQueue("testConcurrentDispatcherAndRemoval1");

      final HARegionQueue hrq2 = createHARegionQueue("testConcurrentDispatcherAndRemoval2");

      final HARegionQueue hrq3 = createHARegionQueue("testConcurrentDispatcherAndRemoval3");

      final HARegionQueue hrq4 = createHARegionQueue("testConcurrentDispatcherAndRemoval4");

      final HARegionQueue hrq5 = createHARegionQueue("testConcurrentDispatcherAndRemoval5");

      HARegionQueue.stopQRMThread();
      final ThreadIdentifier[] ids = new ThreadIdentifier[(int) numberOfIterations];

      for (int i = 0; i < numberOfIterations; i++) {
        ids[i] = new ThreadIdentifier(new byte[] {1}, i);
        hrq1.addDispatchedMessage(ids[i], i);
        hrq2.addDispatchedMessage(ids[i], i);

      }

      Thread thread1 = new Thread() {
        public void run() {
          try {
            Thread.sleep(600);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          list1 = HARegionQueue.createMessageListForTesting();
        };
      };
      Thread thread2 = new Thread() {
        public void run() {
          try {
            Thread.sleep(480);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          for (int i = 0; i < numberOfIterations; i++) {
            hrq3.addDispatchedMessage(ids[i], i);
            hrq4.addDispatchedMessage(ids[i], i);
            hrq5.addDispatchedMessage(ids[i], i);
          }
        };
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
          iterator.next();// region name;
          int size = ((Integer) iterator.next()).intValue();
          for (int i = 0; i < size; i++) {
            id = (EventID) iterator.next();
            map.put(new ThreadIdentifier(id.getMembershipID(), id.getThreadID()),
                new Long(id.getSequenceID()));
          }
        }
      }

      iterator = list2.iterator();
      doOnce = true;
      id = null;
      while (iterator.hasNext()) {
        if (!doOnce) {
          iterator.next(); // read the total message size
          doOnce = true;
        } else {
          iterator.next();// region name;
          int size = ((Integer) iterator.next()).intValue();
          for (int i = 0; i < size; i++) {
            id = (EventID) iterator.next();
            map.put(new ThreadIdentifier(id.getMembershipID(), id.getThreadID()),
                new Long(id.getSequenceID()));
          }
        }
      }
      assertTrue(
          " Expected the map size to be " + (numberOfIterations) + " but it is " + map.size(),
          map.size() == (numberOfIterations));

    } catch (Exception e) {
      throw new AssertionError("Test failed due to : ", e);
    }
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
  public void testConcurrentDispatcherAndRemovalForMultipleRegionsDifferentThreadId() {
    try {
      final long numberOfIterations = 1000;
      final HARegionQueue hrq1 =

          createHARegionQueue("testConcurrentDispatcherAndRemoval1");

      final HARegionQueue hrq2 =

          createHARegionQueue("testConcurrentDispatcherAndRemoval2");
      final HARegionQueue hrq3 =

          createHARegionQueue("testConcurrentDispatcherAndRemoval3");
      final HARegionQueue hrq4 =

          createHARegionQueue("testConcurrentDispatcherAndRemoval4");
      final HARegionQueue hrq5 =

          createHARegionQueue("testConcurrentDispatcherAndRemoval5");

      HARegionQueue.stopQRMThread();

      final ThreadIdentifier[] ids1 = new ThreadIdentifier[(int) numberOfIterations];
      final ThreadIdentifier[] ids2 = new ThreadIdentifier[(int) numberOfIterations];
      final ThreadIdentifier[] ids3 = new ThreadIdentifier[(int) numberOfIterations];
      final ThreadIdentifier[] ids4 = new ThreadIdentifier[(int) numberOfIterations];
      final ThreadIdentifier[] ids5 = new ThreadIdentifier[(int) numberOfIterations];

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
        public void run() {
          try {
            Thread.sleep(600);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          list1 = HARegionQueue.createMessageListForTesting();
        };
      };
      Thread thread2 = new Thread() {
        public void run() {
          try {
            Thread.sleep(480);
          } catch (InterruptedException e) {
            fail("Interrupted");
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
        };
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
          iterator.next();// region name;
          int size = ((Integer) iterator.next()).intValue();
          System.out.println(" size of list 1 iteration x " + size);
          for (int i = 0; i < size; i++) {

            id = (EventID) iterator.next();
            map.put(new ThreadIdentifier(id.getMembershipID(), id.getThreadID()),
                new Long(id.getSequenceID()));
          }
        }
      }

      iterator = list2.iterator();
      doOnce = true;
      id = null;
      while (iterator.hasNext()) {
        if (!doOnce) {
          iterator.next(); // read the total message size
          doOnce = true;
        } else {
          iterator.next();// region name;
          int size = ((Integer) iterator.next()).intValue();
          System.out.println(" size of list 2 iteration x " + size);
          for (int i = 0; i < size; i++) {
            id = (EventID) iterator.next();
            map.put(new ThreadIdentifier(id.getMembershipID(), id.getThreadID()),
                new Long(id.getSequenceID()));
          }
        }
      }

      assertTrue(" Expected the map size to be " + (numberOfIterations * 2 * 5) + " but it is "
          + map.size(), map.size() == (numberOfIterations * 2 * 5));

    } catch (Exception e) {
      throw new AssertionError("Test failed due to : ", e);
    }
  }

  /**
   * Concurrent Peek on Blokcing Queue waiting with for a Put . If concurrent take is also happening
   * such that the object is removed first then the peek should block & not return with null.
   */
  @Test
  public void testBlockingQueueForConcurrentPeekAndTake() {
    exceptionInThread = false;
    testFailed = false;
    try {
      final TestBlockingHARegionQueue bQ =
          new TestBlockingHARegionQueue("testBlockQueueForConcurrentPeekAndTake", cache);
      Thread[] threads = new Thread[3];
      for (int i = 0; i < 3; i++) {
        threads[i] = new Thread() {
          public void run() {
            try {
              long startTime = System.currentTimeMillis();
              Object obj = bQ.peek();
              if (obj == null) {
                testFailed = true;
                message.append(
                    " Failed :  failed since object was null and was not expected to be null \n");
              }
              long totalTime = System.currentTimeMillis() - startTime;

              if (totalTime < 4000) {
                testFailed = true;
                message
                    .append(" Failed :  Expected time to be greater than 4000 but it is not so ");
              }
            } catch (Exception e) {
              exceptionInThread = true;
              exception = e;
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

      bQ.takeFirst = true;
      bQ.put(new ConflatableObject("key", "value", id, true, "testing"));

      Thread.sleep(2000);

      bQ.put(new ConflatableObject("key1", "value1", id1, true, "testing"));

      long startTime = System.currentTimeMillis();
      for (int k = 0; k < 3; k++) {
        ThreadUtils.join(threads[k], 180 * 1000);
      }

      long totalTime = System.currentTimeMillis() - startTime;

      if (totalTime >= 180000) {
        fail(" Test taken too long ");
      }

      if (testFailed) {
        fail(" test failed due to " + message);
      }

    } catch (Exception e) {
      throw new AssertionError(" Test failed due to ", e);
    }
  }

  /**
   * Peek on a blocking queue is in progress & the single element is removed either by take or by
   * QRM thread , the peek should block correctly.
   */
  @Test
  public void testBlockingQueueForTakeWhenPeekInProgress() {
    exceptionInThread = false;
    testFailed = false;
    try {
      final TestBlockingHARegionQueue bQ =
          new TestBlockingHARegionQueue("testBlockQueueForTakeWhenPeekInProgress", cache);
      Thread[] threads = new Thread[3];
      for (int i = 0; i < 3; i++) {
        threads[i] = new Thread() {
          public void run() {
            try {
              long startTime = System.currentTimeMillis();
              Object obj = bQ.peek();
              if (obj == null) {
                testFailed = true;
                message.append(
                    " Failed :  failed since object was null and was not expected to be null \n");
              }
              long totalTime = System.currentTimeMillis() - startTime;

              if (totalTime < 4000) {
                testFailed = true;
                message
                    .append(" Failed :  Expected time to be greater than 4000 but it is not so ");
              }
            } catch (Exception e) {
              exceptionInThread = true;
              exception = e;
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

      bQ.takeWhenPeekInProgress = true;
      bQ.put(new ConflatableObject("key", "value", id, true, "testing"));

      Thread.sleep(2000);

      bQ.put(new ConflatableObject("key1", "value1", id1, true, "testing"));

      long startTime = System.currentTimeMillis();
      for (int k = 0; k < 3; k++) {
        ThreadUtils.join(threads[k], 60 * 1000);
      }

      long totalTime = System.currentTimeMillis() - startTime;

      if (totalTime >= 60000) {
        fail(" Test taken too long ");
      }

      if (testFailed) {
        fail(" test failed due to " + message);
      }

    } catch (Exception e) {
      throw new AssertionError(" Test failed due to ", e);
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
  public void testConcurrentEventExpiryAndTake() {
    try {
      HARegionQueueAttributes haa = new HARegionQueueAttributes();
      haa.setExpiryTime(3);
      final RegionQueue regionqueue =
          new HARegionQueue.TestOnlyHARegionQueue("testing", cache, haa) {
            CacheListener createCacheListenerForHARegion() {

              return new CacheListenerAdapter() {

                public void afterInvalidate(EntryEvent event) {

                  if (event.getKey() instanceof Long) {
                    synchronized (HARegionQueueJUnitTest.this) {
                      expiryCalled = true;
                      HARegionQueueJUnitTest.this.notify();

                    } ;
                    Thread.yield();

                    synchronized (HARegionQueueJUnitTest.this) {
                      if (!allowExpiryToProceed) {
                        try {
                          HARegionQueueJUnitTest.this.wait();
                        } catch (InterruptedException e1) {
                          encounteredException = true;
                        }
                      }
                    }
                    try {
                      expireTheEventOrThreadIdentifier(event);
                    } catch (CacheException e) {
                      e.printStackTrace();
                      encounteredException = true;
                    } finally {
                      synchronized (HARegionQueueJUnitTest.this) {
                        complete = true;
                        HARegionQueueJUnitTest.this.notify();
                      }
                    }
                  }
                }
              };
            }
          };
      EventID ev1 = new EventID(new byte[] {1}, 1, 1);

      Conflatable cf1 = new ConflatableObject("key", "value", ev1, true, "testing");

      regionqueue.put(cf1);
      synchronized (this) {
        if (!expiryCalled) {
          this.wait();
        }
      }
      try {
        Object o = regionqueue.take();
        assertNull(o);
      } catch (Exception e) {
        throw new AssertionError("Test failed due to exception ", e);
      } finally {
        synchronized (this) {
          this.allowExpiryToProceed = true;
          this.notify();
        }
      }
      synchronized (this) {
        if (!this.complete) {
          this.wait();
        }
      }
      assertTrue("Test failed due to exception ", !encounteredException);
    } catch (Exception e) {
      throw new AssertionError("test failed due to ", e);
    }
  }

  /**
   * This test validates that if sequence violation occurs without GII,the put will throw an
   * exception
   *
   * Test is commented as assertion for sequence violation is removed in the
   * source(HARegionQueue.putObject) is removed. - Suyog
   */
  @Ignore("TODO: disabled for unknown reason")
  @Test
  public void testExceptionInPutForSequenceViolationWithoutGII() {
    DistributedSystem ds = cache.getDistributedSystem();
    cache.close();
    ds.disconnect();
    Properties props = new Properties();
    props.put(LOG_LEVEL, "config");
    // props.put("mcast-port","11111");
    try {
      cache = CacheFactory.create(DistributedSystem.connect(props));
    } catch (Exception e1) {
      throw new AssertionError("Test failed because of exception. Exception=", e1);
    }

    HARegionQueue rq = null;
    try {
      // Create a HAregionQueue.
      rq = HARegionQueue.getHARegionQueueInstance("testException", cache,
          HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    } catch (Exception e) {
      throw new AssertionError("Test failed because of exception. Exception=", e);
    }
    ConflatableObject cf1 =
        new ConflatableObject("key1", "val1", new EventID(new byte[] {1}, 1, 2), false, "test");
    ConflatableObject cf2 =
        new ConflatableObject("key1", "val1", new EventID(new byte[] {1}, 1, 1), false, "test");

    try {
      rq.put(cf1);
    } catch (Exception e) {
      throw new AssertionError("Test failed because of exception. Exception=", e);
    }

    try {
      rq.put(cf2);
      fail("Test failed because asertion error was expected but there was'nt any"); // TODO: this is
                                                                                    // broken, the
                                                                                    // following
                                                                                    // catch will
                                                                                    // catch this or
                                                                                    // what's thrown
                                                                                    // by put
    } catch (AssertionError ignore) {
      System.out.println("Got the right assertion failure");
    } catch (Exception e) {
      e.printStackTrace();
      throw new AssertionError("Test failed because of exception. Exception=", e);
    }
  }

  /**
   * Tests the functionality of batch peek & remove with blocking & non blocking HARegionQueue
   */
  @Test
  public void testBatchPeekWithRemoveForNonBlockingQueue() {
    testBatchPeekWithRemove(false);
  }

  /**
   * Tests the functionality of batch peek & remove with blocking & non blocking HARegionQueue
   */
  @Test
  public void testBatchPeekWithRemoveForBlockingQueue() {
    testBatchPeekWithRemove(true);
  }

  private void testBatchPeekWithRemove(boolean createBlockingQueue) {
    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    haa.setExpiryTime(300);
    try {
      HARegionQueue regionqueue = createBlockingQueue
          ? HARegionQueue.getHARegionQueueInstance("testing", cache, haa,
              HARegionQueue.BLOCKING_HA_QUEUE, false)
          : HARegionQueue.getHARegionQueueInstance("testing", cache, haa,
              HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
      for (int i = 0; i < 10; ++i) {
        EventID ev1 = new EventID(new byte[] {1}, 1, i);
        Conflatable cf1 = new ConflatableObject("key", "value", ev1, false, "testing");
        regionqueue.put(cf1);
      }

      List objs = regionqueue.peek(10, 5000);
      assertEquals(10, objs.size());
      Iterator itr = objs.iterator();
      int j = 0;
      while (itr.hasNext()) {
        Conflatable conf = (Conflatable) itr.next();
        assertNotNull(conf);
        assertTrue("The sequence ID of the objects in the queue are not as expected",
            conf.getEventId().getSequenceID() == j++);
      }
      regionqueue.remove();
      assertEquals(0, regionqueue.size());

    } catch (Exception e) {
      throw new AssertionError("Test failed bcoz of exception =", e);
    }
  }

  /**
   * tests whether expiry of entry in the regin queue occurs as expected using system property to
   * set expiry
   */
  @Test
  public void testExpiryUsingSystemProperty()
      throws InterruptedException, IOException, ClassNotFoundException {
    try {
      System.setProperty(HARegionQueue.REGION_ENTRY_EXPIRY_TIME, "1");

      HARegionQueueAttributes haa = new HARegionQueueAttributes();
      HARegionQueue regionqueue = createHARegionQueue("testing", haa);
      long start = System.currentTimeMillis();
      regionqueue.put(new ConflatableObject("key", "value", new EventID(new byte[] {1}, 1, 1), true,
          "testing"));
      Map map = (Map) regionqueue.getConflationMapForTesting().get("testing");
      assertTrue(!map.isEmpty());

      waitAtLeast(1000, start, () -> {
        assertEquals(Collections.EMPTY_MAP, map);
        assertEquals(Collections.EMPTY_SET, regionqueue.getRegion().keys());
      });
    } finally {
      // [yogi]system property set to null, to avoid using it in the subsequent tests
      System.setProperty(HARegionQueue.REGION_ENTRY_EXPIRY_TIME, "");
    }
  }

  /**
   * This tests whether the messageSyncInterval for QueueRemovalThread is refreshed properly when
   * set/updated using cache's setter API
   */
  @Test
  public void testUpdationOfMessageSyncInterval() throws Exception {
    int initialMessageSyncInterval = 5;
    cache.setMessageSyncInterval(initialMessageSyncInterval);
    createHARegionQueue("testUpdationOfMessageSyncInterval");

    assertEquals("messageSyncInterval not set properly", initialMessageSyncInterval,
        HARegionQueue.getMessageSyncInterval());

    int updatedMessageSyncInterval = 10;
    cache.setMessageSyncInterval(updatedMessageSyncInterval);


    Awaitility.await().atMost(1, TimeUnit.MINUTES)
        .until(() -> assertEquals("messageSyncInterval not updated.", updatedMessageSyncInterval,
            HARegionQueue.getMessageSyncInterval()));
  }
}
