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
package com.gemstone.gemfire.internal.cache.ha;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * JUnit test for verifying the proper functioning of HARegionQueue related
 * statistics.
 * 
 * 
 */
@Category(IntegrationTest.class)
public class HARegionQueueStatsJUnitTest
{

  /** The cache instance */
  protected Cache cache = null;

  /**
   * Create the cache in setup. Currently the HA related stats are active under
   * fine logging only.
   * 
   * @throws Exception -
   *           thrown if any exception occurs in setUp
   */
  @Before
  public void setUp() throws Exception
  {
    cache = createCache();
  }

  /**
   * Close the cache in tear down *
   * 
   * @throws Exception -
   *           thrown if any exception occurs in tearDown
   */
  @After
  public void tearDown() throws Exception
  {
    cache.close();
  }

  /**
   * Creates the cache instance for the test
   * 
   * @return the cache instance
   * @throws CacheException -
   *           thrown if any exception occurs in cache creation
   */
  private Cache createCache() throws CacheException
  {
    return new CacheFactory().set(MCAST_PORT, "0").create();
  }

  /**
   * Creates a HARegionQueue object.
   * 
   * @param name -
   *          name of the underlying region for region-queue
   * @return the HARegionQueue instance
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws CacheException
   * @throws InterruptedException
   */
  protected HARegionQueue createHARegionQueue(String name)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException
  {
    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name,
        cache, HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

  /**
   * Creates a HARegionQueue object.
   * 
   * @param name -
   *          name of the underlying region for region-queue
   * @param attrs -
   *          attributes for the HARegionQueue
   * @return the HARegionQueue instance
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws CacheException
   * @throws InterruptedException
   */
  protected HARegionQueue createHARegionQueue(String name,
      HARegionQueueAttributes attrs) throws IOException, ClassNotFoundException, CacheException, InterruptedException
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name,
        cache, attrs, HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

  /**
   * This test does the following:<br>
   * 1)Create HARegionQueue<br>
   * 2)Add objects with unique eventids and conflation false <br>
   * 3)Verify that statistics object is not null<br>
   * 4)Verify that total events added matches the eventsEnqued stats<br>
   * 5)Verify that eventsConflated stats is zero.
   * 
   * @throws Exception -
   *           thrown if any problem occurs in test execution
   */
  @Test
  public void testPutStatsNoConflation() throws Exception
  {

    HARegionQueue rq = createHARegionQueue("testPutStatsNoConflation");
    Conflatable cf = null;

    int totalEvents = 100;
    for (int i = 0; i < totalEvents; i++) {
      cf = new ConflatableObject("key" + i, "value" + i, new EventID(
          new byte[] { 1 }, 1, i), false, "testing");
      rq.put(cf);
    }

    HARegionQueueStats stats = rq.getStatistics();
    assertNotNull("stats for HARegionQueue found null", stats);

    assertEquals(
        "eventsEnqued by stats not equal to the actual number of events added to the queue",
        totalEvents, stats.getEventsEnqued());

    assertEquals("eventsConflated by stats not equal zero", 0, stats
        .getEventsConflated());
  }

  /**
   * This test does the following:<br>
   * 1)Create HARegionQueue<br>
   * 2)Add objects with unique eventids and conflation true with same Key. <br>
   * 3)Verify that statistics object is not null<br>
   * 4)Verify that total events added matches the eventsEnqued stats<br>
   * 5)Verify that eventsConflated stats is total events added minus 1.
   * 
   * @throws Exception -
   *           thrown if any problem occurs in test execution
   */
  @Test
  public void testPutStatsConflationEnabled() throws Exception
  {

    HARegionQueue rq = createHARegionQueue("testPutStatsConflationEnabled");
    Conflatable cf = null;

    int totalEvents = 100;
    for (int i = 0; i < totalEvents; i++) {
      cf = new ConflatableObject("key", "value" + i, new EventID(
          new byte[] { 1 }, 1, i), true, "testing");
      rq.put(cf);
    }

    HARegionQueueStats stats = rq.getStatistics();
    assertNotNull("stats for HARegionQueue found null", stats);

    assertEquals(
        "eventsEnqued by stats not equal to the actual number of events added to the queue",
        totalEvents, stats.getEventsEnqued());

    assertEquals("stats for eventsConflated mismatched", totalEvents - 1, stats
        .getEventsConflated());
  }

  /**
   * This test does the following:<br>
   * 1)Create HARegionQueue with expiry time as 1 sec<br>
   * 2)Add objects with unique eventids and conflation false and sleep for some
   * time.<br>
   * 3)Verify that statistics object is not null<br>
   * 4)Verify that total events added matches the eventsEnqued stats<br>
   * 5)Verify that eventsExpired stats is same as total events added as all
   * events should have expired by 1 sec.
   * 
   * @throws Exception -
   *           thrown if any problem occurs in test execution
   */
  @Test
  public void testExpiryStats() throws Exception
  {

    HARegionQueueAttributes haa = new HARegionQueueAttributes();
    haa.setExpiryTime(1);
    HARegionQueue rq = createHARegionQueue("testExpiryStats", haa);

    Conflatable cf = null;
    int totalEvents = 100;
    for (int i = 0; i < totalEvents; i++) {
      cf = new ConflatableObject("key" + i, "value" + i, new EventID(
          new byte[] { 1 }, 1, i), false, "testing");
      rq.put(cf);
    }

    Thread.sleep(3000);

    HARegionQueueStats stats = rq.stats;
    assertNotNull("stats for HARegionQueue found null", stats);
    assertEquals(
        "eventsEnqued by stats not equal to the actual number of events added to the queue",
        totalEvents, stats.getEventsEnqued());
    assertEquals("expiredEvents not updated", totalEvents, stats
        .getEventsExpired());
  }

  /**
   * This test does the following:<br>
   * 1)Create HARegionQueue.<br>
   * 2)Add objects with unique eventids and conflation false<br>
   * 3)Do some random peek and peek-batch operations and then call remove()<br>
   * 4)Verify that statistics object is not null<br>
   * 5)Verify that total events added matches the eventsEnqued stats<br>
   * 6)Verify that eventsRemoved stats is same as the maximum batch size peeked
   * in above peek operations(step 3).
   * 
   * @throws Exception -
   *           thrown if any problem occurs in test execution
   */
  @Test
  public void testRemoveStats() throws Exception
  {
    HARegionQueue rq = createHARegionQueue("testRemoveStats");
    Conflatable cf = null;

    int totalEvents = 100;
    for (int i = 0; i < totalEvents; i++) {
      cf = new ConflatableObject("key" + i, "value" + i, new EventID(
          new byte[] { 1 }, 1, i), false, "testing");
      rq.put(cf);
    }

    // do some random peek operations.
    int maxPeekBatchSize = 50;
    rq.peek();
    rq.peek(8);
    rq.peek(maxPeekBatchSize);
    rq.peek(35);
    rq.peek();

    rq.remove();

    HARegionQueueStats stats = rq.getStatistics();
    assertNotNull("stats for HARegionQueue found null", stats);

    assertEquals(
        "eventsEnqued by stats not equal to the actual number of events added to the queue",
        totalEvents, stats.getEventsEnqued());

    assertEquals("All the events peeked were not removed", maxPeekBatchSize,
        stats.getEventsRemoved());
  }

  /**
   * This test does the following:<br>
   * 1)Create HARegionQueue.<br>
   * 2)Add objects with unique eventids and conflation false<br>
   * 3)Do some take and take-batch operations.<br>
   * 4)Verify that statistics object is not null<br>
   * 5)Verify that total events added matches the eventsEnqued stats<br>
   * 6)Verify that eventsTaken stats is same as the sum of events taken in batch
   * and individually (Step 3)
   * 
   * @throws Exception -
   *           thrown if any problem occurs in test execution
   */
  @Test
  public void testTakeStats() throws Exception
  {
    HARegionQueue rq = createHARegionQueue("testTakeStats");
    Conflatable cf = null;

    int totalEvents = 100;
    for (int i = 0; i < totalEvents; i++) {
      cf = new ConflatableObject("key" + i, "value" + i, new EventID(
          new byte[] { 1 }, 1, i), false, "testing");
      rq.put(cf);
    }

    int takeInBatch = 50;
    int takeOneByOne = 25;
    rq.take(takeInBatch);
    for (int i = 0; i < takeOneByOne; i++) {
      rq.take();
    }

    HARegionQueueStats stats = rq.getStatistics();
    assertNotNull("stats for HARegionQueue found null", stats);

    assertEquals(
        "eventsEnqued by stats not equal to the actual number of events added to the queue",
        totalEvents, stats.getEventsEnqued());

    assertEquals("eventsTaken stats not matching with actual events taken",
        (takeInBatch + takeOneByOne), stats.getEventsTaken());
  }

  /**
   * This test does the following:<br>
   * 1)Create HARegionQueue.<br>
   * 2)Add objects with unique eventids and conflation false<br>
   * 3)Remove the events through QRM api (
   * <code>removeDispatchedEvents(EventID id)</code>) with a certain
   * lastDispatchedSeqId<br>
   * 4)Verify that statistics object is not null<br>
   * 5)Verify that total events added matches the eventsEnqued stats<br>
   * 6)Verify that eventsRemovedByQrm stats is same as the number of events
   * removed by QRM (upto the event having lastDispatchedSeqId, step 3).
   * 
   * @throws Exception -
   *           thrown if any problem occurs in test execution
   */
  @Test
  public void testRemoveByQrmStats() throws Exception
  {
    HARegionQueue rq = createHARegionQueue("testRemoveByQrmStats");
    Conflatable cf = null;

    int totalEvents = 100;
    for (int i = 0; i < totalEvents; i++) {
      cf = new ConflatableObject("key" + i, "value" + i, new EventID(
          new byte[] { 1 }, 1, i), false, "testing");
      rq.put(cf);
    }

    // call for removal thru QRM api
    int lastDispatchedSqId = 20;
    EventID id = new EventID(new byte[] { 1 }, 1, lastDispatchedSqId);
    rq.removeDispatchedEvents(id);

    HARegionQueueStats stats = rq.getStatistics();
    assertNotNull("stats for HARegionQueue found null", stats);

    assertEquals(
        "eventsEnqued by stats not equal to the actual number of events added to the queue",
        totalEvents, stats.getEventsEnqued());
    assertEquals("eventsRemovedByQrm stats not updated properly",
        (lastDispatchedSqId + 1), stats.getEventsRemovedByQrm());

  }

  /**
   * This test does the following:<br>
   * 1)Create HARegionQueue.<br>
   * 2)Add objects with unique eventids as well as ThreadIDs and conflation
   * false<br>
   * 3)Verify that statistics object is not null<br>
   * 4)Verify that total events added matches the eventsEnqued stats<br>
   * 5)Verify that threadIdentifiers stats is same as the number of events added
   * as all the events had different ThreadIdentifier objects.
   * 
   * @throws Exception -
   *           thrown if any problem occurs in test execution
   */
  @Test
  public void testThreadIdentifierStats() throws Exception
  {
    HARegionQueue rq = createHARegionQueue("testRemoveByQrmStats");
    Conflatable cf = null;

    int totalEvents = 100;
    for (int i = 0; i < totalEvents; i++) {
      cf = new ConflatableObject("key" + i, "value" + i, new EventID(
          new byte[] { 1 }, i, i), false, "testing");
      rq.put(cf);
    }

    HARegionQueueStats stats = rq.getStatistics();
    assertNotNull("stats for HARegionQueue found null", stats);

    assertEquals(
        "eventsEnqued by stats not equal to the actual number of events added to the queue",
        totalEvents, stats.getEventsEnqued());
    assertEquals("threadIdentifiers stats not updated properly", totalEvents,
        stats.getThreadIdentiferCount());

  }

  /**
   * This test does the following:<br>
   * 1)Create HARegionQueue.<br>
   * 2)Add objects with unique eventids and conflation false<br>
   * 3)peek a batch to peek all the events added and take() all the events<br>
   * 4)Call remove()<br>
   * 5)Verify that statistics object is not null<br>
   * 6)Verify that total events added matches the eventsEnqued stats<br>
   * 7)Verify that numVoidRemovals stats is same as the total events added since
   * all the peeked events were removed by take() call and remove() was a void
   * operation.
   * 
   * @throws Exception -
   *           thrown if any problem occurs in test execution
   */
  @Test
  public void testVoidRemovalStats() throws Exception
  {
    HARegionQueue rq = createHARegionQueue("testVoidRemovalStats");
    Conflatable cf = null;

    int totalEvents = 100;
    for (int i = 0; i < totalEvents; i++) {
      cf = new ConflatableObject("key" + i, "value" + i, new EventID(
          new byte[] { 1 }, 1, i), false, "testing");
      rq.put(cf);
    }

    rq.peek(totalEvents);
    rq.take(totalEvents);
    rq.remove();

    HARegionQueueStats stats = rq.getStatistics();
    assertNotNull("stats for HARegionQueue found null", stats);

    assertEquals(
        "eventsEnqued by stats not equal to the actual number of events added to the queue",
        totalEvents, stats.getEventsEnqued());

    assertEquals(
        "Number of void removals shud be equal to total peeked since all the events were removed by take() before remove()",
        totalEvents, stats.getNumVoidRemovals());
  }
  /**
   * This test does the follwing: <br>
   * 1)Create HARegionQueue.<br>
   * 2)Add objects with unique eventids and conflation false.<br>
   * 3)Add some objects with same eventids(sequence ids)- duplicate events.<br>
   * 4)Verify that numSequenceViolated stats is same as number of duplicate
   * events.<br>
   * 5)Verify that eventsEnqued stats is same as the queue size ( i.e.
   * eventsEnqued stats is not updated for duplicate events.)
   * 
   * @throws Exception
   */
  @Test
  public void testSequenceViolationStats() throws Exception
  {
    HARegionQueue rq = createHARegionQueue("testSequenceViolationStats");
    Conflatable cf = null;

    int totalEvents = 10;
    for (int i = 0; i < totalEvents; i++) {
      cf = new ConflatableObject("key" + i, "value" + i, new EventID(
          new byte[] { 1 }, 1, i), false, "testing");
      rq.put(cf);
    }

    int seqViolated = 3;
    for (int i = 0; i < seqViolated; i++) {
      cf = new ConflatableObject("key" + i, "value" + i, new EventID(
          new byte[] { 1 }, 1, i), false, "testing");
      rq.put(cf);
    }

    HARegionQueueStats stats = rq.getStatistics();
    assertNotNull("stats for HARegionQueue found null", stats);
    
    assertEquals(
        "Number of sequence violated by stats not equal to the actual number",
        seqViolated, stats.getNumSequenceViolated());
    assertEquals(
        "Events corresponding to sequence violation not added to the queue but eventsEnqued stats updated for them.",
        rq.size(), stats.getEventsEnqued());
  }
  

}
