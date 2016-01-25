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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test to verify Add operation to HARegion Queue with and without conflation.
 * 
 * @author Yogesh Mahajan
 * @author Dinesh Patel
 */

@Category(IntegrationTest.class)
public class HARQAddOperationJUnitTest
{
  private static final Logger logger = LogService.getLogger();

  /** The cache instance */
  protected Cache cache = null;

  /** Logger for this test */
  protected LogWriter logWriter = null;

  /** The <code>RegionQueue</code> instance */
  private HARegionQueue rq = null;

  protected final static String KEY1 = "Key-1";

  protected final static String KEY2 = "Key-2";

  protected final static String VALUE1 = "Value-1";

  protected final static String VALUE2 = "Value-2";

  protected boolean testFailed = false;

  protected StringBuffer message = null;

  protected int barrierCount = 0;
  
  volatile static int expiryCount = 0;
  
  /**
   * Create the cache in setup
   * 
   * @throws Exception -
   *           thrown if any exception occurs in setUp
   */
  @Before
  public void setUp() throws Exception
  {
    this.cache = createCache();
    this.logWriter = cache.getLogger();
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
    this.cache.close();
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
    return new CacheFactory().set("mcast-port", "0").create();
  }

  /**
   * Creates HA region-queue object
   * 
   * @return HA region-queue object
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws CacheException
   * @throws InterruptedException
   */
  protected HARegionQueue createHARegionQueue(String name)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name,
        cache, HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

  /**
   * Creates HA region-queue object
   * 
   * @return HA region-queue object
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws CacheException
   * @throws InterruptedException
   */
  protected HARegionQueue createHARegionQueue(String name,
      HARegionQueueAttributes attrs) throws IOException, ClassNotFoundException, CacheException, InterruptedException
  {

    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name,
        cache, attrs, HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

  /**
   * Add operation with conflation : 1) Add Key1 Val1 & then Key1 Val2. 2) At
   * the end of second operation , Region should contain the entry correspodning
   * to value 2 3) Available IDs , Last DispatchedWrapper Set & Conflation Map
   * should have size 1. 4) Conflation Map , LastDispatchedWrapper Set &
   * Available IDs should have counter corresponding to second operation
   * 
   */
  @Test
  public void testQueueAddOperationWithConflation() throws Exception
  {
    this.logWriter
        .info("HARegionQueueJUnitTest : testQueueAddOperationWithConflation BEGIN");
    this.rq = createHARegionQueue("testQueueAddOperationWithConflation");
    EventID id1 = new EventID(new byte[] { 1 }, 1, 1);
    EventID id2 = new EventID(new byte[] { 1 }, 1, 2);
    ConflatableObject c1 = new ConflatableObject(KEY1, VALUE1, id1, true,
        "region1");
    ConflatableObject c2 = new ConflatableObject(KEY1, VALUE2, id2, true,
        "region1");
    this.rq.put(c1);
    this.rq.put(c2);
    Map conflationMap = (Map)rq
        .getConflationMapForTesting().get("region1");
    assertEquals(1, conflationMap.size());
    Long cntr = (Long)conflationMap.get(KEY1);
    ConflatableObject retValue = (ConflatableObject)rq.getRegion().get(cntr);
    assertEquals(VALUE2, retValue.getValueToConflate());
    assertEquals(1, rq.getAvalaibleIds().size());

    assertEquals(1, rq.getCurrentCounterSet(id1).size());
    this.logWriter
        .info("HARegionQueueJUnitTest : testQueueAddOperationWithConflation END");
  }

  /**
   * Add operation without conflation : 1) Region should have an entry
   * containing counter vs Conflatable object. 2) Events Map should have size as
   * 1 with one ThreadIdentifer objecta s key & Last DispatchedWrapper as value.
   * 3) This wrapper should have a set with size 1. 4) The available IDs set
   * shoudl have size 1. 5) Put another object by same thread. 6) The wrapper
   * set & availableIs List should have size 2 .
   * 
   */
  @Test
  public void testQueueAddOperationWithoutConflation() throws Exception
  {
    this.logWriter
        .info("HARegionQueueJUnitTest : testQueueAddOperationWithoutConflation BEGIN");
    this.rq = createHARegionQueue("testQueueAddOperationWithConflation");
    EventID id1 = new EventID(new byte[] { 1 }, 1, 1);
    EventID id2 = new EventID(new byte[] { 1 }, 1, 2);
    ConflatableObject c1 = new ConflatableObject(KEY1, VALUE1, id1, false,
        "region1");
    ConflatableObject c2 = new ConflatableObject(KEY2, VALUE2, id2, false,
        "region1");
    this.rq.put(c1);

    assertNull(rq.getConflationMapForTesting().get("region1"));
    assertEquals(1, rq.getAvalaibleIds().size());

    assertEquals(1, rq.getCurrentCounterSet(id1).size());

    this.rq.put(c2);
    assertNull(rq.getConflationMapForTesting().get("region1"));
    assertEquals(2, rq.getAvalaibleIds().size());
    assertEquals(2, rq.getCurrentCounterSet(id1).size());

    Iterator iter = rq.getCurrentCounterSet(id1).iterator();
    if (iter.hasNext()) {
      Long cntr = (Long)iter.next();
      ConflatableObject co = (ConflatableObject)this.rq.getRegion().get(cntr);
      assertEquals(KEY1, co.getKeyToConflate());
      assertEquals(VALUE1, co.getValueToConflate());
    }
    if (iter.hasNext()) {
      Long cntr = (Long)iter.next();
      ConflatableObject co = (ConflatableObject)this.rq.getRegion().get(cntr);
      assertEquals(KEY2, co.getKeyToConflate());
      assertEquals(VALUE2, co.getValueToConflate());
    }
    this.logWriter
        .info("HARegionQueueJUnitTest : testQueueAddOperationWithoutConflation END");
  }

  /**
   * Add operation without conflation followed by a Take operation: RegionSize,
   * available IDs , LastDispatchedWrapper's Set should have size 0. Events map
   * containg should have size 1 ( corresponding to the
   * lastDispatchedAndCurrentEvent Wrapper objcet)
   * 
   * @throws Exception
   */
  @Test
  public void testQueueAddTakeOperationWithoutConflation() throws Exception
  {
    this.logWriter
        .info("HARegionQueueJUnitTest : testQueueAddTakeOperationWithoutConflation BEGIN");

    this.rq = createHARegionQueue("testQueueAddOperationWithConflation");
    EventID id = new EventID(new byte[] { 1 }, 1, 1);
    ConflatableObject obj = new ConflatableObject(KEY1, VALUE1, id, true,
        "region1");
    this.rq.put(obj);
    this.rq.take();
    assertNull(rq.getRegion().get(KEY1));
    assertEquals(0, this.rq.getAvalaibleIds().size());
    Map eventsMap = this.rq.getEventsMapForTesting();
    assertEquals(1, eventsMap.size());
    assertEquals(0, rq.getCurrentCounterSet(id).size());
    this.logWriter
        .info("HARegionQueueJUnitTest : testQueueAddTakeOperationWithoutConflation END");
  }

  /**
   * (Add opn followed by Take without conflation) :Add operation which creates
   * the LastDispatchedAndCurrentEvents object should also add it to the Region
   * with Threaddentifer as key & sequence as the value for Expiry. Perform a
   * take operation. Validate that expiry on ThreadIdentifier removes itself
   * from Events Map
   * 
   * 
   */
  @Test
  public void testExpiryOnThreadIdentifier()
  {
    try {
      HARegionQueueAttributes attrs = new HARegionQueueAttributes();
      attrs.setExpiryTime(2);
      HARegionQueue regionqueue = createHARegionQueue("testing", attrs);
      // create the conflatable object
      EventID id = new EventID(new byte[] { 1 }, 1, 1);
      ConflatableObject obj = new ConflatableObject(KEY1, VALUE1, id, true,
          "region1");

      ThreadIdentifier threadId = new ThreadIdentifier(obj.getEventId()
          .getMembershipID(), obj.getEventId().getThreadID());
      regionqueue.put(obj);
      regionqueue.take();
      Thread.sleep(25000);
      assertFalse(
          "ThreadIdentifier did not remove itself through expiry.The reqgion queue is of type="
              + regionqueue.getClass(), regionqueue.getRegion().containsKey(
              threadId));
      Map eventsMap = regionqueue.getEventsMapForTesting();
      assertNull(
          "expiry action on ThreadIdentifier did not remove itself from eventsMap",
          eventsMap.get(threadId));

    }
    catch (Exception e) {
      fail(" test failed due to " + e);
    }
  }

  /**
   * Add operation which creates the LastDispatchedAndCurrentEvents object
   * should also add it to the Region with Threaddentifer as key & sequence as
   * the value for Expiry. Validate that expiry on ThreadIdentifier does not
   * removes itself from Events Map as data is lying & it resets the self
   * expiry. Validate the data present in Queue experiences expiry. After the
   * expiry of the data , AvaialbleIds size should be 0, entry removed from
   * Region, LastDispatchedWrapperSet should have size 0.
   * 
   * 
   */
  @Test
  public void testNoExpiryOnThreadIdentifier()
  {
    try {
      HARegionQueueAttributes hqa = new HARegionQueueAttributes();
      hqa.setExpiryTime(8);
      HARegionQueue regionqueue = createHARegionQueue("testing", hqa);
      EventID id1 = new EventID(new byte[] { 1 }, 1, 1);
      EventID id2 = new EventID(new byte[] { 1 }, 1, 2);
      ConflatableObject c1 = new ConflatableObject(KEY1, VALUE1, id1, true,
          "region1");
      ConflatableObject c2 = new ConflatableObject(KEY1, VALUE2, id2, true,
          "region1");
      ThreadIdentifier threadId = new ThreadIdentifier(c1.getEventId()
          .getMembershipID(), c1.getEventId().getThreadID());

      regionqueue.put(c1);
      Object o = regionqueue.take();
      assertNotNull(o);
      // wait for some time and put second object
      Thread.sleep(3000);
      regionqueue.put(c2);
      // wait for some more time so that C2 has not expired
      Thread.sleep(4000);

      Map eventsMap = regionqueue.getEventsMapForTesting();

      // verify that ThreadIdentifier does not remove itself as data is
      // lying
      assertNotNull(
          "ThreadIdentifier removed itself through expiry even though data was lying in the queue",
          eventsMap.get(threadId));

      // wait for some more time to allow expiry on data
      Thread.sleep(16000);

      // After the expiry of the data , AvaialbleIds size should be 0,
      // entry
      // removed from Region, LastDispatchedWrapperSet should have size 0.
      assertEquals(0, regionqueue.getRegion().entries(false).size());
      assertEquals(0, regionqueue.getAvalaibleIds().size());
      assertNull(regionqueue.getCurrentCounterSet(id1));

    }
    catch (Exception e) {
      e.printStackTrace();
      fail(" test failed due to " + e);
    }
  }

  /**
   * Multiple arrivals of QRM for the same thread id of the client.Intially
   * queue contains objects from 1- 10. QRM with sequenceID 5 arrives It should
   * remove only remove objects for 1- 5. Then sequenceID 10 come which should
   * remove 5-10.
   * 
   */
  @Test
  public void testMultipleQRMArrival() throws Exception
  {
    HARegionQueue regionqueue = createHARegionQueue("testNoExpiryOnThreadIdentifier");

    EventID[] ids = new EventID[10];
    for (int i = 0; i < 10; i++) {
      ids[i] = new EventID(new byte[] { 1 }, 1, i + 1);
    }
    for (int i = 0; i < 10; i++) {
      regionqueue.put(new ConflatableObject("KEY " + i, "VALUE" + i, ids[i],
          true, "region1"));
    }

    // Available id size should be == 10 after puting ten entries
    assertEquals(10, regionqueue.getAvalaibleIds().size());

    // QRM message for therad id 1 and last sequence id 5
    regionqueue.removeDispatchedEvents(ids[4]);
    assertEquals(5, regionqueue.getAvalaibleIds().size());
    assertEquals(5, regionqueue.getCurrentCounterSet(ids[0]).size());

    Iterator iter = regionqueue.getCurrentCounterSet(ids[0]).iterator();
    while (iter.hasNext()) {
      Long cntr = (Long)iter.next();
      ConflatableObject co = (ConflatableObject)regionqueue.getRegion().get(
          cntr);
      assertTrue(co.getEventId().getSequenceID() > 5);
    }

    regionqueue.removeDispatchedEvents(ids[9]);
    assertEquals(0, regionqueue.getAvalaibleIds().size());

  }

  /**
   * Concurrent arrival of put & QRM messagge for a ThreadIdentifier coming for
   * 1st time. The LastDispatchedObject gets operated first by the put thread ,
   * the QRM thread should be blocked till the completion of add operation. Thus
   * before QRM thread acts , the object should be present in the
   * lastDispatchedSet & AvailableID. Then the QRM thread gets unblocked , it
   * should remove from the available ID.
   * 
   */
  @Test
  public void testConcurrentPutAndQRM() throws Exception
  {
    testFailed = false;
    message = new StringBuffer();
    final HARegionQueue regionqueue = createHARegionQueue("testConcurrentPutAndQRM");
    final EventID id1 = new EventID(new byte[] { 1 }, 1, 1);
    final EventID id2 = new EventID(new byte[] { 1 }, 1, 2);
    Thread t1 = new Thread() {
      public void run()
      {
        try {
          regionqueue.put(new ConflatableObject(KEY1, VALUE1, id1, true,
              "region1"));
          regionqueue.put(new ConflatableObject(KEY2, VALUE2, id2, true,
              "region1"));
        }
        catch (Exception e) {
          message.append("Put in region queue failed");
          testFailed = true;
        }
      }
    };
    t1.setPriority(Thread.MAX_PRIORITY);

    Thread t2 = new Thread() {
      public void run()
      {
        try {
          regionqueue.removeDispatchedEvents(id2);
        }
        catch (Exception e) {
          message.append("Removal by QRM in region queue failed");
          testFailed = true;
        }
      }
    };

    t1.start();
    t2.start();

    DistributedTestCase.join(t1, 180 * 1000, null);
    DistributedTestCase.join(t2, 180 * 1000, null);

    if (testFailed)
      fail("Test failed due to " + message);

    assertEquals(0, regionqueue.getAvalaibleIds().size());
    assertEquals(2, regionqueue.getLastDispatchedSequenceId(id2));
  }

  /**
   * Concurrent arrival of put & QRM messagge for a ThreadIdentifier coming for
   * 1st time. The LastDispatchedObject gets operated first by the QRM thread ,
   * the Put thread should be blocked till the completion of QRM operation. Thus
   * put thread should see that last Sequence is > than the current & hence the
   * put operation shud remove from region without adding the ID anywhere.
   */
  @Test
  public void testConcurrentQRMAndPut() throws Exception
  {
    testFailed = false;
    final HARegionQueue regionqueue = createHARegionQueue("testConcurrentQRMAndPut");
    final EventID id1 = new EventID(new byte[] { 1 }, 1, 1);
    final EventID id2 = new EventID(new byte[] { 1 }, 1, 2);

    Thread t1 = new Thread() {
      public void run()
      {
        try {
          regionqueue.put(new ConflatableObject(KEY1, VALUE1, id1, true,
              "region1"));
          regionqueue.put(new ConflatableObject(KEY2, VALUE2, id2, true,
              "region1"));
        }
        catch (Exception e) {
          message.append("Put in region queue failed");
          testFailed = true;
        }
      }
    };

    Thread t2 = new Thread() {
      public void run()
      {
        try {
          regionqueue.removeDispatchedEvents(id2);
        }
        catch (Exception e) {
          message.append("Removal of Events by QRM in Region queue failed");
          testFailed = true;
        }
      }
    };
    t2.setPriority(Thread.MAX_PRIORITY);

    t2.start();
    t1.start();

    DistributedTestCase.join(t1, 180 * 1000, null);
    DistributedTestCase.join(t2, 180 * 1000, null);

    if (testFailed)
      fail("Test failed due to " + message);

    assertEquals(0, regionqueue.getAvalaibleIds().size());
    assertEquals(2, regionqueue.getLastDispatchedSequenceId(id2));

  }

  /**
   * Two QRMs arriving such that higer sequence number arriving before lower
   * sequence number. The lower squnce number should not set itself & also not
   * do any checking on the IDs of the LinkedHashSet
   * 
   * @throws Exception
   */

  @Test
  public void testEventMapPopulationForQRM() throws Exception
  {
    HARegionQueue regionqueue = createHARegionQueue("testEventMapPopulationForQRM");
    EventID id1 = new EventID(new byte[] { 1 }, 1, 1);
    EventID id2 = new EventID(new byte[] { 1 }, 1, 2);

    this.logWriter.info("RemoveDispatched event for sequence id : "
        + id2.getSequenceID());
    regionqueue.removeDispatchedEvents(id2);
    this.logWriter.info("RemoveDispatched event for sequence id :"
        + id1.getSequenceID());
    regionqueue.removeDispatchedEvents(id1);
    assertEquals("Size of eventMap should be 1 but actual size "
        + regionqueue.getEventsMapForTesting(), regionqueue
        .getEventsMapForTesting().size(), 1);
    this.logWriter.info("sequence id : "
        + regionqueue.getLastDispatchedSequenceId(id2));
    assertEquals(
        "Last dispatched sequence id should be 2 but actual sequence id is ",
        regionqueue.getLastDispatchedSequenceId(id2), id2.getSequenceID());
    this.logWriter.info("testEventMapPopulationForQRM() completed successfully");

  }

  /**
   * Concurrent arrival of put operations on different threadIDs but for same
   * key . One should conflate the other. Whosoever confaltes , that ID should
   * be present in the availableIDs list , Region , ConflationMap & its HashSet
   * for that ThreadIdentifier. The ID which gets conflated should not be
   * present in the availableID, Region & that ThreadIdentifier's HashSet . The
   * conflation map should contain the Old IDs position.
   * 
   * @throws Exception
   */

  @Test
  public void testCleanUpForConflation() throws Exception
  {
    this.logWriter
        .info("HARQAddOperationJUnitTest : testCleanUpForConflation BEGIN");
    testFailed = false;
    message = null;
    final int numOfThreads = 10;
    final int numOfPuts = 567;
    final HARegionQueue regionqueue = createHARegionQueue("testCleanUpForConflation");

    this.logWriter
        .info("HARQAddOperationJUnitTest : testCleanUpForConflation after regionqueue create");
    /*
     * doing concurrent put operations on different threadIDs but for the same
     * key
     */
    Thread[] threads = new Thread[10];
    for (int i = 0; i < numOfThreads; i++) {
      final long ids = i;
      threads[i] = new Thread() {
        public void run()
        {
          for (int j = 0; j < numOfPuts; j++) {
            EventID id = new EventID(new byte[] { (byte)ids }, ids, j);
            try {
              regionqueue.put(new ConflatableObject(KEY1, id.getThreadID()
                  + "VALUE" + j, id, true, "region1"));
            }
            catch (Exception ex) {
              testFailed = true;
              message.append("put failed for the threadId " + id.getThreadID());
            }

          }

        }
      };
    }

    for (int k = 0; k < numOfThreads; k++) {
      threads[k].start();
    }

    for (int k = 0; k < numOfThreads; k++) {
      DistributedTestCase.join(threads[k], 180 * 1000, null);
    }

    this.logWriter
        .info("HARQAddOperationJUnitTest : testCleanUpForConflation after join");

    if (testFailed)
      fail("Test failed due to " + message);

    assertEquals("size of the conflation map should be 1 but actual size is "
        + regionqueue.getConflationMapForTesting().size(), 1, regionqueue
        .getConflationMapForTesting().size());
    assertEquals("size of the event map should be " + numOfThreads
        + " but actual size " + regionqueue.getEventsMapForTesting().size(),
        numOfThreads, regionqueue.getEventsMapForTesting().size());
    assertEquals("size of availableids should 1 but actual size "
        + regionqueue.getAvalaibleIds().size(), 1, regionqueue
        .getAvalaibleIds().size());
    int count = 0;
    for (int i = 0; i < numOfThreads; i++) {
      if ((regionqueue.getCurrentCounterSet(new EventID(new byte[] { (byte)i },
          i, i))).size() > 0) {
        count++;
      }
    }

    assertEquals("size of the counter set is  1 but the actual size is "
        + count, 1, count);

    Long position = null;
    if (regionqueue.getAvalaibleIds().size() == 1) {
      position = (Long)regionqueue.getAvalaibleIds().iterator().next();
    }
    ConflatableObject id = (ConflatableObject)regionqueue.getRegion().get(
        position);
    assertEquals(regionqueue.getCurrentCounterSet(id.getEventId()).size(), 1);
    this.logWriter
        .info("HARQAddOperationJUnitTest : testCleanUpForConflation END");
  }

  /**
   * Test where a 5 separate threads do 4 puts each. A thread then peeks the 20
   * objects & then invokes remove. The remove should ensure that the entries
   * are deleted from the available IDs & the Counters set contained in DACE.
   * Conflation is disabled.
   */

  @Test
  public void testPeekAndRemoveWithoutConflation() throws Exception
  {
    testFailed = false;
    message = null;
    final int numOfThreads = 5;
    final int numOfPuts = 4;
    final int batchSize = 20;
    final HARegionQueue regionqueue = createHARegionQueue("testPeekAndRemoveWithoutConflation");
    Thread[] threads = new Thread[numOfThreads];
    for (int i = 0; i < numOfThreads; i++) {
      final long ids = i;
      threads[i] = new Thread() {
        public void run()
        {
          for (int j = 0; j < numOfPuts; j++) {
            EventID id = new EventID(new byte[] { (byte)ids }, ids, j);
            try {
              regionqueue.put(new ConflatableObject(
                  KEY1 + id.getThreadID() + j, id.getThreadID() + "VALUE" + j,
                  id, false, "region1"));
            }
            catch (Exception ex) {
              testFailed = true;
              message.append("put failed for the threadId " + id.getThreadID());
            }
          }
        }
      };
    }

    for (int k = 0; k < numOfThreads; k++) {
      threads[k].start();
    }

    for (int k = 0; k < numOfThreads; k++) {
      DistributedTestCase.join(threads[k], 180 * 1000, null);
    }

    if (testFailed)
      fail("Test failed due to " + message);

    List pickObjects = regionqueue.peek(batchSize);
    assertEquals(batchSize, pickObjects.size());
    regionqueue.remove();

    for (int i = 0; i < numOfThreads; i++) {
      assertEquals(3, regionqueue.getLastDispatchedSequenceId(new EventID(
          new byte[] { (byte)i }, i, 1)));
      assertEquals(0, regionqueue.getCurrentCounterSet(
          new EventID(new byte[] { (byte)i }, i, 1)).size());
    }

    assertEquals(0, regionqueue.getAvalaibleIds().size());

    this.logWriter
        .info("testPeekAndRemoveWithoutConflation() completed successfully");

  }

  /**
   * Test where a 5 separate threads do 4 puts each. A thread then peeks the 20
   * objects & then invokes remove. The remove should ensure that the entries
   * are deleted from the available IDs & the Counters set contained in DACE.
   * Conflation is enabled
   */

  @Test
  public void testPeekAndRemoveWithConflation() throws Exception
  {
    testFailed = false;
    message = null;
    final int numOfThreads = 5;

    final int numOfPuts = 4;
    final int batchSize = numOfThreads * numOfPuts;
    final HARegionQueue regionqueue = createHARegionQueue("testPeekAndRemoveWithConflation");
    Thread[] threads = new Thread[numOfThreads];
    for (int i = 0; i < numOfThreads; i++) {
      final long ids = i;
      threads[i] = new Thread() {
        public void run()
        {
          for (int j = 0; j < numOfPuts; j++) {
            EventID id = new EventID(new byte[] { (byte)ids }, ids, j);
            try {
              regionqueue.put(new ConflatableObject(KEY1 + ids, id
                  .getThreadID()
                  + "VALUE" + j, id, true, "region1"));
            }
            catch (Exception ex) {
              testFailed = true;
              message.append("put failed for the threadId " + id.getThreadID());
            }
          }
        }
      };
    }

    for (int k = 0; k < numOfThreads; k++) {
      threads[k].start();
    }

    for (int k = 0; k < numOfThreads; k++) {
      DistributedTestCase.join(threads[k], 180 * 1000, null);
    }

    if (testFailed)
      fail("Test failed due to " + message);

    List pickObject = regionqueue.peek(batchSize);
    assertEquals(numOfThreads, pickObject.size());
    regionqueue.remove();

    for (int i = 0; i < numOfThreads; i++) {
      // assertEquals(numOfPuts,
      // regionqueue.getLastDispatchedSequenceId(new EventID(
      // new byte[] { (byte)i }, i, 1)));
      assertEquals(0, regionqueue.getCurrentCounterSet(
          new EventID(new byte[] { (byte)i }, i, 1)).size());
    }

    assertEquals("size of availableIds map should be 0 ", 0, regionqueue
        .getAvalaibleIds().size());
    assertEquals("size of conflation map should be 0 ", 0, ((Map)regionqueue
        .getConflationMapForTesting().get("region1")).size());

    this.logWriter
        .info("testPeekAndRemoveWithConflation() completed successfully");

  }

  /**
   * Test where a 5 separate threads do 4 puts each. 4 threads then concurrently
   * do a peek of batch size 5, 10 , 15 & 20 respectively. And all of them
   * concurrently cal remove. The remove should ensure that the entries are
   * deleted from the available IDs & the Counters set contained in DACE.
   * 
   * @throws Exception
   */

  @Test
  public void testPeekForDiffBatchSizeAndRemoveAll() throws Exception
  {
    testFailed = false;
    message = null;
    barrierCount = 0;
    final int numOfThreads = 5;
    final int numOfPuts = 4;
    // final CountDownLatch mylatch = new CountDownLatch(4);
    final HARegionQueue regionqueue = createHARegionQueue("testPeekForDiffBatchSizeAndRemoveAll");
    Thread[] threads = new Thread[numOfThreads];
    for (int i = 0; i < numOfThreads; i++) {
      final long ids = i;
      threads[i] = new Thread() {
        public void run()
        {
          for (int j = 0; j < numOfPuts; j++) {
            EventID id = new EventID(new byte[] { (byte)ids }, ids, j);
            try {
              regionqueue.put(new ConflatableObject(
                  KEY1 + id.getThreadID() + j, id.getThreadID() + "VALUE" + j,
                  id, false, "region1"));
            }
            catch (Exception ex) {
              testFailed = true;
              message.append("put failed for the threadId " + id.getThreadID());
            }
          }
        }
      };
    }

    for (int k = 0; k < numOfThreads; k++) {
      threads[k].start();
    }

    for (int k = 0; k < numOfThreads; k++) {
      DistributedTestCase.join(threads[k], 180 * 1000, null);
    }

    if (testFailed)
      fail("Test failed due to " + message);

    testFailed = false;
    message = null;

    Thread[] threads_peek_remove = new Thread[numOfPuts];
    for (int i = 1; i < (numOfPuts + 1); i++) {
      final int peakBatchSize = i * 5;
      threads_peek_remove[i - 1] = new Thread() {

        public void run()
        {
          try {
            List peakObjects = regionqueue.peek(peakBatchSize);
            assertEquals(peakBatchSize, peakObjects.size());
            synchronized (HARQAddOperationJUnitTest.this) {
              ++barrierCount;
              if (barrierCount == 4) {
                HARQAddOperationJUnitTest.this.notifyAll();
              }
              else {
                HARQAddOperationJUnitTest.this.wait();
              }
            }
            regionqueue.remove();

          }
          catch (Exception ex) {
            testFailed = true;
            ex.printStackTrace();
            message.append("Exception while performing peak operation "
                + ex.getStackTrace());

          }

        }

      };
    }

    for (int k = 0; k < numOfPuts; k++) {
      threads_peek_remove[k].start();
    }

    for (int k = 0; k < numOfPuts; k++) {
      DistributedTestCase.join(threads_peek_remove[k], 180 * 1000, null);
    }

    if (testFailed)
      fail("Test failed due to " + message);

    for (int i = 0; i < numOfThreads; i++) {
      assertEquals(3, regionqueue.getLastDispatchedSequenceId(new EventID(
          new byte[] { (byte)i }, i, 1)));
      assertEquals(0, regionqueue.getCurrentCounterSet(
          new EventID(new byte[] { (byte)i }, i, 1)).size());
    }

    assertEquals(0, regionqueue.getAvalaibleIds().size());

    this.logWriter
        .info("testPeekForDiffBatchSizeAndRemoveAll() completed successfully");
  }

  /**
   * Test where a 5 separate threads do 4 puts each. 3 threads then concurrently
   * do a peek of batch size 5, 10 and 15 respectively. And all of them
   * concurrently call remove. The remove should ensure that the entries are
   * deleted from the available IDs & the Counters set contained in DACE.
   * 
   * @throws Exception
   */
  @Test
  public void testPeekForDiffBatchSizeAndRemoveSome() throws Exception
  {
    testFailed = false;
    barrierCount = 0;
    message = null;
    final int numOfThreads = 5;
    final int numOfPuts = 4;
    final HARegionQueue regionqueue = createHARegionQueue("testPeekForDiffBatchSizeAndRemoveSome");
    Thread[] threads = new Thread[numOfThreads];
    for (int i = 0; i < numOfThreads; i++) {
      final long ids = i;
      threads[i] = new Thread() {
        public void run()
        {
          for (int j = 0; j < numOfPuts; j++) {
            EventID id = new EventID(new byte[] { (byte)ids }, ids, j);
            try {
              regionqueue.put(new ConflatableObject(
                  KEY1 + id.getThreadID() + j, id.getThreadID() + "VALUE" + j,
                  id, false, "region1"));
            }
            catch (Exception ex) {
              testFailed = true;
              message.append("put failed for the threadId " + id.getThreadID());
            }
          }
        }
      };
    }

    for (int k = 0; k < numOfThreads; k++) {
      threads[k].start();
    }

    for (int k = 0; k < numOfThreads; k++) {
      DistributedTestCase.join(threads[k], 180 * 1000, null);
    }

    if (testFailed)
      fail("Test failed due to " + message);

    testFailed = false;
    message = null;
    Thread[] threads_peek_remove = new Thread[numOfPuts - 1];
    for (int i = 1; i < numOfPuts; i++) {
      final int peakBatchSize = i * 5;
      threads_peek_remove[i - 1] = new Thread() {

        public void run()
        {
          try {
            List peakObjects = regionqueue.peek(peakBatchSize);
            assertEquals(peakBatchSize, peakObjects.size());
            synchronized (HARQAddOperationJUnitTest.this) {
              ++barrierCount;
              if (barrierCount == 3) {
                HARQAddOperationJUnitTest.this.notifyAll();
              }
              else {
                HARQAddOperationJUnitTest.this.wait();
              }
            }
            regionqueue.remove();

          }
          catch (Exception ex) {
            testFailed = true;
            ex.printStackTrace();
            message.append("Exception while performing peak operation "
                + ex.getStackTrace());

          }

        }

      };
    }

    for (int k = 0; k < numOfPuts - 1; k++) {
      threads_peek_remove[k].start();
    }

    for (int k = 0; k < numOfPuts - 1; k++) {
      DistributedTestCase.join(threads_peek_remove[k], 180 * 1000, null);
    }

    if (testFailed)
      fail("Test failed due to " + message);

    assertEquals(5, regionqueue.getAvalaibleIds().size());

    this.logWriter
        .info("testPeekForDiffBatchSizeAndRemoveSome() completed successfully");

  }

  /**
   * Add with QRM & expiry : Add 10 conflatable objects (0-9). Send QRM
   * LastDispatched as 4. Validate the sequence ID field of
   * LastDispatchedWrapper object is updated to 4. Perform Take operations to
   * remove next 5 to 9. The seqeunceId field should be 9. Allow
   * ThreadIdenitifier to expire. The expiration should fail as the original
   * sequenceId ( -1) does not match with 9. Validate reset with value 9 . The
   * next expiry should remove the LastDisptachedWrapper
   */
  @Test
  public void testAddWithQRMAndExpiry() throws Exception
  {
	try{  
      HARegionQueueAttributes attrs = new HARegionQueueAttributes();
      attrs.setExpiryTime(10);
      final HARegionQueue regionqueue = new HARegionQueue.TestOnlyHARegionQueue("testing", cache, attrs) {
        CacheListener createCacheListenerForHARegion()
        {
          return new CacheListenerAdapter() {
            public void afterInvalidate(EntryEvent event) {
              try {
                expireTheEventOrThreadIdentifier(event);
              }
              catch (CacheException ce) {
                logger.error("HAREgionQueue::createCacheListener::Exception in the expiry thread", ce);
              }
              if (event.getKey() instanceof ThreadIdentifier) {
                synchronized (HARQAddOperationJUnitTest.this) {
                  expiryCount++;		  
                  HARQAddOperationJUnitTest.this.notify();
                }
              }
            }
          };
        }
      };
      Conflatable[] cf = new Conflatable[10];
      // put 10 conflatable objects
      for (int i = 0; i < 10; i++) {
        cf[i] = new ConflatableObject("key" + i, "value", new EventID(
            new byte[] { 1 }, 1, i), true, "testing");
        regionqueue.put(cf[i]);
      }
      ThreadIdentifier tID = new ThreadIdentifier(new byte[] { 1 }, 1);
      // verify that the sequence-id for Thread-identifier is -1 (default value).
      assertEquals(new Long(-1), regionqueue.getRegion().get(tID));

      // remove the first 5 - (0-4 sequence IDs)
      regionqueue.removeDispatchedEvents(new EventID(new byte[] { 1 }, 1, 4));

      // verify that the last dispatched event was of sequence id 4
      assertEquals(4, regionqueue.getLastDispatchedSequenceId(new EventID(new byte[] { 1 }, 1, 1)));
      // verify 1-5 not in region
      for (long i = 1; i < 6; i++) {
        Assert.assertTrue(!regionqueue.getRegion().containsKey(new Long(i)));
      }
      // verify 6-10 still in region queue
      for (long i = 6; i < 11; i++) {
        Assert.assertTrue(regionqueue.getRegion().containsKey(new Long(i)));
      }

      // Perform 5 take operations to remove next 5-9 sequence ids
      for (long i = 6; i < 11; i++) {
        regionqueue.take();
      }

      // verify that the last dispatched event was of sequence id 10
      assertEquals(9, regionqueue.getLastDispatchedSequenceId(new EventID(new byte[] { 1 }, 1, 1)));
      // verify that sequence ids 1-10 all are removed from the RQ
      for (long i = 1; i < 11; i++) {
        Assert.assertTrue(!regionqueue.getRegion().containsKey(new Long(i)));
      }

      // wait until expiry thread has run once
      synchronized(HARQAddOperationJUnitTest.this) {
        if (0 == expiryCount) {	     
	     HARQAddOperationJUnitTest.this.wait();
        }
        if(1 == expiryCount) {
          // verify that the Thread-identifier has not yet expired          
          assertEquals(1,regionqueue.getEventsMapForTesting().size());

          // verify that the sequence-id for Thread-identifier is updated to 9
          assertEquals(new Long(9), regionqueue.getRegion().get(tID));
          
          // wait until expiry thread has run again
          HARQAddOperationJUnitTest.this.wait();
        }
      }
    
      // verify that the Thread-identifier has expired
      assertEquals(0,regionqueue.getEventsMapForTesting().size());
      // verify that the sequence-id for Thread-identifier is null
      assertNull(regionqueue.getRegion().get(tID));
    }
    catch (Exception e) {
      fail("Exception occured in test due to " + e);
    }
  }

  /**
   * This test does the following:<br>
   * 1)Create a blocking HARegionQueue<br>
   * 2)Add some events to the queue with same ThreadIdentifier<br>
   * 3)Do take() operations to drain the queue<br>
   * 4)Verify that dispatchedMessagesMap is not null<br>
   * 5)Verify that size of the dispatchedMessagesMap is 1 as one regionqueue is
   * created in this test<br>
   * 6)Verify that the map contains an entry for the queue-region name<br>
   * 7)Verify that the size of wrapper-map is 1 as all events had same ThreadId<br>
   * 8)Verify that the sequenceId against the ThreadId in the wrapper-map is
   * same as that of the last event taken<br>
   * 
   * @throws Exception -
   *           thrown if any exception occurs in test execution
   */
  
  /**
   * Behaviour of take() has been changed for relaible messaging feature. Region queue take()
   * operation will no longer add to the Dispatch Message Map. Hence disabling the test - SUYOG
  */
 
  public void _testDispatchedMsgsMapUpdateOnTakes() throws Exception
  {
    this.logWriter
        .info("HARQAddOperationJUnitTest : testDispatchedEventsMapUpdateOnTakes BEGIN");

    String regionName = "testDispatchedEventsMapUpdateOnTakes";
    HARegionQueue rq = createHARegionQueue(regionName);

    Conflatable cf = null;
    EventID id = null;

    int totalEvents = 10;
    for (int i = 0; i < totalEvents; i++) {
      id = new EventID(new byte[] { 1 }, 1, i);
      cf = new ConflatableObject("key" + i, "value" + i, id, false, "testing");
      rq.put(cf);
    }

    for (int i = 0; i < totalEvents; i++) {
      rq.take();
    }

    Map dispatchedMsgMap = HARegionQueue.getDispatchedMessagesMapForTesting();
    // verify that map is not null
    assertNotNull("dispatchedMessagesMap found null", dispatchedMsgMap);

    // size of the dispatchedMessagesMap should be 1 as one regionqueue is
    // created in this test
    assertEquals("size of dispatched msgs should be 1", 1, dispatchedMsgMap
        .size());

    // verify that the map contains an entry for the queue-region name
    MapWrapper wrapper = (MapWrapper)dispatchedMsgMap.get(regionName);
    assertNotNull(
        "dispatchedMsgMap should contain an entry with queueregion name as key",
        wrapper);

    Map dispatchedData = wrapper.map;
    assertEquals(
        "size of wrapper-map should be 1 as all events had same ThreadId", 1,
        dispatchedData.size());

    ThreadIdentifier tid = new ThreadIdentifier(new byte[] { 1 }, 1);
    Long seqId = (Long)dispatchedData.get(tid);

    assertEquals(
        "sequenceId against the ThreadId in the wrapper-map should be that of the last event taken.",
        id.getSequenceID(), seqId.longValue());

    this.logWriter
        .info("HARQAddOperationJUnitTest : testDispatchedEventsMapUpdateOnTakes END");
  }
}
