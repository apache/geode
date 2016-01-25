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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import util.TestException;

import junit.framework.Assert;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 *
 * @author Mitul Bid
 * @author Asif
 *
 *
 */
public class HARegionQueueDUnitTest extends DistributedTestCase
{
  VM vm0 = null;

  VM vm1 = null;

  VM vm3 = null;

  VM vm2 = null;

  protected static Cache cache = null;

  protected static HARegionQueue hrq = null;

//  private static int counter = 0;

  protected static volatile boolean toCnt = true;

  protected static Thread opThreads[];
  
  protected static volatile Thread createQueuesThread;

  /** constructor */
  public HARegionQueueDUnitTest(String name) {
    super(name);
  }

  /**
   * get the VM's
   */
  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
  }

  /**
   * close the cache in tearDown
   */
  public void tearDown2() throws Exception
  {
    super.tearDown2();
    vm0.invoke(HARegionQueueDUnitTest.class, "closeCache");
    vm1.invoke(HARegionQueueDUnitTest.class, "closeCache");
    vm2.invoke(HARegionQueueDUnitTest.class, "closeCache");
    vm3.invoke(HARegionQueueDUnitTest.class, "closeCache");
  }

  /**
   * create cache
   *
   * @return
   * @throws Exception
   */
  protected Cache createCache() throws CacheException
  {
    Properties props = new Properties();
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    Cache cache = null;
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new CacheException("CacheFactory.create() returned null ") {
      };
    }
    return cache;
  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 and VM2 2) do a put in VM1 3)
   * assert that the put has not propagated from VM1 to VM2 4) do a put in VM2
   * 5) assert that the value in VM1 has not changed to due to put in VM2 6)
   * assert put in VM2 was successful by doing a get
   *
   */
  public void testLocalPut()
  {
    vm0.invoke(HARegionQueueDUnitTest.class, "createRegion");
    vm1.invoke(HARegionQueueDUnitTest.class, "createRegion");
    vm0.invoke(HARegionQueueDUnitTest.class, "putValue1");
    vm1.invoke(HARegionQueueDUnitTest.class, "getNull");
    vm1.invoke(HARegionQueueDUnitTest.class, "putValue2");
    vm0.invoke(HARegionQueueDUnitTest.class, "getValue1");
    vm1.invoke(HARegionQueueDUnitTest.class, "getValue2");

  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 and VM2 2) do a put in VM1 3)
   * assert that the put has not propagated from VM1 to VM2 4) do a put in VM2
   * 5) assert that the value in VM1 has not changed to due to put in VM2 6)
   * assert respective puts the VMs were successful by doing a get 7)
   * localDestroy key in VM1 8) assert key has been destroyed in VM1 9) assert
   * key has not been destroyed in VM2
   *
   */
  public void testLocalDestroy()
  {
    vm0.invoke(HARegionQueueDUnitTest.class, "createRegion");
    vm1.invoke(HARegionQueueDUnitTest.class, "createRegion");
    vm0.invoke(HARegionQueueDUnitTest.class, "putValue1");
    vm1.invoke(HARegionQueueDUnitTest.class, "getNull");
    vm1.invoke(HARegionQueueDUnitTest.class, "putValue2");
    vm0.invoke(HARegionQueueDUnitTest.class, "getValue1");
    vm1.invoke(HARegionQueueDUnitTest.class, "getValue2");
    vm0.invoke(HARegionQueueDUnitTest.class, "destroy");
    vm0.invoke(HARegionQueueDUnitTest.class, "getNull");
    vm1.invoke(HARegionQueueDUnitTest.class, "getValue2");
  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 2) do a put in VM1 3) get teh
   * value in VM1 to assert put has happened successfully 4) Create mirrored
   * HARegion region1 in VM2 5) do a get in VM2 to verify that value was got
   * through GII 6) do a put in VM2 7) assert put in VM2 was successful
   *
   */
  public void testGII()
  {
    vm0.invoke(HARegionQueueDUnitTest.class, "createRegion");
    vm0.invoke(HARegionQueueDUnitTest.class, "putValue1");
    vm0.invoke(HARegionQueueDUnitTest.class, "getValue1");
    vm1.invoke(HARegionQueueDUnitTest.class, "createRegion");
    vm1.invoke(HARegionQueueDUnitTest.class, "getValue1");
    vm1.invoke(HARegionQueueDUnitTest.class, "putValue2");
    vm1.invoke(HARegionQueueDUnitTest.class, "getValue2");

  }

  /**
   * Tests the relevant data structures are updated after GII happens.
   *
   * In this test, a HARegion is created in vm0. 10 conflatable objects are put
   * in vm0's region HARegion is then created in vm1. After region creation, the
   * verification whether the relevant data structuers have been updated is
   * done.
   *
   */
 /* public void testGIIAndMapUpdates()
  {
    vm0.invoke(HARegionQueueDUnitTest.class, "createRegionQueue2");
    vm0.invoke(HARegionQueueDUnitTest.class, "putConflatables");
    vm1.invoke(HARegionQueueDUnitTest.class, "createRegionQueue2");
    vm0.invoke(HARegionQueueDUnitTest.class, "clearRegion");
    vm1.invoke(HARegionQueueDUnitTest.class, "verifyMapsAndData");

  } */

  /**
   * 1) Create mirrored HARegion region1 in VM1 2) do a put in VM1 3) get teh
   * value in VM1 to assert put has happened successfully 4) Create mirrored
   * HARegion region1 in VM2 5) do a get in VM2 to verify that value was got
   * through GII 6) do a put in VM2 7) assert put in VM2 was successful
   *
   */
  public void testQRM()
  {
    vm0.invoke(HARegionQueueDUnitTest.class, "createRegionQueue");
    vm1.invoke(HARegionQueueDUnitTest.class, "createRegionQueue");
    vm0.invoke(HARegionQueueDUnitTest.class, "verifyAddingDispatchMesgs");

    vm1.invoke(HARegionQueueDUnitTest.class, "verifyDispatchedMessagesRemoved");
  }

  /**
   * 1)Create regionqueue on VM0 and VM1 2) put same conflated object from VM1
   * aand VM2 3)perform take() operation from VM0 4) Wait for the QRM to
   * execute. 4)check the size of the regionqueue in VM1. It should be zero
   * because QRM should remove entry from the regionqueue of VM1
   * 
   * 
   */
  
  /**
   * Behaviour of take() has been changed for relaible messaging feature. Region queue take()
   * operation will no longer add to the Dispatch Message Map. Hence disabling the test - SUYOG
  */
    
  public void _testBugNo35988() throws Exception
  {
    
    CacheSerializableRunnable createQueue = new CacheSerializableRunnable(
        "CreateCache, HARegionQueue and start thread") {
      public void run2() throws CacheException
      {
        HARegionQueueDUnitTest test = new HARegionQueueDUnitTest("region1");
        //TODO:ASIF: Bcoz of the QRM thread cannot take frequency below
        // 1 second , thus we need to carfully evaluate what to do. Though
        //in this case 1 second instead of 500 ms will work
       // System.getProperties().put("QueueRemovalThreadWaitTime", new Long(500));
        cache = test.createCache();
        cache.setMessageSyncInterval(1);
        HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
        hrqa.setExpiryTime(300);
        try {
          hrq = HARegionQueue.getHARegionQueueInstance("testregion1", cache,
              hrqa, HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
          // Do 1000 putand 100 take in a separate thread
          hrq.put(new ConflatableObject(new Long(1), new Long(1), new EventID(
              new byte[] { 0 }, 1, 1), false, "dummy"));
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
    vm0.invoke(createQueue);
    vm1.invoke(createQueue);

    vm0.invoke(new CacheSerializableRunnable("takeFromVm0") {
      public void run2() throws CacheException {
        try {
          Conflatable obj = (Conflatable)hrq.take();
          assertNotNull(obj);
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    });



    vm1.invoke(new CacheSerializableRunnable("checkInVm1") {
      public void run2() throws CacheException
      {
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            Thread.yield(); // TODO is this necessary?
            return hrq.size() == 0;
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
      }
    });

  }

  /**
   * create a client with 2 regions sharing a common writer
   *
   * @throws Exception
   */

  public static void createRegion() throws Exception
  {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest(
        "HARegionQueueDUnitTest_region");
    cache = test.createCache();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    HARegion.getInstance("HARegionQueueDUnitTest_region", (GemFireCacheImpl)cache,
        null, factory.create());
  }

  /**
   *
   *
   * @throws Exception
   */

  public static void createRegionQueue() throws Exception
  {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest(
        "HARegionQueueDUnitTest_region");
    cache = test.createCache();
    /*
     * AttributesFactory factory = new AttributesFactory();
     * factory.setScope(Scope.DISTRIBUTED_ACK);
     * factory.setDataPolicy(DataPolicy.REPLICATE);
     */
    hrq = HARegionQueue.getHARegionQueueInstance(
        "HARegionQueueDUnitTest_region", cache,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    EventID id1 = new EventID(new byte[] { 1 }, 1, 1);
    EventID id2 = new EventID(new byte[] { 1 }, 1, 2);
    ConflatableObject c1 = new ConflatableObject("1", "1", id1, false,
        "HARegionQueueDUnitTest_region");
    ConflatableObject c2 = new ConflatableObject("2", "2", id2, false,
        "HARegionQueueDUnitTest_region");
    hrq.put(c1);
    hrq.put(c2);

  }

  public static void createRegionQueue2() throws Exception
  {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest(
        "HARegionQueueDUnitTest_region");
    cache = test.createCache();
    /*
     * AttributesFactory factory = new AttributesFactory();
     * factory.setScope(Scope.DISTRIBUTED_ACK);
     * factory.setDataPolicy(DataPolicy.REPLICATE);
     */
    HARegionQueueAttributes harqAttr = new HARegionQueueAttributes();
    harqAttr.setExpiryTime(3);
    hrq = HARegionQueue.getHARegionQueueInstance(
        "HARegionQueueDUnitTest_region", cache, harqAttr,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
  }

  public static void clearRegion()
  {
    try {
      Iterator iterator = hrq.getRegion().keys().iterator();
      while (iterator.hasNext()) {
        hrq.getRegion().localDestroy(iterator.next());
      }
    }
    catch (Exception e) {
      fail("Exception occured while trying to destroy region");
    }

  }

  public static void verifyAddingDispatchMesgs()
  {
    Assert.assertTrue(HARegionQueue.getDispatchedMessagesMapForTesting()
        .isEmpty());
    hrq.addDispatchedMessage(new ThreadIdentifier(new byte[1], 1), 1);
    Assert.assertTrue(!HARegionQueue.getDispatchedMessagesMapForTesting()
        .isEmpty());
  }

  public static void verifyDispatchedMessagesRemoved()
  {
    try {
      final Region region = hrq.getRegion();
      // wait until we have a dead
      // server
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          Thread.yield(); // TODO is this necessary?
          return region.get(new Long(0)) == null;
        }
        public String description() {
          return null;
        }
      };
      DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);

      /*
       * if (region.get(new Long(0)) != null) { fail("Expected message to have
       * been deleted but it is not deleted"); }
       */

      if (region.get(new Long(1)) == null) {
        fail("Expected message not to have been deleted but it is deleted");
      }

    }
    catch (Exception e) {
      fail("test failed due to an exception :  " + e);
    }
  }

  /**
   * close the cache
   * 
   */
  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * do puts on key-1
   *
   */
  public static void putValue1()
  {
    try {
      Region r1 = cache.getRegion("/HARegionQueueDUnitTest_region");
      r1.put("key-1", "value-1");
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("failed while region.put()", ex);
    }
  }

  public static void putConflatables()
  {
    try {
      Region r1 = hrq.getRegion();
      for (int i = 1; i < 11; i++) {
        r1.put(new Long(i), new ConflatableObject("key" + i, "value" + i,
            new EventID(new byte[] { 1 }, 1, i), true,
            "HARegionQueueDUnitTest_region"));
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("failed while region.put()", ex);
    }
  }

  /**
   * verifies the data has been populated correctly after GII
   *
   */
  public static void verifyMapsAndData()
  {
    try {
      HARegion r1 = (HARegion)hrq.getRegion();
      // region should not be null
      Assert.assertNotNull(" Did not expect the HARegion to be null but it is",
          r1);
      // it should have ten non null entries
      for (int i = 1; i < 11; i++) {
        Assert.assertNotNull(" Did not expect the entry to be null but it is",
            r1.get(new Long(i)));
      }
      // HARegionQueue should not be null
      Assert.assertNotNull(
          " Did not expect the HARegionQueue to be null but it is", hrq);

      Map conflationMap = hrq.getConflationMapForTesting();
      // conflationMap size should be greater than 0
      Assert.assertTrue(
          " Did not expect the conflationMap size to be 0 but it is",
          conflationMap.size() > 0);
      Map internalMap = (Map)conflationMap.get("HARegionQueueDUnitTest_region");
      // internal map should not be null. it should be present
      Assert.assertNotNull(
          " Did not expect the internalMap to be null but it is", internalMap);
      // get and verify the entries in the conflation map.
      for (int i = 1; i < 11; i++) {
        Assert.assertTrue(
            " Did not expect the entry not to be equal but it is", internalMap
                .get("key" + i).equals(new Long(i)));
      }
      Map eventMap = hrq.getEventsMapForTesting();
      // DACE should not be null
      Assert.assertNotNull(
          " Did not expect the result (DACE object) to be null but it is",
          eventMap.get(new ThreadIdentifier(new byte[] { 1 }, 1)));
      Set counterSet = hrq.getCurrentCounterSet(new EventID(new byte[] { 1 },
          1, 1));
      Assert.assertTrue(
          " excpected the counter set size to be 10 but it is not so",
          counterSet.size() == 10);
      long i = 1;
      Iterator iterator = counterSet.iterator();
      // verify the order of the iteration. it should be 1 - 10. The underlying
      // set is a LinkedHashSet
      while (iterator.hasNext()) {
        Assert.assertTrue(((Long)iterator.next()).longValue() == i);
        i++;
      }
      // The last dispactchde sequence Id should be -1 since no dispatch has
      // been made
      Assert.assertTrue(hrq.getLastDispatchedSequenceId(new EventID(
          new byte[] { 1 }, 1, 1)) == -1);

      // sleep for 8.0 seconds. Everythign should expire and everything should
      // be null and empty
      Thread.sleep(7500);

      for (int j = 1; j < 11; j++) {
        Assert
            .assertNull(
                "expected the entry to be null since expiry time exceeded but it is not so",
                r1.get(new Long(j)));
      }

      internalMap = (Map)hrq.getConflationMapForTesting().get(
          "HARegionQueueDUnitTest_region");

      Assert.assertNotNull(
          " Did not expect the internalMap to be null but it is", internalMap);
      Assert
          .assertTrue(
              "internalMap (conflation) should have been emptry since expiry of all entries has been exceeded but it is not so",
              internalMap.isEmpty());
      Assert
          .assertTrue(
              "eventMap should have been emptry since expiry of all entries has been exceeded but it is not so",
              eventMap.isEmpty());
      Assert
          .assertTrue(
              "counter set should have been emptry since expiry of all entries has been exceeded but it is not so",
              counterSet.isEmpty());

    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("failed while region.put()", ex);
    }
  }

  /**
   * do puts on key-1,value-2
   *
   */
  public static void putValue2()
  {
    try {
      Region r1 = cache.getRegion("/HARegionQueueDUnitTest_region");
      r1.put("key-1", "value-2");
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("failed while region.put()", ex);
    }
  }

  /**
   * do a get on region1
   *
   */
  public static void getValue1()
  {
    try {
      Region r = cache.getRegion("/HARegionQueueDUnitTest_region");
      if (!(r.get("key-1").equals("value-1"))) {
        fail("expected value to be value-1 but it is not so");
      }

    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("failed while region.get()", ex);
    }
  }

  /**
   * do a get on region1
   *
   */
  public static void getNull()
  {
    try {
      Region r = cache.getRegion("/HARegionQueueDUnitTest_region");
      if (!(r.get("key-1") == (null))) {
        fail("expected value to be null but it is not so");
      }

    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("failed while region.get()", ex);
    }
  }

  /**
   * do a get on region1
   *
   */
  public static void getValue2()
  {
    try {
      Region r = cache.getRegion("/HARegionQueueDUnitTest_region");
      if (!(r.get("key-1").equals("value-2"))) {
        fail("expected value to be value-2 but it is not so");
      }

    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("failed while region.get()", ex);
    }
  }

  /**
   * destroy key-1
   *
   */
  public static void destroy()
  {
    try {
      Region region1 = cache.getRegion("/HARegionQueueDUnitTest_region");
      region1.localDestroy("key-1");
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("test failed due to exception in destroy ");
    }
  }

  /**
   * Tests the Non Blocking HARegionQueue by doing concurrent put /remove / take /
   * peek , batch peek operations in multiple regions. The test will have
   * take/remove occuring in all the VMs. This test is targetted to test for
   * hang or exceptions in non blocking queue.
   *
   * @author Asif
   *
   */
  public void testConcurrentOperationsDunitTestOnNonBlockingQueue()
  {
    concurrentOperationsDunitTest(false, Scope.DISTRIBUTED_ACK);
  }

  /**
   * Tests the Non Blocking HARegionQueue by doing concurrent put /remove / take /
   * peek , batch peek operations in multiple regions. The test will have
   * take/remove occuring in all the VMs. This test is targetted to test for
   * hang or exceptions in non blocking queue.
   *
   * @author Asif
   *
   */
  public void testConcurrentOperationsDunitTestOnNonBlockingQueueWithDNoAckRegion()
  {
    concurrentOperationsDunitTest(false, Scope.DISTRIBUTED_NO_ACK);
  }

  /**
   * Tests the Blokcing HARegionQueue by doing concurrent put /remove / take /
   * peek , batch peek operations in multiple regions. The test will have
   * take/remove occuring in all the VMs. This test is targetted to test for
   * hang or exceptions in blocking queue.
   *
   * @author Asif
   *
   */
  public void testConcurrentOperationsDunitTestOnBlockingQueue()
  {
    concurrentOperationsDunitTest(true, Scope.DISTRIBUTED_ACK);
  }

  private void concurrentOperationsDunitTest(
      final boolean createBlockingQueue, final Scope rscope)
  {
    // Create Cache and HARegionQueue in all the 4 VMs.

    CacheSerializableRunnable createRgnsAndQueues = new CacheSerializableRunnable(
        "CreateCache, mirrored Region & HARegionQueue with a CacheListener") {
      public void run2() throws CacheException
      {
        HARegionQueueDUnitTest test = new HARegionQueueDUnitTest(
            "HARegionQueueDUnitTest_region");
        System.getProperties()
            .put("QueueRemovalThreadWaitTime", "2000");
        cache = test.createCache();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(rscope);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
        hrqa.setExpiryTime(5);
        try {
          if (createBlockingQueue) {
            hrq = HARegionQueue.getHARegionQueueInstance("testregion1", cache,
                hrqa, HARegionQueue.BLOCKING_HA_QUEUE, false);
          }
          else {
            hrq = HARegionQueue.getHARegionQueueInstance("testregion1", cache,
                hrqa, HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
          }
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
        factory.addCacheListener(new CacheListenerAdapter() {
          public void afterCreate(final EntryEvent event)
          {
            Conflatable conflatable = new ConflatableObject(event.getKey(),
                event.getNewValue(), ((EntryEventImpl)event).getEventId(),
                false, event.getRegion().getFullPath());

            try {
              hrq.put(conflatable);
            }
            catch (Exception e) {
              e.printStackTrace();
              fail("The put operation in queue did not succeed due to exception ="
                  + e);
            }
          }

          public void afterUpdate(final EntryEvent event)
          {
            Conflatable conflatable = new ConflatableObject(event.getKey(),
                event.getNewValue(), ((EntryEventImpl)event).getEventId(),
                true, event.getRegion().getFullPath());

            try {
              hrq.put(conflatable);
            }
            catch (Exception e) {
              e.printStackTrace();
              fail("The put operation in queue did not succeed due to exception ="
                  + e);
            }
          }

        });

        cache.createRegion("test_region", factory.create());

      }
    };

    vm0.invoke(createRgnsAndQueues);
    vm1.invoke(createRgnsAndQueues);
    vm2.invoke(createRgnsAndQueues);
    vm3.invoke(createRgnsAndQueues);
    CacheSerializableRunnable spawnThreadsAndperformOps = new CacheSerializableRunnable(
        "Spawn multipe threads which do various operations") {

      public void run2() throws CacheException
      {
        opThreads = new Thread[4 + 2 + 2 + 2];
        for (int i = 0; i < 4; ++i) {
          opThreads[i] = new Thread(new RunOp(RunOp.PUT, i), "ID="
              + i + ",Op=" + RunOp.PUT);
        }
        for (int i = 4; i < 6; ++i) {
          opThreads[i] = new Thread(new RunOp(RunOp.PEEK, i), "ID="
              + i + ",Op=" + RunOp.PEEK);
        }

        for (int i = 6; i < 8; ++i) {
          opThreads[i] = new Thread(new RunOp(RunOp.TAKE, i), "ID="
              + i + ",Op=" + RunOp.TAKE);
        }

        for (int i = 8; i < 10; ++i) {
          opThreads[i] = new Thread(new RunOp(RunOp.TAKE, i), "ID="
              + i + ",Op=" + RunOp.BATCH_PEEK);
        }

        for (int i = 0; i < opThreads.length; ++i) {
          opThreads[i].start();
        }

      }

    };

    vm0.invokeAsync(spawnThreadsAndperformOps);
    vm1.invokeAsync(spawnThreadsAndperformOps);
    vm2.invokeAsync(spawnThreadsAndperformOps);
    vm3.invokeAsync(spawnThreadsAndperformOps);
    try {
      Thread.sleep(2000);
    }
    catch (InterruptedException e1) {
      fail("Test failed as the test thread encoutered exception in sleep");
    }
    // Asif : In case of blocking HARegionQueue do some extra puts so that the
    // blocking threads
    // are exited
    CacheSerializableRunnable toggleFlag = new CacheSerializableRunnable(
        "Toggle the flag to signal end of threads") {
      public void run2() throws CacheException {
        toCnt = false;
        if (createBlockingQueue) {
          try {
            for (int i = 0; i < 100; ++i) {
              hrq.put(new ConflatableObject("1", "1", new EventID(
                  new byte[] { 1 }, 100, i), false, "/x"));
            }
          }
          catch (Exception e) {
            throw new CacheException(e) {
            };
          }
        }

      }
    };

    vm0.invokeAsync(toggleFlag);
    vm1.invokeAsync(toggleFlag);
    vm2.invokeAsync(toggleFlag);
    vm3.invokeAsync(toggleFlag);
//     try {
//       Thread.sleep(5000);
//     }
//     catch (InterruptedException e2) {
//       fail("Test failed as the test thread encoutered exception in sleep");
//     }
    CacheSerializableRunnable joinWithThreads = new CacheSerializableRunnable(
        "Join with the threads") {
      public void run2() throws CacheException
      {
        for (int i = 0; i < opThreads.length; ++i) {

          if (opThreads[i].isInterrupted()) {
            fail("Test failed because  thread encountered exception");
          }
          DistributedTestCase.join(opThreads[i], 30 * 1000, getLogWriter());
        }
      }
    };

    vm0.invoke(joinWithThreads);
    vm1.invoke(joinWithThreads);
    vm2.invoke(joinWithThreads);
    vm3.invoke(joinWithThreads);
    System.getProperties().remove("QueueRemovalThreadWaitTime");
  }

  /**
   * This is to test the bug which is caused when HARegionQueue object hasnot
   * been fully constructed but as the HARegion has got constructed , it gets
   * visible to QRM Message Thread.
   *
   * @author Asif
   *
   */
  public void testNPEDueToHARegionQueueEscapeInConstructor()
  {
    // changing EXPIRY_TIME to 5 doesn't change how long the test runs!
    final int EXPIRY_TIME = 30; // test will run for this many seconds
    // Create two HARegionQueue 's in the two VMs. The frequency of QRM thread
    // should be high
    // Check for NullPointeException in the other VM.
    CacheSerializableRunnable createQueuesAndThread = new CacheSerializableRunnable(
        "CreateCache, HARegionQueue and start thread") {
      public void run2() throws CacheException
      {
        HARegionQueueDUnitTest test = new HARegionQueueDUnitTest("region1");
        //TODO:ASIF: Bcoz of the QRM thread cannot take frequency below
        // 1 second , thus we need to carfully evaluate what to do. 
        //For this bug to appear ,without bugfix , qrm needs to run
        //very fast.
        //System.getProperties().put("QueueRemovalThreadWaitTime", new Long(10));
        cache = test.createCache();
        cache.setMessageSyncInterval(1);
        HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
        hrqa.setExpiryTime(EXPIRY_TIME);
        try {
          hrq = HARegionQueue.getHARegionQueueInstance(
              "testNPEDueToHARegionQueueEscapeInConstructor", cache, hrqa,
              HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
          // changing OP_COUNT to 20 makes no difference in test time
          final int OP_COUNT = 200;
          // Do 1000 putand 100 take in a separate thread
          for (int i = 0; i < OP_COUNT; ++i) {
            hrq.put(new ConflatableObject(new Long(i), new Long(i),
                new EventID(new byte[] { 0 }, 1, i), false, "dummy"));
          }
          opThreads = new Thread[1];
          opThreads[0] = new Thread(new Runnable() {
            public void run()
            {
              for (int i = 0; i < OP_COUNT; ++i) {
                try {
                  Object o = hrq.take();
                  if (o == null) {
                    Thread.sleep(50);
                  }
                }
                catch (InterruptedException e) {
                  fail("interrupted");
                }
              }
            }
          });
          opThreads[0].start();

        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };

    CacheSerializableRunnable createQueues = new CacheSerializableRunnable(
        "CreateCache, HARegionQueue ") {
      public void run2() throws CacheException
      {
        createQueuesThread = Thread.currentThread();
        HARegionQueueDUnitTest test = new HARegionQueueDUnitTest("region1");
        //System.getProperties().put("QueueRemovalThreadWaitTime",
         //   new Long(120000));
        cache = test.createCache();
        cache.setMessageSyncInterval(EXPIRY_TIME);
        HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
        hrqa.setExpiryTime(EXPIRY_TIME);
        try {
          hrq = HARegionQueue.getHARegionQueueInstance(
              "testNPEDueToHARegionQueueEscapeInConstructor", cache, hrqa,
              HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
    
    CacheSerializableRunnable waitForCreateQueuesThread = new CacheSerializableRunnable(
        "joinCreateCache") {
      public void run2() throws TestException {
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return createQueuesThread != null;
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, 30 * 1000, 200, true);
        DistributedTestCase.join(createQueuesThread, 300 * 1000, getLogWriter());
      }
    };

    vm0.invoke(createQueuesAndThread);
    vm1.invokeAsync(createQueues);

    CacheSerializableRunnable joinWithThread = new CacheSerializableRunnable(
        "CreateCache, HARegionQueue join with thread") {
      public void run2() throws CacheException
      {
        if (opThreads[0].isInterrupted()) {
          fail("The test has failed as it encountered interrupts in puts & takes");
        }
        DistributedTestCase.join(opThreads[0], 30 * 1000, getLogWriter());
      }
    };
    vm0.invoke(joinWithThread);
    vm1.invoke(waitForCreateQueuesThread);
  }

  class RunOp implements Runnable
  {

    int opType;

    int threadID;

    public static final int PUT = 1;

    public static final int TAKE = 2;

    public static final int PEEK = 3;

    public static final int BATCH_PEEK = 4;

    public RunOp(int opType, int id) {
      this.opType = opType;
      this.threadID = id;
    }

    public void run()
    {
      Region rgn = cache.getRegion("test_region");
      int counter = 0;
      LogWriter logger = cache.getLogger();
      Conflatable cnf;
      try {
        while (toCnt) {
          Thread.sleep(20);
          // Thread.currentThread().getName() + " before doing operation of
          // type= "+ this.opType);
          switch (opType) {
          case PUT:
            rgn.put("key" + threadID, "val" + counter++);
            if (counter == 10)
              counter = 0;
            break;
          case TAKE:
            cnf = (Conflatable)hrq.take();
            if (logger.fineEnabled() && cnf != null) {
              logger.fine("Object retrieved  by take has key ="
                  + cnf.getKeyToConflate() + " and value as"
                  + cnf.getValueToConflate());
            }
            break;
          case PEEK:
            cnf = (Conflatable)hrq.peek();
            if (logger.fineEnabled() && cnf != null) {
              logger.fine("Object retrieved  by peek has key ="
                  + cnf.getKeyToConflate() + " and value as"
                  + cnf.getValueToConflate());
            }
            // Thread.currentThread().getName() + " before doing remove= "+
            // this.opType);
            hrq.remove();
            break;
          case BATCH_PEEK:
            List confList = hrq.peek(3, 2000);
            if (logger.fineEnabled() && confList != null) {
              logger.fine("Object retrieved  by  batch peek are =" + confList);
            }
            // Thread.currentThread().getName() + " before doing remove= "+
            // this.opType);
            hrq.remove();
            break;

          }
          // Thread.currentThread().getName() + " after Operation of type= "+
          // this.opType);

        }
      }
      catch (Exception e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * This is to test the bug which is caused when HARegionQueue object hasnot
   * been fully constructed but as the HARegion has got constructed , it gets
   * visible to expiry thread task causing NullPointerException in some
   * situations.
   *
   */
 /* public void testBugNo35989()
  {
    vm0.invoke(HARegionQueueDUnitTest.class, "createRegionQueue");
    vm1.invoke(HARegionQueueDUnitTest.class,
        "createHARegionQueueandCheckExpiration");

  } */

  /**
   * Checks the data received by GII, only gets expired after proper
   * construction of HARegionQueue object.
   *
   * @throws Exception
   */
  public static void createHARegionQueueandCheckExpiration() throws Exception
  {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest(
        "HARegionQueueDUnitTest_region");
    cache = test.createCache();
    HARegionQueueAttributes attrs = new HARegionQueueAttributes();
    attrs.setExpiryTime(1);
    hrq = HARegionQueue.getHARegionQueueInstance(
        "HARegionQueueDUnitTest_region", cache, attrs,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    // wait until we have a dead
    // server
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return hrq.getAvalaibleIds().size() == 0;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
    // assertEquals(0, hrq.getAvalaibleIds().size());
  }

  public void testForDuplicateEvents()
  {
    vm0.invoke(HARegionQueueDUnitTest.class, "createRegionQueue");
    vm1.invoke(HARegionQueueDUnitTest.class, "createRegionQueueandCheckDuplicates");
  }

  /**
   *  HARegionQueue should not allow data with duplicate EventIds.
   *
   * @throws Exception
   */
  public static void createRegionQueueandCheckDuplicates() throws Exception
  {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest(
        "HARegionQueueDUnitTest_region");
    cache = test.createCache();

    hrq = HARegionQueue.getHARegionQueueInstance("HARegionQueueDUnitTest_region", cache,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false);

    assertEquals(2, hrq.size());

    EventID id1 = new EventID(new byte[] { 1 }, 1, 1);
    EventID id2 = new EventID(new byte[] { 1 }, 1, 2);
    ConflatableObject c1 = new ConflatableObject("1", "1", id1, false,
        "HARegionQueueDUnitTest_region");
    ConflatableObject c2 = new ConflatableObject("2", "2", id2, false,
        "HARegionQueueDUnitTest_region");

    hrq.put(c1);
    hrq.put(c2);

    //HARegion size should be 2 as data with same EventIDs is inserted into the queue
    assertEquals(2, hrq.size());
  }
}
