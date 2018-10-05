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

import static java.lang.Thread.yield;
import static org.apache.geode.internal.cache.ha.HARegionQueue.NON_BLOCKING_HA_QUEUE;
import static org.apache.geode.internal.cache.ha.HARegionQueue.getHARegionQueueInstance;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.ThreadUtils.join;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.AdditionalAnswers;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class HARegionQueueDUnitTest extends JUnit4DistributedTestCase {

  private static volatile boolean toCnt = true;
  private static volatile Thread createQueuesThread;

  private static InternalCache cache = null;
  private static HARegionQueue hrq = null;
  private static Thread[] opThreads;

  private VM vm0 = null;
  private VM vm1 = null;
  private VM vm3 = null;
  private VM vm2 = null;

  /**
   * get the VM's
   */
  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    vm0.invoke(() -> HARegionQueueDUnitTest.toCnt = true);
    vm1.invoke(() -> HARegionQueueDUnitTest.toCnt = true);
    vm2.invoke(() -> HARegionQueueDUnitTest.toCnt = true);
    vm3.invoke(() -> HARegionQueueDUnitTest.toCnt = true);
  }

  /**
   * close the cache in tearDown
   */
  @Override
  public final void preTearDown() throws Exception {
    vm0.invoke(() -> HARegionQueueDUnitTest.closeCache());
    vm1.invoke(() -> HARegionQueueDUnitTest.closeCache());
    vm2.invoke(() -> HARegionQueueDUnitTest.closeCache());
    vm3.invoke(() -> HARegionQueueDUnitTest.closeCache());

    cache = null;
    hrq = null;
    opThreads = null;
  }

  /**
   * create cache
   */
  private InternalCache createCache() throws CacheException {
    Properties props = new Properties();
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    InternalCache cache = null;
    cache = (InternalCache) CacheFactory.create(ds);
    if (cache == null) {
      // TODO: never throw an anonymous inner class
      throw new CacheException("CacheFactory.create() returned null ") {};
    }
    return cache;
  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 and VM2 2) do a put in VM1 3) assert that the put
   * has not propagated from VM1 to VM2 4) do a put in VM2 5) assert that the value in VM1 has not
   * changed to due to put in VM2 6) assert put in VM2 was successful by doing a get
   */
  @Test
  public void testLocalPut() throws Exception {
    vm0.invoke(() -> HARegionQueueDUnitTest.createRegion());
    vm1.invoke(() -> HARegionQueueDUnitTest.createRegion());
    vm0.invoke(() -> HARegionQueueDUnitTest.putValue1());
    vm1.invoke(() -> HARegionQueueDUnitTest.getNull());
    vm1.invoke(() -> HARegionQueueDUnitTest.putValue2());
    vm0.invoke(() -> HARegionQueueDUnitTest.getValue1());
    vm1.invoke(() -> HARegionQueueDUnitTest.getValue2());
  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 and VM2 2) do a put in VM1 3) assert that the put
   * has not propagated from VM1 to VM2 4) do a put in VM2 5) assert that the value in VM1 has not
   * changed to due to put in VM2 6) assert respective puts the VMs were successful by doing a get
   * 7) localDestroy key in VM1 8) assert key has been destroyed in VM1 9) assert key has not been
   * destroyed in VM2
   */
  @Test
  public void testLocalDestroy() throws Exception {
    vm0.invoke(() -> HARegionQueueDUnitTest.createRegion());
    vm1.invoke(() -> HARegionQueueDUnitTest.createRegion());
    vm0.invoke(() -> HARegionQueueDUnitTest.putValue1());
    vm1.invoke(() -> HARegionQueueDUnitTest.getNull());
    vm1.invoke(() -> HARegionQueueDUnitTest.putValue2());
    vm0.invoke(() -> HARegionQueueDUnitTest.getValue1());
    vm1.invoke(() -> HARegionQueueDUnitTest.getValue2());
    vm0.invoke(() -> HARegionQueueDUnitTest.destroy());
    vm0.invoke(() -> HARegionQueueDUnitTest.getNull());
    vm1.invoke(() -> HARegionQueueDUnitTest.getValue2());
  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 2) do a put in VM1 3) get the value in VM1 to assert
   * put has happened successfully 4) Create mirrored HARegion region1 in VM2 5) do a get in VM2 to
   * verify that value was got through GII 6) do a put in VM2 7) assert put in VM2 was successful
   */
  @Test
  public void testGII() throws Exception {
    vm0.invoke(() -> HARegionQueueDUnitTest.createRegion());
    vm0.invoke(() -> HARegionQueueDUnitTest.putValue1());
    vm0.invoke(() -> HARegionQueueDUnitTest.getValue1());
    vm1.invoke(() -> HARegionQueueDUnitTest.createRegion());
    vm1.invoke(() -> HARegionQueueDUnitTest.getValue1());
    vm1.invoke(() -> HARegionQueueDUnitTest.putValue2());
    vm1.invoke(() -> HARegionQueueDUnitTest.getValue2());
  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 2) do a put in VM1 3) get the value in VM1 to assert
   * put has happened successfully 4) Create mirrored HARegion region1 in VM2 5) do a get in VM2 to
   * verify that value was got through GII 6) do a put in VM2 7) assert put in VM2 was successful
   */
  @Test
  public void testQRM() throws Exception {
    vm0.invoke(() -> HARegionQueueDUnitTest.createRegionQueue());
    vm1.invoke(() -> HARegionQueueDUnitTest.createRegionQueue());
    vm0.invoke(() -> HARegionQueueDUnitTest.verifyAddingDispatchMesgs());

    vm1.invoke(() -> HARegionQueueDUnitTest.verifyDispatchedMessagesRemoved());
  }

  /**
   * Behaviour of take() has been changed for reliable messaging feature. Region queue take()
   * operation will no longer add to the Dispatch Message Map. Hence disabling the test - SUYOG
   *
   * Test for #35988 HARegionQueue.take() is not functioning as expected
   */
  @Ignore("TODO: this test was disabled")
  @Test
  public void testBugNo35988() throws Exception {
    CacheSerializableRunnable createQueue =
        new CacheSerializableRunnable("CreateCache, HARegionQueue and start thread") {
          @Override
          public void run2() throws CacheException {
            HARegionQueueDUnitTest test = new HARegionQueueDUnitTest();
            // TODO:ASIF: Bcoz of the QRM thread cannot take frequency below
            // 1 second , thus we need to carfully evaluate what to do. Though
            // in this case 1 second instead of 500 ms will work
            // System.getProperties().put("QueueRemovalThreadWaitTime", new Long(500));
            cache = test.createCache();
            cache.setMessageSyncInterval(1);
            HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
            hrqa.setExpiryTime(300);
            try {
              hrq = HARegionQueue.getHARegionQueueInstance("testregion1", cache, hrqa,
                  HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
              // Do 1000 putand 100 take in a separate thread
              hrq.put(new ConflatableObject(new Long(1), new Long(1),
                  new EventID(new byte[] {0}, 1, 1), false, "dummy"));
            } catch (Exception e) {
              throw new AssertionError(e);
            }
          }
        };
    vm0.invoke(createQueue);
    vm1.invoke(createQueue);

    vm0.invoke(new CacheSerializableRunnable("takeFromVm0") {
      @Override
      public void run2() throws CacheException {
        try {
          Conflatable obj = (Conflatable) hrq.take();
          assertNotNull(obj);
        } catch (Exception e) {
          throw new AssertionError(e);
        }
      }
    });

    vm1.invoke(new CacheSerializableRunnable("checkInVm1") {
      @Override
      public void run2() throws CacheException {
        WaitCriterion ev = new WaitCriterion() {
          @Override
          public boolean done() {
            yield(); // TODO is this necessary?
            return hrq.size() == 0;
          }

          @Override
          public String description() {
            return null;
          }
        };
        GeodeAwaitility.await().untilAsserted(ev);
      }
    });

  }

  /**
   * create a client with 2 regions sharing a common writer
   */
  private static void createRegion() throws Exception {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest();
    cache = test.createCache();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    // Mock the HARegionQueue and answer the input CachedDeserializable when updateHAEventWrapper is
    // called
    HARegionQueue harq = mock(HARegionQueue.class);
    when(harq.updateHAEventWrapper(any(), any(), any()))
        .thenAnswer(AdditionalAnswers.returnsSecondArg());

    HARegion.getInstance("HARegionQueueDUnitTest_region", (GemFireCacheImpl) cache, harq,
        factory.create());
  }

  private static void createRegionQueue() throws Exception {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest();
    cache = test.createCache();
    /*
     * AttributesFactory factory = new AttributesFactory(); factory.setScope(Scope.DISTRIBUTED_ACK);
     * factory.setDataPolicy(DataPolicy.REPLICATE);
     */
    hrq = HARegionQueue.getHARegionQueueInstance("HARegionQueueDUnitTest_region", cache,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    EventID id1 = new EventID(new byte[] {1}, 1, 1);
    EventID id2 = new EventID(new byte[] {1}, 1, 2);
    ConflatableObject c1 =
        new ConflatableObject("1", "1", id1, false, "HARegionQueueDUnitTest_region");
    ConflatableObject c2 =
        new ConflatableObject("2", "2", id2, false, "HARegionQueueDUnitTest_region");
    hrq.put(c1);
    hrq.put(c2);
  }

  private static void createRegionQueue2() throws Exception {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest();
    cache = test.createCache();
    /*
     * AttributesFactory factory = new AttributesFactory(); factory.setScope(Scope.DISTRIBUTED_ACK);
     * factory.setDataPolicy(DataPolicy.REPLICATE);
     */
    HARegionQueueAttributes harqAttr = new HARegionQueueAttributes();
    harqAttr.setExpiryTime(3);
    hrq = HARegionQueue.getHARegionQueueInstance("HARegionQueueDUnitTest_region", cache, harqAttr,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
  }

  private static void clearRegion() {
    try {
      Iterator iterator = hrq.getRegion().keys().iterator();
      while (iterator.hasNext()) {
        hrq.getRegion().localDestroy(iterator.next());
      }
    } catch (Exception e) {
      fail("Exception occurred while trying to destroy region", e);
    }

  }

  private static void verifyAddingDispatchMesgs() {
    assertTrue(HARegionQueue.getDispatchedMessagesMapForTesting().isEmpty());
    hrq.addDispatchedMessage(new ThreadIdentifier(new byte[1], 1), 1);
    assertTrue(!HARegionQueue.getDispatchedMessagesMapForTesting().isEmpty());
  }

  private static void verifyDispatchedMessagesRemoved() {
    try {
      final Region region = hrq.getRegion();
      // wait until we have a dead server
      WaitCriterion ev = new WaitCriterion() {
        @Override
        public boolean done() {
          yield(); // TODO is this necessary?
          return region.get(new Long(0)) == null;
        }

        @Override
        public String description() {
          return null;
        }
      };
      GeodeAwaitility.await().untilAsserted(ev);

      /*
       * if (region.get(new Long(0)) != null) { fail("Expected message to have been deleted but it
       * is not deleted"); }
       */

      if (region.get(new Long(1)) == null) {
        fail("Expected message not to have been deleted but it is deleted");
      }

    } catch (Exception e) {
      fail("test failed due to an exception", e);
    }
  }

  /**
   * close the cache
   */
  private static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * do puts on key-1
   */
  private static void putValue1() {
    try {
      Region r1 = cache.getRegion("/HARegionQueueDUnitTest_region");
      r1.put("key-1", "value-1");
    } catch (Exception ex) {
      fail("failed while region.put()", ex);
    }
  }

  private static void putConflatables() {
    try {
      Region r1 = hrq.getRegion();
      for (int i = 1; i < 11; i++) {
        r1.put(new Long(i), new ConflatableObject("key" + i, "value" + i,
            new EventID(new byte[] {1}, 1, i), true, "HARegionQueueDUnitTest_region"));
      }
    } catch (Exception ex) {
      fail("failed while region.put()", ex);
    }
  }

  /**
   * verifies the data has been populated correctly after GII
   */
  private static void verifyMapsAndData() {
    try {
      HARegion r1 = (HARegion) hrq.getRegion();
      // region should not be null
      assertNotNull(" Did not expect the HARegion to be null but it is", r1);
      // it should have ten non null entries
      for (int i = 1; i < 11; i++) {
        assertNotNull(" Did not expect the entry to be null but it is", r1.get(new Long(i)));
      }
      // HARegionQueue should not be null
      assertNotNull(" Did not expect the HARegionQueue to be null but it is", hrq);

      Map conflationMap = hrq.getConflationMapForTesting();
      // conflationMap size should be greater than 0
      assertTrue(" Did not expect the conflationMap size to be 0 but it is",
          conflationMap.size() > 0);
      Map internalMap = (Map) conflationMap.get("HARegionQueueDUnitTest_region");
      // internal map should not be null. it should be present
      assertNotNull(" Did not expect the internalMap to be null but it is", internalMap);
      // get and verify the entries in the conflation map.
      for (int i = 1; i < 11; i++) {
        assertTrue(" Did not expect the entry not to be equal but it is",
            internalMap.get("key" + i).equals(new Long(i)));
      }
      Map eventMap = hrq.getEventsMapForTesting();
      // DACE should not be null
      assertNotNull(" Did not expect the result (DACE object) to be null but it is",
          eventMap.get(new ThreadIdentifier(new byte[] {1}, 1)));
      Set counterSet = hrq.getCurrentCounterSet(new EventID(new byte[] {1}, 1, 1));
      assertTrue(" excpected the counter set size to be 10 but it is not so",
          counterSet.size() == 10);
      long i = 1;
      Iterator iterator = counterSet.iterator();
      // verify the order of the iteration. it should be 1 - 10. The underlying
      // set is a LinkedHashSet
      while (iterator.hasNext()) {
        assertTrue(((Long) iterator.next()).longValue() == i);
        i++;
      }
      // The last dispactchde sequence Id should be -1 since no dispatch has
      // been made
      assertTrue(hrq.getLastDispatchedSequenceId(new EventID(new byte[] {1}, 1, 1)) == -1);

      // sleep for 8.0 seconds. Everythign should expire and everything should
      // be null and empty
      Thread.sleep(7500);

      for (int j = 1; j < 11; j++) {
        assertNull("expected the entry to be null since expiry time exceeded but it is not so",
            r1.get(new Long(j)));
      }

      internalMap = (Map) hrq.getConflationMapForTesting().get("HARegionQueueDUnitTest_region");

      assertNotNull(" Did not expect the internalMap to be null but it is", internalMap);
      assertTrue(
          "internalMap (conflation) should have been emptry since expiry of all entries has been exceeded but it is not so",
          internalMap.isEmpty());
      assertTrue(
          "eventMap should have been emptry since expiry of all entries has been exceeded but it is not so",
          eventMap.isEmpty());
      assertTrue(
          "counter set should have been emptry since expiry of all entries has been exceeded but it is not so",
          counterSet.isEmpty());

    } catch (Exception ex) {
      fail("failed while region.put()", ex);
    }
  }

  /**
   * do puts on key-1,value-2
   */
  private static void putValue2() {
    try {
      Region r1 = cache.getRegion("/HARegionQueueDUnitTest_region");
      r1.put("key-1", "value-2");
    } catch (Exception ex) {
      fail("failed while region.put()", ex);
    }
  }

  /**
   * do a get on region1
   */
  private static void getValue1() {
    try {
      Region r = cache.getRegion("/HARegionQueueDUnitTest_region");
      if (!(r.get("key-1").equals("value-1"))) {
        fail("expected value to be value-1 but it is not so");
      }

    } catch (Exception ex) {
      fail("failed while region.get()", ex);
    }
  }

  /**
   * do a get on region1
   */
  private static void getNull() {
    try {
      Region r = cache.getRegion("/HARegionQueueDUnitTest_region");
      if (!(r.get("key-1") == (null))) {
        fail("expected value to be null but it is not so");
      }

    } catch (Exception ex) {
      fail("failed while region.get()", ex);
    }
  }

  /**
   * do a get on region1
   */
  public static void getValue2() {
    try {
      Region r = cache.getRegion("/HARegionQueueDUnitTest_region");
      if (!(r.get("key-1").equals("value-2"))) {
        fail("expected value to be value-2 but it is not so");
      }

    } catch (Exception ex) {
      fail("failed while region.get()", ex);
    }
  }

  /**
   * destroy key-1
   */
  public static void destroy() {
    try {
      Region region1 = cache.getRegion("/HARegionQueueDUnitTest_region");
      region1.localDestroy("key-1");
    } catch (Exception e) {
      fail("test failed due to exception in destroy", e);
    }
  }

  /**
   * Tests the Non Blocking HARegionQueue by doing concurrent put /remove / take / peek , batch peek
   * operations in multiple regions. The test will have take/remove occuring in all the VMs. This
   * test is targetted to test for hang or exceptions in non blocking queue.
   */
  @Test
  public void testConcurrentOperationsDunitTestOnNonBlockingQueue() throws Exception {
    concurrentOperationsDunitTest(false, Scope.DISTRIBUTED_ACK);
  }

  /**
   * Tests the Non Blocking HARegionQueue by doing concurrent put /remove / take / peek , batch peek
   * operations in multiple regions. The test will have take/remove occuring in all the VMs. This
   * test is targetted to test for hang or exceptions in non blocking queue.
   */
  @Test
  public void testConcurrentOperationsDunitTestOnNonBlockingQueueWithDNoAckRegion()
      throws Exception {
    concurrentOperationsDunitTest(false, Scope.DISTRIBUTED_NO_ACK);
  }

  /**
   * Tests the Blokcing HARegionQueue by doing concurrent put /remove / take / peek , batch peek
   * operations in multiple regions. The test will have take/remove occuring in all the VMs. This
   * test is targetted to test for hang or exceptions in blocking queue.
   */
  @Test
  public void testConcurrentOperationsDunitTestOnBlockingQueue() throws Exception {
    concurrentOperationsDunitTest(true, Scope.DISTRIBUTED_ACK);
  }

  private void concurrentOperationsDunitTest(final boolean createBlockingQueue,
      final Scope rscope) {
    // Create Cache and HARegionQueue in all the 4 VMs.

    CacheSerializableRunnable createRgnsAndQueues = new CacheSerializableRunnable(
        "CreateCache, mirrored Region & HARegionQueue with a CacheListener") {
      @Override
      public void run2() throws CacheException {
        HARegionQueueDUnitTest test = new HARegionQueueDUnitTest();
        System.getProperties().put("QueueRemovalThreadWaitTime", "2000");
        cache = test.createCache();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(rscope);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
        hrqa.setExpiryTime(5);
        try {
          if (createBlockingQueue) {
            hrq = HARegionQueue.getHARegionQueueInstance("testregion1", cache, hrqa,
                HARegionQueue.BLOCKING_HA_QUEUE, false);
          } else {
            hrq = HARegionQueue.getHARegionQueueInstance("testregion1", cache, hrqa,
                HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
          }
        } catch (Exception e) {
          throw new AssertionError(e);
        }
        factory.addCacheListener(new CacheListenerAdapter() {
          @Override
          public void afterCreate(final EntryEvent event) {
            if (toCnt) {
              Conflatable conflatable = new ConflatableObject(event.getKey(), event.getNewValue(),
                  ((EntryEventImpl) event).getEventId(), false, event.getRegion().getFullPath());

              try {
                hrq.put(conflatable);
              } catch (Exception e) {
                fail("The put operation in queue did not succeed due to exception =", e);
              }
            }
          }

          @Override
          public void afterUpdate(final EntryEvent event) {
            if (toCnt) {
              Conflatable conflatable = new ConflatableObject(event.getKey(), event.getNewValue(),
                  ((EntryEventImpl) event).getEventId(), true, event.getRegion().getFullPath());

              try {
                hrq.put(conflatable);
              } catch (Exception e) {
                fail("The put operation in queue did not succeed due to exception =", e);
              }
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
    CacheSerializableRunnable spawnThreadsAndperformOps =
        new CacheSerializableRunnable("Spawn multiple threads which do various operations") {

          @Override
          public void run2() throws CacheException {
            opThreads = new RunOp[4 + 2 + 2 + 2];
            for (int i = 0; i < 4; ++i) {
              opThreads[i] = new RunOp(RunOp.PUT, i);
            }
            for (int i = 4; i < 6; ++i) {
              opThreads[i] = new RunOp(RunOp.PEEK, i);
            }

            for (int i = 6; i < 8; ++i) {
              opThreads[i] = new RunOp(RunOp.TAKE, i);
            }

            for (int i = 8; i < 10; ++i) {
              opThreads[i] = new RunOp(RunOp.TAKE, i);
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


    SerializableCallable guaranteeOperationsOccured =
        new SerializableCallable("Check Ops Occurred") {
          @Override
          public Object call() throws CacheException {
            if (opThreads == null) {
              return false;
            }
            for (int i = 0; i < opThreads.length; ++i) {
              if (((RunOp) opThreads[i]).getNumOpsPerformed() == 0) {
                return false;
              }
            }
            return true;
          }
        };

    await()
        .untilAsserted(() -> assertTrue((Boolean) vm0.invoke(guaranteeOperationsOccured)));
    await()
        .untilAsserted(() -> assertTrue((Boolean) vm1.invoke(guaranteeOperationsOccured)));
    await()
        .untilAsserted(() -> assertTrue((Boolean) vm2.invoke(guaranteeOperationsOccured)));
    await()
        .untilAsserted(() -> assertTrue((Boolean) vm3.invoke(guaranteeOperationsOccured)));

    // In case of blocking HARegionQueue do some extra puts so that the
    // blocking threads
    // are exited
    CacheSerializableRunnable toggleFlag =
        new CacheSerializableRunnable("Toggle the flag to signal end of threads") {
          @Override
          public void run2() throws CacheException {
            toCnt = false;
            if (createBlockingQueue) {
              try {
                for (int i = 0; i < 100; ++i) {
                  hrq.put(new ConflatableObject("1", "1", new EventID(new byte[] {1}, 100, i),
                      false, "/x"));
                }
              } catch (Exception e) {
                throw new AssertionError(e);
              }
            }

          }
        };

    vm0.invokeAsync(toggleFlag);
    vm1.invokeAsync(toggleFlag);
    vm2.invokeAsync(toggleFlag);
    vm3.invokeAsync(toggleFlag);

    CacheSerializableRunnable joinWithThreads =
        new CacheSerializableRunnable("Join with the threads") {
          @Override
          public void run2() throws CacheException {
            for (int i = 0; i < opThreads.length; ++i) {

              if (opThreads[i].isInterrupted()) {
                fail("Test failed because  thread encountered exception");
              }
              ThreadUtils.join(opThreads[i], 30 * 1000);
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
   * This is to test the bug which is caused when HARegionQueue object hasnot been fully constructed
   * but as the HARegion has got constructed , it gets visible to QRM Message Thread.
   *
   * TODO: this test runs too long! Shorten run time. 1m 40s on new Mac.
   */
  @Test
  public void testNPEDueToHARegionQueueEscapeInConstructor() {
    // changing EXPIRY_TIME to 5 doesn't change how long the test runs!
    final int EXPIRY_TIME = 30; // test will run for this many seconds
    // Create two HARegionQueue 's in the two VMs. The frequency of QRM thread
    // should be high
    // Check for NullPointeException in the other VM.
    CacheSerializableRunnable createQueuesAndThread =
        new CacheSerializableRunnable("CreateCache, HARegionQueue and start thread") {
          @Override
          public void run2() throws CacheException {
            HARegionQueueDUnitTest test = new HARegionQueueDUnitTest();
            // TODO:ASIF: Bcoz of the QRM thread cannot take frequency below
            // 1 second , thus we need to carfully evaluate what to do.
            // For this bug to appear ,without bugfix , qrm needs to run
            // very fast.
            // System.getProperties().put("QueueRemovalThreadWaitTime", new Long(10));
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
                    new EventID(new byte[] {0}, 1, i), false, "dummy"));
              }
              opThreads = new Thread[1];
              opThreads[0] = new Thread(new Runnable() {
                @Override
                public void run() {
                  for (int i = 0; i < OP_COUNT; ++i) {
                    try {
                      Object o = hrq.take();
                      if (o == null) {
                        Thread.sleep(50);
                      }
                    } catch (InterruptedException e) {
                      throw new AssertionError(e);
                    }
                  }
                }
              });
              opThreads[0].start();

            } catch (Exception e) {
              throw new AssertionError(e);
            }
          }
        };

    CacheSerializableRunnable createQueues =
        new CacheSerializableRunnable("CreateCache, HARegionQueue ") {
          @Override
          public void run2() throws CacheException {
            createQueuesThread = Thread.currentThread();
            HARegionQueueDUnitTest test = new HARegionQueueDUnitTest();
            // System.getProperties().put("QueueRemovalThreadWaitTime",
            // new Long(120000));
            cache = test.createCache();
            cache.setMessageSyncInterval(EXPIRY_TIME);
            HARegionQueueAttributes hrqa = new HARegionQueueAttributes();
            hrqa.setExpiryTime(EXPIRY_TIME);
            try {
              hrq = HARegionQueue.getHARegionQueueInstance(
                  "testNPEDueToHARegionQueueEscapeInConstructor", cache, hrqa,
                  HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
            } catch (Exception e) {
              throw new AssertionError(e);
            }
          }
        };

    CacheSerializableRunnable waitForCreateQueuesThread =
        new CacheSerializableRunnable("joinCreateCache") {
          @Override
          public void run2() {
            WaitCriterion ev = new WaitCriterion() {
              @Override
              public boolean done() {
                return createQueuesThread != null;
              }

              @Override
              public String description() {
                return null;
              }
            };
            GeodeAwaitility.await().untilAsserted(ev);
            join(createQueuesThread, 300 * 1000);
          }
        };

    vm0.invoke(createQueuesAndThread);
    vm1.invokeAsync(createQueues);

    CacheSerializableRunnable joinWithThread =
        new CacheSerializableRunnable("CreateCache, HARegionQueue join with thread") {
          @Override
          public void run2() throws CacheException {
            if (opThreads[0].isInterrupted()) {
              fail("The test has failed as it encountered interrupts in puts & takes");
            }
            ThreadUtils.join(opThreads[0], 30 * 1000);
          }
        };
    vm0.invoke(joinWithThread);
    vm1.invoke(waitForCreateQueuesThread);
  }

  private static class RunOp extends Thread {

    private static final int PUT = 1;
    private static final int TAKE = 2;
    private static final int PEEK = 3;
    private static final int BATCH_PEEK = 4;

    private int opType;
    private int threadID;

    private int numOpsPerformed = 0;

    public RunOp(int opType, int id) {
      super("ID=" + id + ",Op=" + opType);
      this.opType = opType;
      this.threadID = id;
    }

    public int getNumOpsPerformed() {
      return numOpsPerformed;
    }

    @Override
    public void run() {
      Region rgn = cache.getRegion("test_region");
      int counter = 0;
      LogWriter logger = cache.getLogger();
      Conflatable cnf;
      try {
        while (toCnt) {
          // Thread.currentThread().getName() + " before doing operation of
          // type= "+ this.opType);
          switch (opType) {
            case PUT:
              rgn.put("key" + threadID, "val" + counter++);
              if (counter == 10)
                counter = 0;
              break;
            case TAKE:
              cnf = (Conflatable) hrq.take();
              if (logger.fineEnabled() && cnf != null) {
                logger.fine("Object retrieved  by take has key =" + cnf.getKeyToConflate()
                    + " and value as" + cnf.getValueToConflate());
              }
              break;
            case PEEK:
              cnf = (Conflatable) hrq.peek();
              if (logger.fineEnabled() && cnf != null) {
                logger.fine("Object retrieved  by peek has key =" + cnf.getKeyToConflate()
                    + " and value as" + cnf.getValueToConflate());
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
          numOpsPerformed++;
          // Thread.currentThread().getName() + " after Operation of type= "+
          // this.opType);

        }
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }
  }

  /**
   * Checks the data received by GII, only gets expired after proper construction of HARegionQueue
   * object.
   */
  private static void createHARegionQueueandCheckExpiration() throws Exception {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest();
    cache = test.createCache();
    HARegionQueueAttributes attrs = new HARegionQueueAttributes();
    attrs.setExpiryTime(1);
    hrq = getHARegionQueueInstance("HARegionQueueDUnitTest_region", cache, attrs,
        NON_BLOCKING_HA_QUEUE, false);
    // wait until we have a dead
    // server
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return hrq.getAvailableIds().size() == 0;
      }

      @Override
      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    // assertIndexDetailsEquals(0, hrq.getAvailableIds().size());
  }

  @Test
  public void testForDuplicateEvents() throws Exception {
    vm0.invoke(() -> HARegionQueueDUnitTest.createRegionQueue());
    vm1.invoke(() -> HARegionQueueDUnitTest.createRegionQueueandCheckDuplicates());
  }

  /**
   * HARegionQueue should not allow data with duplicate EventIds.
   */
  private static void createRegionQueueandCheckDuplicates() throws Exception {
    HARegionQueueDUnitTest test = new HARegionQueueDUnitTest();
    cache = test.createCache();

    hrq = HARegionQueue.getHARegionQueueInstance("HARegionQueueDUnitTest_region", cache,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false);

    assertEquals(2, hrq.size());

    EventID id1 = new EventID(new byte[] {1}, 1, 1);
    EventID id2 = new EventID(new byte[] {1}, 1, 2);
    ConflatableObject c1 =
        new ConflatableObject("1", "1", id1, false, "HARegionQueueDUnitTest_region");
    ConflatableObject c2 =
        new ConflatableObject("2", "2", id2, false, "HARegionQueueDUnitTest_region");

    hrq.put(c1);
    hrq.put(c2);

    // HARegion size should be 2 as data with same EventIDs is inserted into the queue
    assertEquals(2, hrq.size());
  }
}
