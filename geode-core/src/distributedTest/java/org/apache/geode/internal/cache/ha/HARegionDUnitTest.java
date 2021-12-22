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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.AdditionalAnswers;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Test to verify :
 *
 * 1)put() on a mirrored HARegion does not propagate 2)localDestroy() allowed on a mirrored region
 * 3) GII happens normally
 */
@Category({ClientSubscriptionTest.class})
public class HARegionDUnitTest extends JUnit4DistributedTestCase {

  VM vm0 = null;

  VM vm1 = null;

  private static InternalCache cache = null;
  private static final String REGION_NAME = "HARegionDUnitTest_region";

  /** constructor */
  public HARegionDUnitTest() {
    super();
  }

  /**
   * get the VM's
   */
  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
  }

  /**
   * close the cache in tearDown
   */
  @Override
  public final void preTearDown() throws Exception {
    vm0.invoke(HARegionDUnitTest::closeCache);
    vm1.invoke(HARegionDUnitTest::closeCache);
  }

  /**
   * create cache
   *
   */
  private InternalCache createCache() throws Exception {
    Properties props = new Properties();
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    InternalCache cache = null;
    cache = (InternalCache) CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 and VM2 2) do a put in VM1 3) assert that the put
   * has not propagated from VM1 to VM2 4) do a put in VM2 5) assert that the value in VM1 has not
   * changed to due to put in VM2 6) assert put in VM2 was successful by doing a get
   *
   */
  @Test
  public void testLocalPut() {
    vm0.invoke(HARegionDUnitTest::createRegion);
    vm1.invoke(HARegionDUnitTest::createRegion);
    vm0.invoke(HARegionDUnitTest::putValue1);
    vm1.invoke(HARegionDUnitTest::getNull);
    vm1.invoke(HARegionDUnitTest::putValue2);
    vm0.invoke(HARegionDUnitTest::getValue1);
    vm1.invoke(HARegionDUnitTest::getValue2);

  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 and VM2 2) do a put in VM1 3) assert that the put
   * has not propagated from VM1 to VM2 4) do a put in VM2 5) assert that the value in VM1 has not
   * changed to due to put in VM2 6) assert respective puts the VMs were successful by doing a get
   * 7) localDestroy key in VM1 8) assert key has been destroyed in VM1 9) assert key has not been
   * destroyed in VM2
   *
   */
  @Test
  public void testLocalDestroy() {
    vm0.invoke(HARegionDUnitTest::createRegion);
    vm1.invoke(HARegionDUnitTest::createRegion);
    vm0.invoke(HARegionDUnitTest::putValue1);
    vm1.invoke(HARegionDUnitTest::getNull);
    vm1.invoke(HARegionDUnitTest::putValue2);
    vm0.invoke(HARegionDUnitTest::getValue1);
    vm1.invoke(HARegionDUnitTest::getValue2);
    vm0.invoke(HARegionDUnitTest::destroy);
    vm0.invoke(HARegionDUnitTest::getNull);
    vm1.invoke(HARegionDUnitTest::getValue2);
  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 2) do a put in VM1 3) get the value in VM1 to assert
   * put has happened successfully 4) Create mirrored HARegion region1 in VM2 5) do a get in VM2 to
   * verify that value was got through GII 6) do a put in VM2 7) assert put in VM2 was successful
   *
   */
  @Test
  public void testGII() {
    vm0.invoke(HARegionDUnitTest::createRegion);
    vm0.invoke(HARegionDUnitTest::putValue1);
    vm0.invoke(HARegionDUnitTest::getValue1);
    vm1.invoke(HARegionDUnitTest::createRegion);
    vm1.invoke(HARegionDUnitTest::getValue1);
    vm1.invoke(HARegionDUnitTest::putValue2);
    vm1.invoke(HARegionDUnitTest::getValue2);

  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 2) do a put in VM1 3) get the value in VM1 to assert
   * put has happened successfully 4) Create mirrored HARegion region1 in VM2 5) do a get in VM2 to
   * verify that value was got through GII 6) do a put in VM2 7) assert put in VM2 was successful
   *
   */
  @Test
  public void testLocalDestroyRegion() {
    vm0.invoke(HARegionDUnitTest::createRegion);
    vm1.invoke(HARegionDUnitTest::createRegion);
    vm0.invoke(HARegionDUnitTest::destroyRegion);
    vm1.invoke(HARegionDUnitTest::verifyRegionNotDestroyed);

  }

  /**
   * Destroy the region
   *
   */
  public static void destroyRegion() {
    cache.getRegion(REGION_NAME).localDestroyRegion(null);
  }

  /**
   * Verify Region exists
   *
   */
  public static void verifyRegionNotDestroyed() {
    assertTrue(cache.getRegion(REGION_NAME) != null);
  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 2) do a put in VM1 3) get the value in VM1 to assert
   * put has happened successfully 4) Create mirrored HARegion region1 in VM2 5) do a get in VM2 to
   * verify that value was got through GII 6) do a put in VM2 7) assert put in VM2 was successful
   *
   */
  @Test
  public void testQRM() {
    vm0.invoke(HARegionDUnitTest::createRegionQueue);
    vm1.invoke(HARegionDUnitTest::createRegionQueue);
    vm0.invoke(HARegionDUnitTest::verifyAddingDispatchMesgs);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      fail("interrupted");
    }
    vm1.invoke(HARegionDUnitTest::verifyDispatchedMessagesRemoved);
  }

  /**
   * create a client with 2 regions sharing a common writer
   *
   */

  public static void createRegion() throws Exception {
    HARegionDUnitTest test = new HARegionDUnitTest();
    cache = test.createCache();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    // Mock the HARegionQueue and answer the input CachedDeserializable when updateHAEventWrapper is
    // called
    HARegionQueue harq = mock(HARegionQueue.class);
    when(harq.updateHAEventWrapper(any(), any(), any()))
        .thenAnswer(AdditionalAnswers.returnsSecondArg());

    HARegion.getInstance(REGION_NAME, cache, harq, factory.create(),
        disabledClock());
  }

  private static HARegionQueue hrq = null;

  // private static int counter = 0;

  public static void createRegionQueue() throws Exception {
    HARegionDUnitTest test = new HARegionDUnitTest();
    cache = test.createCache();
    /*
     * AttributesFactory factory = new AttributesFactory(); factory.setScope(Scope.DISTRIBUTED_ACK);
     * factory.setDataPolicy(DataPolicy.REPLICATE);
     */
    hrq = HARegionQueue.getHARegionQueueInstance(REGION_NAME, cache,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false, disabledClock());
    EventID id1 = new EventID(new byte[] {1}, 1, 1);
    EventID id2 = new EventID(new byte[] {1}, 1, 2);
    ConflatableObject c1 = new ConflatableObject("1", "1", id1, false, REGION_NAME);
    ConflatableObject c2 = new ConflatableObject("2", "2", id2, false, REGION_NAME);
    hrq.put(c1);
    hrq.put(c2);

  }

  public static void verifyAddingDispatchMesgs() {
    assertTrue(HARegionQueue.getDispatchedMessagesMapForTesting().isEmpty());
    hrq.addDispatchedMessage(new ThreadIdentifier(new byte[1], 1), 1);
    assertTrue(!HARegionQueue.getDispatchedMessagesMapForTesting().isEmpty());
  }



  public static void verifyDispatchedMessagesRemoved() {
    try {
      Region region = hrq.getRegion();
      if (region.get(0L) != null) {
        fail("Expected message to have been deleted but it is not deleted");
      }

      if (region.get(1L) == null) {
        fail("Expected message not to have been deleted but it is deleted");
      }
    } catch (Exception e) {
      fail("test failed due to an exception :  " + e);
    }
  }

  /**
   * close the cache
   *
   */
  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * do three puts on key-1
   *
   */
  public static void putValue1() {
    try {
      Region r1 = cache.getRegion(SEPARATOR + REGION_NAME);
      r1.put("key-1", "value-1");
    } catch (Exception ex) {
      ex.printStackTrace();
      org.apache.geode.test.dunit.Assert.fail("failed while region.put()", ex);
    }
  }

  /**
   * do three puts on key-1
   *
   */
  public static void putValue2() {
    try {
      Region r1 = cache.getRegion(SEPARATOR + REGION_NAME);
      r1.put("key-1", "value-2");
    } catch (Exception ex) {
      ex.printStackTrace();
      org.apache.geode.test.dunit.Assert.fail("failed while region.put()", ex);
    }
  }

  /**
   * do a get on region1
   *
   */
  public static void getValue1() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      if (!(r.get("key-1").equals("value-1"))) {
        fail("expected value to be value-1 but it is not so");
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      org.apache.geode.test.dunit.Assert.fail("failed while region.get()", ex);
    }
  }

  /**
   * do a get on region1
   *
   */
  public static void getNull() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      if (!(r.get("key-1") == (null))) {
        fail("expected value to be null but it is not so");
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      org.apache.geode.test.dunit.Assert.fail("failed while region.get()", ex);
    }
  }

  /**
   * do a get on region1
   *
   */
  public static void getValue2() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      if (!(r.get("key-1").equals("value-2"))) {
        fail("expected value to be value-2 but it is not so");
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      org.apache.geode.test.dunit.Assert.fail("failed while region.get()", ex);
    }
  }

  /**
   * destroy key-1
   *
   */
  public static void destroy() {
    try {
      Region region1 = cache.getRegion(SEPARATOR + REGION_NAME);
      region1.localDestroy("key-1");
    } catch (Exception e) {
      e.printStackTrace();
      fail("test failed due to exception in destroy ");
    }
  }

}
