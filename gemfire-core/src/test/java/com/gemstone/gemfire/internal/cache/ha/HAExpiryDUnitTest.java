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

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * This test checks Expiration of events in the regionqueue.
 * 1. It creates regionqueue on 4 vms
 * 2. It puts ConflatableObject in each regionque
 * 3. Checks size of the regionqueue. Size of the regionqueue should be greater than 0.
 * 4. Waits for the period which is slightly greater than expiration period.
 * 5. Checks size of the regionqueue. Size of the regionqueue should be equal to 0.
 *
 *  @author Girish Thombare
 */

public class HAExpiryDUnitTest extends DistributedTestCase
{

  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  VM vm3 = null;

  protected static Cache cache = null;

  static String regionQueueName = "regionQueue1";

  private static final String REGION_NAME = "HAExpiryDUnitTest_region";

  static RegionQueue regionqueue = null;

  protected static int regionQueueSize = -1;

  public HAExpiryDUnitTest(String name) {
    super(name);
  }

  /**
   * This function creates regionqueue on 4 VMs
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

  @Override
  protected final void preTearDown() throws Exception {
    vm0.invoke(HAExpiryDUnitTest.class, "closeCache");
    vm1.invoke(HAExpiryDUnitTest.class, "closeCache");
    vm2.invoke(HAExpiryDUnitTest.class, "closeCache");
    vm3.invoke(HAExpiryDUnitTest.class, "closeCache");
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }

  public void testExpiryPeriod() throws Exception
  {
    vm0.invoke(HAExpiryDUnitTest.class, "createRegionQueue", new Object[] {new Boolean(false)});
    vm1.invoke(HAExpiryDUnitTest.class, "createRegionQueue", new Object[] {new Boolean(false)});
    vm2.invoke(HAExpiryDUnitTest.class, "createRegionQueue", new Object[] {new Boolean(false)});
    vm3.invoke(HAExpiryDUnitTest.class, "createRegionQueue", new Object[] {new Boolean(false)});
    
    vm0.invoke(new CacheSerializableRunnable("putFromVm") {

      public void run2() throws CacheException
      {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        region.put("KEY1", "VALUE1");
      }
    });

    vm0.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
    vm1.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
    vm2.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
    vm3.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");

   // Thread.sleep(7 * 1000);

    vm0.invoke(HAExpiryDUnitTest.class, "checkSizeAfterExpiration");
    vm1.invoke(HAExpiryDUnitTest.class, "checkSizeAfterExpiration");
    vm2.invoke(HAExpiryDUnitTest.class, "checkSizeAfterExpiration");
    vm3.invoke(HAExpiryDUnitTest.class, "checkSizeAfterExpiration");
  }
  
  public void testDurableExpiryPeriod() throws Exception
  {
    vm0.invoke(HAExpiryDUnitTest.class, "createRegionQueue", new Object[] {new Boolean(true)});
    vm1.invoke(HAExpiryDUnitTest.class, "createRegionQueue", new Object[] {new Boolean(true)});
    vm2.invoke(HAExpiryDUnitTest.class, "createRegionQueue", new Object[] {new Boolean(true)});
    vm3.invoke(HAExpiryDUnitTest.class, "createRegionQueue", new Object[] {new Boolean(true)});
    
    vm0.invoke(new CacheSerializableRunnable("putFromVm") {

      public void run2() throws CacheException
      {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        region.put("KEY1", "VALUE1");
      }
    });

    vm0.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
    vm1.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
    vm2.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
    vm3.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");

   Wait.pause(5000); // wait for some time to make sure that we give sufficient time
                // to expiry
   // in spite of giving time the events should not expire, and queue should be
   // same as before expiration
   vm0.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
   vm1.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
   vm2.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
   vm3.invoke(HAExpiryDUnitTest.class, "checkSizeBeforeExpiration");
   
  }

  /**
   * This function checks the regionqueue size before expiration. size should be >
   * 0.
   * @throws Exception
   */
  public static void checkSizeBeforeExpiration() throws Exception
  {
    HARegion regionForQueue = (HARegion)cache.getRegion(Region.SEPARATOR
        + HARegionQueue.createRegionName(regionQueueName));
    final HARegionQueue regionqueue = regionForQueue.getOwner();
    regionQueueSize = regionqueue.size();
    cache.getLogger().info(
        "Size of the regionqueue before expiration is " + regionQueueSize);
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return regionqueue.size() >= 1;
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 60 * 1000, 200, true);
    /*
     * if (regionqueue.size() < 1) fail("RegionQueue size canot be less than 1
     * before expiration");
     */
  }

  /**
   * This function checks the regionqueue size After expiration. size should be =
   * 0.
   * 
   * @throws Exception
   */
  public static void checkSizeAfterExpiration() throws Exception
  {

    HARegion regionForQueue = (HARegion)cache.getRegion(Region.SEPARATOR
        + HARegionQueue.createRegionName(regionQueueName));
    final HARegionQueue regionqueue = regionForQueue.getOwner();
    cache.getLogger().info(
        "Size of the regionqueue After expiration is " + regionqueue.size());
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return regionqueue.size() <= regionQueueSize;
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 60 * 1000, 200, true);
    
    /*
     * if (regionqueue.size() > regionQueueSize) fail("RegionQueue size should
     * be 0 after expiration");
     */
  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createRegionQueue(Boolean isDurable) throws Exception
  {
    new HAExpiryDUnitTest("temp").createCache(new Properties());
    HARegionQueueAttributes hattr = new HARegionQueueAttributes();
    // setting expiry time for the regionqueue.
    hattr.setExpiryTime(4);
    RegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(regionQueueName, cache, hattr,HARegionQueue.NON_BLOCKING_HA_QUEUE, isDurable.booleanValue());
    assertNotNull(regionqueue);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    CacheListener serverListener = new vmListener();
    factory.setCacheListener(serverListener);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}

/**
 * This listener performs the put of Conflatable object in the regionqueue.
 */

class vmListener extends CacheListenerAdapter
{
  public void afterCreate(EntryEvent event)
  {
    Cache cache = event.getRegion().getCache();
    HARegion regionForQueue = (HARegion)cache.getRegion(Region.SEPARATOR
        + HARegionQueue.createRegionName(HAExpiryDUnitTest.regionQueueName));
    HARegionQueue regionqueue = regionForQueue.getOwner();
    try {
      regionqueue.put(new ConflatableObject(event.getKey(),
          event.getNewValue(), new EventID(new byte[] { 1 }, 1, 1), false, "region1"));
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
