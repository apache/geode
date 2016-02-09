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

import java.util.ArrayList;
import java.util.Iterator;
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
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This test points out the bug when GII of HARegion Queue is happening and at the same time it is receiving puts from peer to peer.
 * The test performs following operations in sequence
 * 1. Create HARegion Queue on vm0
 * 2. Perform put operations on fixed keys from vm0.
 * 4. Start vm1 and create HARegion Queue asynchronously and notify vm0 after its creation
 * 5. Put the data from vm0 asynchronously till HARegion Queue gets created in vm1.
 * 6. Validate the data. Puts happened during GII should be missed.
 * @author Girish Thombare
 *
 */

public class HAGIIBugDUnitTest extends DistributedTestCase
{

  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  VM vm3 = null;

  protected static Cache cache = null;

  static String regionQueueName = "regionQueue1";

  private static final String REGION_NAME = "HAGIIBugDUnitTest_region";

  static RegionQueue regionqueue = null;

  static boolean isTestFailed = false;

  final static int NO_OF_PUTS = 100;

  final static int NO_OF_PUTS_BEFORE_GII = 10000;;

  static int TOTAL_NO_OF_PUTS = 0;

  static volatile boolean isHARegionQueueUp = false;

  static volatile boolean isStop = false;

  static Object total_no_puts[] = new Object[4];

  ArrayList keys_set_before_gii = new ArrayList();

  static ArrayList keys_set_after_gii = new ArrayList();

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

    vm0.invoke(HAGIIBugDUnitTest.class, "createRegionQueue");

  }

  @Override
  protected final void preTearDown() throws Exception {
    vm0.invoke(HAGIIBugDUnitTest.class, "closeCache");
    vm1.invoke(HAGIIBugDUnitTest.class, "closeCache");
    vm2.invoke(HAGIIBugDUnitTest.class, "closeCache");
    vm3.invoke(HAGIIBugDUnitTest.class, "closeCache");
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }

  protected void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  
  public void testDummy() throws Exception
  {
    LogWriterUtils.getLogWriter().info("This is Dummy test for the GII");  
  }
  
  
  public void _testGIIBug() throws Exception
  {

    vm0.invoke(putFromVmBeforeGII("vm0_1"));
    populateKeySet("vm0_1");
    Thread t1 = new Thread() {
      public void run()
      {
        try {

          createCache(new Properties());
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
          CacheListener regionListener = new vmListenerToCheckHARegionQueue();
          factory.setCacheListener(regionListener);
          RegionAttributes attrs = factory.create();
          Region region = cache.createRegion(REGION_NAME, attrs);
          LogWriterUtils.getLogWriter().info(
              "Name of the region is : " + region.getFullPath());

          HARegionQueueAttributes hattr = new HARegionQueueAttributes();
          // setting expiry time for the regionqueue.
          hattr.setExpiryTime(12000000);
          RegionQueue regionqueue = null;
          regionqueue = HARegionQueue.getHARegionQueueInstance(regionQueueName,
              cache, hattr, HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
          isHARegionQueueUp = true;
          vm0.invoke(setStopFlag());
          assertNotNull(regionqueue);

        }
        catch (Exception e) {

          isTestFailed = true;
          e.printStackTrace();
        }
      }
    };

    AsyncInvocation[] async = new AsyncInvocation[4];
    async[0] = vm0.invokeAsync(putFrmVm("vm0_2"));
    t1.start();
    ThreadUtils.join(t1, 30 * 1000);
    if (isTestFailed)
      fail("HARegionQueue can not be created");

    for (int count = 0; count < 1; count++) {
      ThreadUtils.join(async[count], 30 * 1000);
      if (async[count].exceptionOccurred()) {
        Assert.fail("Got exception on " + count, async[count].getException());
      }
    }

    total_no_puts[0] = vm0.invoke(HAGIIBugDUnitTest.class, "getTotalNoPuts");
    populate_keys_after_gii();

    boolean validationFlag = false;
    validateResults(validationFlag);
    if (keys_set_before_gii.size() != 0)
      fail("Data in the HARegion Queue is inconsistent for the keys that are put before GII");

    validationFlag = true;
    validateResults(validationFlag);
    LogWriterUtils.getLogWriter().info(
        "No. of keys that are missed by HARegion Queue during GII "
            + keys_set_after_gii.size());

    if (keys_set_after_gii.size() != 0)
      fail("Set of the keys are missed by HARegion Queue during GII");

  }

  private void populate_keys_after_gii()
  {
//    int k = 0;
    for (int i = 0; i < 1; i++) {
      long totalPuts = ((Long)total_no_puts[i]).longValue() - 3 * NO_OF_PUTS;
      LogWriterUtils.getLogWriter().info("Total no of puts expectesd " + totalPuts);
      for (int j = 0; j < totalPuts; j++) {
        keys_set_after_gii.add("vm" + i + "_2" + j);

      }
    }
  }

  private void populateKeySet(String whichKey)
  {
    for (int i = 0; i < NO_OF_PUTS_BEFORE_GII; i++) {
      keys_set_before_gii.add(whichKey + i);
    }

  }

  
  private void validateResults(boolean isSecond)
  {
    HARegion regionForQueue = (HARegion)cache.getRegion(Region.SEPARATOR
        + HARegionQueue.createRegionName(HAExpiryDUnitTest.regionQueueName));
    LogWriterUtils.getLogWriter().info(
        "Region Queue size : " + regionForQueue.keys().size());
    Iterator itr = regionForQueue.entries(false).iterator();
    while (itr.hasNext()) {
      Entry entry = (Entry)itr.next();
      if (entry.getKey() instanceof Long) {
        String strValue = (String)((ConflatableObject)entry.getValue())
            .getKey();
        if (isSecond) {
          if (strValue.indexOf("_2") != -1) {
            if (keys_set_after_gii.contains(((ConflatableObject)entry
                .getValue()).getKey())) {
              keys_set_after_gii.remove(strValue);
            }
          }
        }
        else {
          if (strValue.indexOf("_1") != -1) {
            if (!keys_set_before_gii.contains(strValue)) {
              fail("Key " + ((ConflatableObject)entry.getValue()).getKey()
                  + " not present in the region queue before GII");
            }
            else {
              keys_set_before_gii.remove(strValue);

            }
          }
        }
      }
    }
  }

  public static Object getTotalNoPuts()
  {

    return new Long(TOTAL_NO_OF_PUTS);

  }

  private CacheSerializableRunnable putFromVmBeforeGII(final String whichVm)
  {

    CacheSerializableRunnable putBeforeGII = new CacheSerializableRunnable(
        "putBeforeGII") {
      public void run2() throws CacheException
      {

        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        for (int i = 0; i < NO_OF_PUTS_BEFORE_GII; i++) {
          region.put(whichVm + i, whichVm + i);
        }
      }
    };

    return putBeforeGII;

  }

  private CacheSerializableRunnable putFrmVm(final String whichVm)
  {

    CacheSerializableRunnable putFromVM = new CacheSerializableRunnable(
        "putFromVM") {
      public void run2() throws CacheException
      {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        int j = 0;
        while (true) {
          if (isStop)
            break;
          for (int i = 0; i < NO_OF_PUTS; i++) {
            region.put(whichVm + j, whichVm + j);
            j++;
          }
          TOTAL_NO_OF_PUTS = TOTAL_NO_OF_PUTS + NO_OF_PUTS;
        }
        LogWriterUtils.getLogWriter().info("Total no of puts : " + TOTAL_NO_OF_PUTS);
      }
    };

    return putFromVM;
  }

  protected CacheSerializableRunnable setStopFlag()
  {
    CacheSerializableRunnable setFlag = new CacheSerializableRunnable("setFlag") {
      public void run2() throws CacheException
      {
        isStop = true;
      }
    };

    return setFlag;
  }

  public static void createRegionQueue() throws Exception
  {
    new HAGIIBugDUnitTest("temp").createCache(new Properties());
    HARegionQueueAttributes hattr = new HARegionQueueAttributes();
    // setting expiry time for the regionqueue.
    hattr.setExpiryTime(12000000);
    RegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(
        regionQueueName, cache, hattr, HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    assertNotNull(regionqueue);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    CacheListener regionListener = new vmListenerToPutInHARegionQueue();
    factory.setCacheListener(regionListener);
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

  public HAGIIBugDUnitTest(String arg0) {
    super(arg0);

  }

}

/**
 * This listener performs the put of Conflatable object in the regionqueue.
 */

class vmListenerToPutInHARegionQueue extends CacheListenerAdapter
{
  public void afterCreate(EntryEvent event)
  {

    Cache cache = event.getRegion().getCache();
    HARegion regionForQueue = (HARegion)cache.getRegion(Region.SEPARATOR
        + HARegionQueue.createRegionName(HAGIIBugDUnitTest.regionQueueName));
    HARegionQueue regionqueue = regionForQueue.getOwner();

    try {
      regionqueue.put(new ConflatableObject(event.getKey(),
          event.getNewValue(), ((EntryEventImpl)event).getEventId(),
          false, "region1"));
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}

class vmListenerToCheckHARegionQueue extends CacheListenerAdapter
{
  public void afterCreate(EntryEvent event)
  {
    if (HAGIIBugDUnitTest.isHARegionQueueUp) {
      Cache cache = event.getRegion().getCache();
      HARegion regionForQueue = (HARegion)cache.getRegion(Region.SEPARATOR
          + HARegionQueue.createRegionName(HAGIIBugDUnitTest.regionQueueName));
      HARegionQueue regionqueue = regionForQueue.getOwner();
      try {
        regionqueue.put(new ConflatableObject(event.getKey(),
            event.getNewValue(), ((EntryEventImpl)event).getEventId(),
            false, "region1"));
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
