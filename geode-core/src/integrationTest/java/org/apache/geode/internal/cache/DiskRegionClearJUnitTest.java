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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.ThreadUtils;

/**
 * Test methods to ensure that disk Clear is apparently atomic to region clear.
 *
 * Data on disk should reflect data in memory. A put while clear is going on should wait for clear
 * and if it is successfully recorded in memory than it should be recorded on disk. Else if not
 * successfully recorded in memory than should not be recorded on disk
 *
 * TODO: use DiskRegionTestingBase and DiskRegionHelperFactory
 */
public class DiskRegionClearJUnitTest {

  private static Region testRegion = null;
  private static volatile int counter = 0;
  private static volatile boolean cleared = false;
  private static Cache cache = null;
  private static DistributedSystem distributedSystem = null;

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    distributedSystem = DistributedSystem.connect(properties);
    cache = CacheFactory.create(distributedSystem);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    RegionAttributes regionAttributes = factory.create();
    testRegion = cache.createRegion("TestRegion1", regionAttributes);
    CacheObserverHolder.setInstance(new CacheObserverListener());
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (cache != null && !cache.isClosed()) {
        for (final Region<?, ?> region : cache.rootRegions()) {
          Region root = (Region) region;
          // String name = root.getName();
          if (root.isDestroyed() || root instanceof HARegion) {
            continue;
          }
          try {
            root.localDestroyRegion("teardown");
          } catch (VirtualMachineError e) { // TODO: remove all this error handling
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable t) {
            cache.getLogger().error(t);
          }
        }
      }
    } finally {
      try {
        closeCache();
      } catch (VirtualMachineError e) { // TODO: remove all this error handling
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        cache.getLogger().error("Error in closing the cache ", t);

      }
    }
  }

  /**
   * Make sure the disk region stats are set to zero when the region is cleared.
   */
  @Test
  public void testClearAndStats() throws Exception {
    DiskRegion dr = ((LocalRegion) testRegion).getDiskRegion();
    assertEquals(0, dr.getStats().getNumEntriesInVM());
    // put a value in the region
    testRegion.put(new Long(1), new Long(1));
    assertEquals(1, dr.getStats().getNumEntriesInVM());
    testRegion.clear();
    assertEquals(0, dr.getStats().getNumEntriesInVM());
  }

  /** Close the cache */
  private static synchronized void closeCache() {
    if (cache != null) {
      try {
        if (!cache.isClosed()) {
          CacheTransactionManager txMgr = cache.getCacheTransactionManager();
          if (txMgr != null) {
            if (txMgr.exists()) {
              // make sure we cleanup this threads txid stored in a thread local
              txMgr.rollback();
            }
          }
          cache.close();
        }
      } finally {
        cache = null;
      }
    }
  }

  @Test
  public void testPutWhileclear() {
    // warm up
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    for (long i = 0; i < 100; i++) {
      testRegion.put(new Long(i), new Long(i));
    }
    Thread thread = new Thread(new Thread2());
    thread.start();
    final long tilt = System.currentTimeMillis() + 60 * 1000;
    // TODO why is this loop necessary?
    while (counter != 3) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        fail("interrupted");
      }
      if (System.currentTimeMillis() >= tilt) {
        fail("timed out counter=" + counter);
      }
    }
    ThreadUtils.join(thread, 10 * 60 * 1000);
    assertTrue(counter == 3);
    if (!cleared) {
      fail("clear not done although puts have been done");
    }
  }

  @Test
  public void testRecreateRegionAndCacheNegative() throws Exception {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    for (long i = 0; i < 100; i++) {
      testRegion.put(new Long(i), new Long(i));
    }
    testRegion.clear();
    assertEquals(0, testRegion.size());
    cache.close();
    distributedSystem.disconnect();
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    distributedSystem = DistributedSystem.connect(properties);
    cache = CacheFactory.create(distributedSystem);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    RegionAttributes regionAttributes = factory.create();
    testRegion = cache.createRegion("TestRegion1", regionAttributes);

    System.out.println("keySet after recovery = " + testRegion.keySet());
    assertEquals(0, testRegion.size());
  }

  @Test
  public void testRecreateRegionAndCachePositive() {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    for (long i = 0; i < 1000; i++) {
      testRegion.put(new Long(i), new Long(i));
    }
    testRegion.clear();
    for (long i = 0; i < 1000; i++) {
      testRegion.put(new Long(i), new Long(i));
    }
    assertEquals(1000, testRegion.size());
    cache.close();
    distributedSystem.disconnect();
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    distributedSystem = DistributedSystem.connect(properties);
    cache = CacheFactory.create(distributedSystem);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    RegionAttributes regionAttributes = factory.create();
    testRegion = cache.createRegion("TestRegion1", regionAttributes);
    assertEquals(1000, testRegion.size());
  }

  private static class Thread1 implements Runnable {
    @Override
    public void run() {
      for (long i = 0; i < 100; i++) {
        testRegion.put(new Long(i), new Long(i));
      }
      counter++;
    }
  }

  private static class Thread2 implements Runnable {
    @Override
    public void run() {
      testRegion.clear();
    }
  }

  private static class CacheObserverListener extends CacheObserverAdapter {

    @Override
    public void afterRegionClear(RegionEvent event) {
      cleared = true;
    }

    @Override
    public void beforeDiskClear() {
      for (int i = 0; i < 3; i++) {
        Thread thread = new Thread(new Thread1());
        thread.start();
      }
    }
  }

  private static class CacheObserver extends CacheObserverAdapter {
  }
}
