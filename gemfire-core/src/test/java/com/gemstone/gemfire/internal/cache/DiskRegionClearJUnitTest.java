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
package com.gemstone.gemfire.internal.cache;

import java.util.Iterator;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.versions.VersionStamp;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

// @TODO: use DiskRegionTestingBase and DiskRegionHelperFactory
/**
 * Test methods to ensure that disk Clear is apparently atomic to region clear.
 * 
 * Data on disk should reflect data in memory. A put while clear is going on should
 * wait for clear and if it is successfully recorded in memory than it should
 * be recored on disk. Else if not successfully recorded in memory than should not be
 * recorded on disk
 * 
 */
@Category(IntegrationTest.class)
public class DiskRegionClearJUnitTest {

  static Region testRegion = null;
  static Object returnObject = null;
  static boolean done = false;
  static volatile int counter = 0;
  static volatile boolean cleared = false;
  static volatile long entries = 0;
  static Cache cache = null;  
  static DistributedSystem distributedSystem = null;
  
  private static String regionName = "TestRegion";

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("mcast-port", "0");
    properties.setProperty("locators", "");
    distributedSystem = DistributedSystem
    .connect(properties);
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
        for (Iterator itr = cache.rootRegions().iterator(); itr.hasNext();) {
          Region root = (Region)itr.next();
//          String name = root.getName();
					if(root.isDestroyed() || root instanceof HARegion) {
            continue;
        	}
          try {
            root.localDestroyRegion("teardown");
          }
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable t) {
            cache.getLogger().error(t);
          }
        }
      }
    }
    finally {
      try {
        closeCache();
      }
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        cache.getLogger().error("Error in closing the cache ", t);
        
      }
    }
  }
  
  
  /**
   * Make sure the disk region stats are set to zero when the region is cleared.
   */
  @Test
  public void testClearAndStats() throws Exception {
    DiskRegion dr = ((LocalRegion)testRegion).getDiskRegion();
    assertEquals(0, dr.getStats().getNumEntriesInVM());
    // put a value in the region
    testRegion.put(new Long(1), new Long(1));
    assertEquals(1, dr.getStats().getNumEntriesInVM());
    testRegion.clear();
    assertEquals(0, dr.getStats().getNumEntriesInVM());
  }
 

  /** Close the cache */
  private static synchronized final void closeCache() {
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
    //warm up
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    for(long i=0;i<100; i++) {
      testRegion.put(new Long(i), new Long(i));
    }
    Thread thread = new Thread(new Thread2());
    thread.start();
    final long tilt = System.currentTimeMillis() + 60 * 1000;
    // TODO why is this loop necessary?
    while(counter!=3) {
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        fail("interrupted");
       }      
      if (System.currentTimeMillis() >= tilt) {
        fail("timed out counter="+counter);
      }
    }
    DistributedTestCase.join(thread, 10 * 60 * 1000, null);
    Assert.assertTrue(counter == 3);
    if(!cleared)
      fail("clear not done although puts have been done");    
  }

  @Test
  public void testRecreateRegionAndCacheNegative() {
    try {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      for(long i=0;i<100; i++) {
        testRegion.put(new Long(i), new Long(i));
      }
      testRegion.clear();
      assertEquals(0, testRegion.size());
      cache.close();
      distributedSystem.disconnect();
      Properties properties = new Properties();
      properties.setProperty("mcast-port", "0");
      properties.setProperty("locators", "");
      distributedSystem = DistributedSystem.connect(properties);
      cache = CacheFactory.create(distributedSystem);
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      RegionAttributes regionAttributes = factory.create();
      testRegion = cache.createRegion("TestRegion1", regionAttributes);
      
    }
    catch (Exception e) {
      fail("test failed due to "+e);
     }
    System.out.println("keySet after recovery = " + testRegion.keySet());
    assertEquals(0, testRegion.size());
  }
  
  @Test
  public void testRecreateRegionAndCachePositive() {
    int size = 0;
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      for(long i=0;i<1000; i++) {
        testRegion.put(new Long(i), new Long(i));
      }
      testRegion.clear();
      for(long i=0;i<1000; i++) {
        testRegion.put(new Long(i), new Long(i));
      }
      assertEquals(1000, testRegion.size());
      cache.close();
      distributedSystem.disconnect();
      Properties properties = new Properties();
      properties.setProperty("mcast-port", "0");
      properties.setProperty("locators", "");
      distributedSystem = DistributedSystem.connect(properties);
      cache = CacheFactory.create(distributedSystem);
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      RegionAttributes regionAttributes = factory.create();
      testRegion = cache.createRegion("TestRegion1", regionAttributes);
    assertEquals(1000, testRegion.size());
  }
  
  protected static class Thread1 implements Runnable {

    
    
    public void run() {
      for(long i=0 ; i< 100 ; i++) {     
      testRegion.put(new Long(i), new Long(i));
      }
      counter++;
    }
  }

  protected static class Thread2 implements Runnable {

    public void run() {
      testRegion.clear();
    }
  }

  protected static class CacheObserverListener extends CacheObserverAdapter {
    
    
    public void afterRegionClear(RegionEvent event) {
      cleared = true;
    }

    public void beforeDiskClear() {
      for(int i=0; i<3; i++) {
      Thread thread = new Thread(new Thread1());
      thread.start();
      }
    }
  }

  protected static class CacheObserver extends CacheObserverAdapter
  {

  }
}
