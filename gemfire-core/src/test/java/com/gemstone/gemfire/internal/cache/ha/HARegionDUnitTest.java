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

import junit.framework.Assert;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test to verify :
 * 
 * 1)put() on a mirrored HARegion does not propagate 2)localDestroy() allowed on
 * a mirrored region 3) GII happens normally
 * 
 * @author Mitul Bid
 * 
 */
public class HARegionDUnitTest extends DistributedTestCase
{
  VM vm0 = null;

  VM vm1 = null;

  private static Cache cache = null;
  private static final String REGION_NAME = "HARegionDUnitTest_region" ;

  /** constructor */
  public HARegionDUnitTest(String name) {
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
  }

  /**
   * close the cache in tearDown
   */
  public void tearDown2() throws Exception
  {
	super.tearDown2();  
    vm0.invoke(HARegionDUnitTest.class, "closeCache");
    vm1.invoke(HARegionDUnitTest.class, "closeCache");
  }

  /**
   * create cache
   * 
   * @return
   * @throws Exception
   */
  private Cache createCache() throws Exception
  {
    Properties props = new Properties();
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    Cache cache = null;
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
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
    vm0.invoke(HARegionDUnitTest.class, "createRegion");
    vm1.invoke(HARegionDUnitTest.class, "createRegion");
    vm0.invoke(HARegionDUnitTest.class, "putValue1");
    vm1.invoke(HARegionDUnitTest.class, "getNull");
    vm1.invoke(HARegionDUnitTest.class, "putValue2");
    vm0.invoke(HARegionDUnitTest.class, "getValue1");
    vm1.invoke(HARegionDUnitTest.class, "getValue2");

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
    vm0.invoke(HARegionDUnitTest.class, "createRegion");
    vm1.invoke(HARegionDUnitTest.class, "createRegion");
    vm0.invoke(HARegionDUnitTest.class, "putValue1");
    vm1.invoke(HARegionDUnitTest.class, "getNull");
    vm1.invoke(HARegionDUnitTest.class, "putValue2");
    vm0.invoke(HARegionDUnitTest.class, "getValue1");
    vm1.invoke(HARegionDUnitTest.class, "getValue2");
    vm0.invoke(HARegionDUnitTest.class, "destroy");
    vm0.invoke(HARegionDUnitTest.class, "getNull");
    vm1.invoke(HARegionDUnitTest.class, "getValue2");
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
    vm0.invoke(HARegionDUnitTest.class, "createRegion");
    vm0.invoke(HARegionDUnitTest.class, "putValue1");
    vm0.invoke(HARegionDUnitTest.class, "getValue1");
    vm1.invoke(HARegionDUnitTest.class, "createRegion");
    vm1.invoke(HARegionDUnitTest.class, "getValue1");
    vm1.invoke(HARegionDUnitTest.class, "putValue2");
    vm1.invoke(HARegionDUnitTest.class, "getValue2");

  }

  /**
   * 1) Create mirrored HARegion region1 in VM1 2) do a put in VM1 3) get teh
   * value in VM1 to assert put has happened successfully 4) Create mirrored
   * HARegion region1 in VM2 5) do a get in VM2 to verify that value was got
   * through GII 6) do a put in VM2 7) assert put in VM2 was successful
   * 
   */
  public void testLocalDestroyRegion()
  {
    vm0.invoke(HARegionDUnitTest.class, "createRegion");
    vm1.invoke(HARegionDUnitTest.class, "createRegion");
    vm0.invoke(HARegionDUnitTest.class, "destroyRegion");
    vm1.invoke(HARegionDUnitTest.class, "verifyRegionNotDestroyed");
  
  }

  /**
   * Destroy the region
   * 
   */
  public static void destroyRegion()
  {
    cache.getRegion(REGION_NAME).localDestroyRegion(null);
  }

  /**
   * Verify Region exists
   *
   */
  public static void verifyRegionNotDestroyed()
  {
    Assert.assertTrue(cache.getRegion(REGION_NAME) != null);
  }
  
  /**
   * 1) Create mirrored HARegion region1 in VM1 2) do a put in VM1 3) get teh
   * value in VM1 to assert put has happened successfully 4) Create mirrored
   * HARegion region1 in VM2 5) do a get in VM2 to verify that value was got
   * through GII 6) do a put in VM2 7) assert put in VM2 was successful
   * 
   */
  public void testQRM()
  {
    vm0.invoke(HARegionDUnitTest.class, "createRegionQueue");
    vm1.invoke(HARegionDUnitTest.class, "createRegionQueue");
    vm0.invoke(HARegionDUnitTest.class, "verifyAddingDispatchMesgs");
    try {
      Thread.sleep(5000);
    }
    catch (InterruptedException e) {
      fail("interrupted");
    }
    vm1.invoke(HARegionDUnitTest.class, "verifyDispatchedMessagesRemoved");
  }

  /**
   * create a client with 2 regions sharing a common writer
   * 
   * @throws Exception
   */

  public static void createRegion() throws Exception
  {
    HARegionDUnitTest test = new HARegionDUnitTest(REGION_NAME);
    cache = test.createCache();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    HARegion.getInstance(REGION_NAME, (GemFireCacheImpl)cache, null,factory.create());
  }

  private static HARegionQueue hrq = null;

//  private static int counter = 0;

  /**
   * 
   * 
   * @throws Exception
   */

  public static void createRegionQueue() throws Exception
  {
    HARegionDUnitTest test = new HARegionDUnitTest(REGION_NAME);
    cache = test.createCache();
    /*AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);*/
    hrq = HARegionQueue.getHARegionQueueInstance(REGION_NAME, cache,HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    EventID id1 = new EventID(new byte[] { 1 }, 1, 1);
    EventID id2 = new EventID(new byte[] { 1 }, 1, 2);
    ConflatableObject c1 = new ConflatableObject("1", "1", id1, false,
        REGION_NAME);
    ConflatableObject c2 = new ConflatableObject("2", "2", id2, false,
        REGION_NAME);
    hrq.put(c1);
    hrq.put(c2);

  }

  public static void verifyAddingDispatchMesgs()
  {
    Assert.assertTrue(HARegionQueue.getDispatchedMessagesMapForTesting()
        .isEmpty());
    hrq.addDispatchedMessage(new ThreadIdentifier(new byte[1],1),1);
    Assert.assertTrue(!HARegionQueue.getDispatchedMessagesMapForTesting()
        .isEmpty());
  }



  public static void verifyDispatchedMessagesRemoved()
  {
    try {
      Region region = hrq.getRegion();
      if (region.get(new Long(0)) != null) {
        fail("Expected message to have been deleted but it is not deleted");
      }

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
   * do three puts on key-1
   * 
   */
  public static void putValue1()
  {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      r1.put("key-1", "value-1");
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("failed while region.put()", ex);
    }
  }

  /**
   * do three puts on key-1
   * 
   */
  public static void putValue2()
  {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
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
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
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
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
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
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
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
      Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      region1.localDestroy("key-1");
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("test failed due to exception in destroy ");
    }
  }

}
