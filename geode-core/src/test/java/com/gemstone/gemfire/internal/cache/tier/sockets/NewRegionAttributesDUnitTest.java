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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;

/**
 * This tests that basic entry operations work properly when regions are
 * configured with newly added RegionAttributes in a P2P environment.
 * 
 * @author Dinesh Patel
 * @author Suyog Bhokare
 */
public class NewRegionAttributesDUnitTest extends DistributedTestCase
{

  /** test VM */
  VM vm0, vm1 = null;

  /** the cache instance for the test */
  private static Cache cache = null;

  /** total number of puts for the test */
  private static final int TOTAL_PUTS = 10;

  private static final String REGION_NAME = "NewRegionAttributesDUnitTest_region" ;

  /**
   * Creates a test instance with the given name
   * 
   * @param name -
   *          name of test instance
   */
  public NewRegionAttributesDUnitTest(String name) {
    super(name);
  }

  /**
   * Creates the server cache on test-VMs
   * 
   * @throws Exception
   *           thrown if any problem occurs in set-up
   */
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    Wait.pause(5000);
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    Object[] objArr = new Object[4];
    // enable WAN
    objArr[0] = new Boolean(true);
    // enable Publisher
    objArr[1] = new Boolean(true);
    // enableConflation
    objArr[2] = new Boolean(true);
    // enableAsyncConflation
    objArr[3] = new Boolean(true);

    // create cache on test VMs
    vm0.invoke(NewRegionAttributesDUnitTest.class, "createServerCache", objArr);
    vm1.invoke(NewRegionAttributesDUnitTest.class, "createServerCache", objArr);
  }

  /**
   * Closes the cache on test VMs
   * 
   * @throws Exception
   *           thrown if any problem occurs while closing the cache
   */
  @Override
  protected final void preTearDown() throws Exception {
    vm0.invoke(NewRegionAttributesDUnitTest.class, "closeCache");
    vm1.invoke(NewRegionAttributesDUnitTest.class, "closeCache");
  }

  /**
   * Creates the cache and the region on the test VM with given parameters
   * 
   * @param enableWan
   *          boolean to make test-region wan-enabled
   * @param setPublisher
   *          boolean to make test-region as publisher
   * @param enableConflation
   *          boolean to enable conflation for test-region
   * @param enableAsyncConflation
   *          boolean to enable async conflation on test-region
   * @throws Exception
   *           thrown if any problem occurs while creating cache or test-region
   * 
   * @see AttributesFactory#setPublisher(boolean)
   * @see AttributesFactory#setEnableConflation(boolean)
   * @see AttributesFactory#setEnableAsyncConflation(boolean)
   */
  public static void createServerCache(Boolean enableWan, Boolean setPublisher,
      Boolean enableConflation, Boolean enableAsyncConflation) throws Exception
  {
    NewRegionAttributesDUnitTest test = new NewRegionAttributesDUnitTest("temp");
    cache = test.createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
//    factory.setPublisher(setPublisher.booleanValue());
    factory.setEnableConflation(enableConflation.booleanValue());
    factory.setEnableAsyncConflation(enableAsyncConflation.booleanValue());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
  }

  /**
   * Creates cache instance
   * 
   * @param props -
   *          properties of the distributed system
   * @return cache
   * @throws Exception -
   *           thrown if any problem occurs while connecting to distributed
   *           system or creating cache
   */
  private Cache createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    Cache cache = null;
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  /**
   * Closes the cache instance created and disconnects from the distributed
   * system.
   */
  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * This test configures a region on test VMs with newly added attributes
   * enabled and verifies that basic entry operations are properly working. The
   * test does the followings : <br>
   * 1)Verify on both the VMs that both the attributes are set properly<br>
   * 2)Perform PUTs from one VM and verify that all of them reach the other VM<br>
   * 3)Perform PUTs,INVALIDATEs and DESTROYs from one VM and verify at the end
   * that all are destroyed in the other VM also<br>
   * 
   * @see AttributesFactory#setPublisher(boolean)
   * @see AttributesFactory#setEnableConflation(boolean)
   * @see AttributesFactory#setEnableAsyncConflation(boolean)
   */
  public void testEntryOperationsWithNewAttributesEnabled()
  {
    vm0.invoke(NewRegionAttributesDUnitTest.class, "checkAttributes");
    vm1.invoke(NewRegionAttributesDUnitTest.class, "checkAttributes");
    vm0.invoke(NewRegionAttributesDUnitTest.class, "doPuts");
    Integer cnt1 = (Integer)vm1.invoke(NewRegionAttributesDUnitTest.class,
        "getEntryCount");
    assertEquals(TOTAL_PUTS, cnt1.intValue());
    vm0.invoke(NewRegionAttributesDUnitTest.class, "doPuts");
    vm0.invoke(NewRegionAttributesDUnitTest.class, "doInvalidates");
    vm0.invoke(NewRegionAttributesDUnitTest.class, "doDestroy");

    Integer cnt2 = (Integer)vm1.invoke(NewRegionAttributesDUnitTest.class,
        "getEntryCount");
    assertEquals(0, cnt2.intValue());
  }

  /**
   * This test tries to call register/unregister interest related API's on the
   * test-region (which does not have any bridge-client or bridge-server) and
   * verifies that <code>UnsupportedOperationException</code> occurs as expected
   */
  public void testRegisterInterestUseCases()
  {
    vm1.invoke(NewRegionAttributesDUnitTest.class, "registerInterest");
    vm1.invoke(NewRegionAttributesDUnitTest.class, "unregisterInterest");
    vm1.invoke(NewRegionAttributesDUnitTest.class, "getInterestForRegion");
  }

  /**
   * This methods verifies on both test VMs that enableWan, setPublisher,
   * enableConflation and enableAysncConflation are set properly to true for the
   * test-region
   */
  public static void checkAttributes()
  {
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
//    assertTrue(region1.getAttributes().getPublisher());
    assertTrue(region1.getAttributes().getEnableConflation());
    assertTrue(region1.getAttributes().getEnableAsyncConflation());
  }

  /**
   * Performs PUT operations on the test-region and fails if any Exception
   * occurs during the PUTs
   */
  public static void doPuts()
  {
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    for (int i = 0; i < TOTAL_PUTS; i++) {
      try {
        region1.put("key-" + i, "val-" + i);
      }
      catch (Exception e) {
        fail("Test failed due to unexpected exception during PUTs : " + e);
      }
    }
  }

  /**
   * Performs INVALIDATES operations on the test-region and fails if any
   * Exception occurs during the INVALIDATESs
   */
  public static void doInvalidates()
  {
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    for (int i = 0; i < TOTAL_PUTS; i++) {
      try {
        region1.invalidate("key-" + i);
      }
      catch (Exception e) {
        fail("Test failed due to unexpected exception during INVALIDATESs : "
            + e);
      }
    }
 }

  /**
   * Performs DESTROY operations on the test-region and fails if any Exception
   * occurs during the DESTROYs
   */
  public static void doDestroy()
  {
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    for (int i = 0; i < TOTAL_PUTS; i++) {
      try {
        region1.destroy("key-" + i);
      }
      catch (Exception e) {
        fail("Test failed due to unexpected exception during DESTROYs : " + e);
      }
    }
  }

  /**
   * Gets the total number of entries in the test region
   * 
   * @return total entries
   */
  public static Object getEntryCount()
  {
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    int keysSize = region1.entries(false).size();
    return new Integer(keysSize);
  }

  /**
   * This method invokes all the registerInterest related API's for the
   * test-region and verifies that <code>UnsupportedOperationException</code> is thrown
   * in all the cases since test-region does not have bridge-server or
   * bridge-client.
   * 
   * @see Region#registerInterest(Object)
   * @see Region#registerInterest(Object, InterestResultPolicy)
   * @see Region#registerInterestRegex(String)
   * @see Region#registerInterestRegex(String, InterestResultPolicy)
   */
  public static void registerInterest()
  {
    InterestResultPolicy policy = InterestResultPolicy.KEYS_VALUES;
    int totalKeys = 5;
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    List keylist = new ArrayList();
    for (int i = 0; i < totalKeys; i++) {
      keylist.add("key-" + i);
    }
    boolean exceptionOccured = false;

    // test registerInterest(key)
    try {
      region1.registerInterest("DummyKey1");
    }
    catch (UnsupportedOperationException expected) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for registerInterest(key)");
    }

    // test registerInterest(key,policy)
    exceptionOccured = false;
    try {
      region1.registerInterest("DummyKey2", policy);
    }
    catch (UnsupportedOperationException expected) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for  registerInterest(key,policy)");
    }

    // test registerInterest(keylist)
    exceptionOccured = false;
    try {
      region1.registerInterest(keylist);
    }
    catch (UnsupportedOperationException expected) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for registerInterest(keylist)");
    }

    // test registerInterest(keylist,policy)
    exceptionOccured = false;
    try {
      region1.registerInterest(keylist, policy);
    }
    catch (UnsupportedOperationException expected) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for registerInterest(keylist,policy)");
    }

    // test registerInterestRegex(expr)
    exceptionOccured = false;
    try {
      region1.registerInterestRegex("ke?");
    }
    catch (UnsupportedOperationException expected) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for registerInterestRegex(expr)");
    }

    // test registerInterestRegex(expr,policy)
    exceptionOccured = false;
    try {
      region1.registerInterestRegex("ke?", InterestResultPolicy.KEYS_VALUES);
    }
    catch (UnsupportedOperationException expected) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for registerInterestRegex(expr,policy)");
    }
  }

  /**
   * This method invokes all the unregisterInterest related API's for the
   * test-region and verifies that <code>UnsupportedOperationException</code> is thrown
   * in all the cases since test-region does not have bridge-server or
   * bridge-client.
   * 
   * @see Region#unregisterInterest(Object)
   * @see Region#unregisterInterestRegex(String)
   */
  public static void unregisterInterest()
  {
    int totalKeys = 5;
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    List keylist = new ArrayList();
    for (int i = 0; i < totalKeys; i++) {
      keylist.add("key-" + i);
    }

    // test unregisterInterest(key)
    boolean exceptionOccured = false;
    try {
      region1.unregisterInterest("DummyKey1");
    }
    catch (UnsupportedOperationException expected) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for unregisterInterest(key)");
    }

    // test unregisterInterest(keylist)
    exceptionOccured = false;
    try {
      region1.unregisterInterest(keylist);
    }
    catch (UnsupportedOperationException expected) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for unregisterInterest(keylist)");
    }

    // test unregisterInterestRegex(expr)
    exceptionOccured = false;
    try {
      region1.unregisterInterestRegex("kp?");
    }
    catch (UnsupportedOperationException expected) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for unregisterInterestRegex(expr)");
    }
  }

  /**
   * This method invokes all the getter API's for interest-list on the
   * test-region and verifies that <code>UnsupportedOperationException</code> is thrown
   * in all the cases since test-region does not have bridge-server or
   * bridge-client.
   * 
   * @see Region#getInterestList()
   * @see Region#getInterestListRegex()
   */
  public static void getInterestForRegion()
  {
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    boolean exceptionOccured = false;

    // test getInterestList()
    try {
      region1.getInterestList();
    }
    catch (UnsupportedOperationException e) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for getInterestList()");
    }

    // test getInterestListRegex()

    exceptionOccured = false;
    try {
      region1.getInterestListRegex();
    }
    catch (UnsupportedOperationException e) {
      exceptionOccured = true;
    }
    finally {
      if (!exceptionOccured)
        fail("UnsupportedOperationException was not thrown as expected for getInterestListRegex()");
    }
  }
}
