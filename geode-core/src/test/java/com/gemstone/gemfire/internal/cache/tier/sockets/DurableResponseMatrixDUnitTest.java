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

import java.util.Properties;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;

/**
 * 
 * Tests that the Matris defined in <code>ServerResponseMatrix</code> is
 * applied or not
 * 
 * @author Yogesh Mahajan
 * @since 5.1
 * 
 */
public class DurableResponseMatrixDUnitTest extends DistributedTestCase
{

  protected static Cache cache = null;

  VM server1 = null;

  private static Integer PORT1;

  private static final String REGION_NAME = "DurableResponseMatrixDUnitTest_region";
  
  public static final String KEY = "KeyMatrix1" ;

  /** constructor */
  public DurableResponseMatrixDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    // start servers first
    PORT1 = ((Integer)server1.invoke(DurableResponseMatrixDUnitTest.class,
        "createServerCache"));
    createCacheClient(NetworkUtils.getServerHostName(server1.getHost()));
    //Disconnecting the client can cause this
    IgnoredException.addIgnoredException("Connection reset||Unexpected IOException");
  }

  public void testRegisterInterestResponse_NonExistent_Invalid()
      throws Exception
  {
    server1.invoke(DurableResponseMatrixDUnitTest.class, "invalidateEntry",
        new Object[] { KEY });
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertFalse(r.containsValueForKey(KEY)); // invalidate
    assertEquals(null, r.getEntry(KEY).getValue()); // invalidate
  }

  public void testRegisterInterestResponse_NonExistent_Valid() throws Exception
  {
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }

  public void BROKEN_testRegisterInterestResponse_Valid_Invalid() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "ValueMatrix1");
    server1.invoke(DurableResponseMatrixDUnitTest.class, "invalidateEntry",
        new Object[] { KEY });
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }

  public void testRegisterInterestResponse_Valid_Valid() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "ValueMatrix1");
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }

  public void testRegisterInterestResponse_Invalid_Invalid() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "invalidateEntry",
        new Object[] { KEY });
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals(null, r.getEntry(KEY).getValue());
  }

  public void BROKEN_testRegisterInterestResponse_Invalid_Valid()
      throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }

  public void testRegisterInterestResponse_Destroyed_Invalid() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.destroy(KEY);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "invalidateEntry",
        new Object[] { KEY });
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertFalse(r.containsValueForKey(KEY)); // invalidate
    assertEquals(null, r.getEntry(KEY).getValue()); // invalidate
  }

  public void testRegisterInterestResponse_Destroyed_Valid() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.destroy(KEY);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }
  public void testRegisterInterest_Destroy_Concurrent() throws Exception
  {  
	PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = true;
	ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
		public void beforeInterestRegistration()
	    {	          
	      	Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);	  
	      	r.put(KEY, "AgainDummyValue");
	      	r.destroy(KEY);
	        PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
	    }
	});
		
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
          new Object[] { KEY, "ValueMatrix1" });

    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);      
    assertEquals(null, r.getEntry(KEY));   
  }

  private void waitForValue(final Region r, final Object key, final Object expected) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        com.gemstone.gemfire.cache.Region.Entry entry = r.getEntry(KEY);
        if (expected == null) {
          if (!r.containsValueForKey(key)) {
            return true; // success!
          }
        }
        else {
          if (entry != null) {
            if (expected.equals(entry.getValue())) {
              return true;
            }
          }
        }
        return false;
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 120 * 1000, 200, true);
  }
  
  public void testNotification_NonExistent_Create() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    waitForValue(r, KEY, "ValueMatrix1");
  }

  public void testNotification_NonExistent_Update() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix2" });
    waitForValue(r, KEY, "ValueMatrix2");
  }

  public void testNotification_NonExistent_Invalid() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "invalidateEntryOnly",
        new Object[] { KEY });
    waitForValue(r, KEY, null); // invalidate
  }

  public void testNotification_NonExistent_Destroy() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "destroyEntry",
        new Object[] { KEY });
    waitForValue(r, KEY, null); // destroyed
  }

  public void testNotification_Valid_Create() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    waitForValue(r, KEY, "ValueMatrix1");
  }

  public void testNotification_Valid_Update() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix2" });
    waitForValue(r, KEY, "ValueMatrix2");
  }

  public void BROKEN_testNotification_Valid_Invalid() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "invalidateEntryOnly",
        new Object[] { KEY });
    waitForValue(r, KEY, null); // invalidate
  }

  public void testNotification_Valid_Destroy() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "destroyEntry",
        new Object[] { KEY });
    waitForValue(r, KEY, null); // destroyed
  }

  public void testNotification_Invalid_Create() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    waitForValue(r, KEY, "ValueMatrix1");
  }

  public void testNotification_Invalid_Update() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    waitForValue(r, KEY, "ValueMatrix1");
  }

  public void BROKEN_testNotification_Invalid_Invalid() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "invalidateEntryOnly",
        new Object[] { KEY });
    waitForValue(r, KEY, null); // invalidate
  }

  public void testNotification_Invalid_Destroy() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "destroyEntry",
        new Object[] { KEY });
    waitForValue(r, KEY, null); // destroyed
  }

  public void testNotification_LocalInvalid_Create() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    localInvalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    waitForValue(r, KEY, "ValueMatrix1");
  }

  public void testNotification_LocalInvalid_Update() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    localInvalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "updateEntry",
        new Object[] { KEY, "ValueMatrix1" });
    waitForValue(r, KEY, "ValueMatrix1");
  }

  public void BROKEN_testNotification_LocalInvalid_Invalid() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    localInvalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "invalidateEntryOnly",
        new Object[] { KEY });
    waitForValue(r, KEY, null); // invalidate
  }

  public void testNotification_LocalInvalid_Destroy() throws Exception
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(DurableResponseMatrixDUnitTest.class, "destroyEntry",
        new Object[] { KEY });
    waitForValue(r, KEY, null); // destroyed
  }

  public static void updateEntry(String key, String value) throws Exception
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.put(key, value);
    }
    catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  public static void destroyEntry(String key) throws Exception
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.destroy(key);
    }
    catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  public static void invalidateEntryOnly(String key) throws Exception
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.invalidate(key);
    }
    catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  public static void invalidateEntry(String key) throws Exception
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.put(key, "DummyValue");
      r.invalidate(key);
    }
    catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  public static void localInvalidateEntry(String key) throws Exception
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.put(key, "DummyValue");
      r.localInvalidate(key);
    }
    catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  private void createCache(Properties props)
  {
    try {
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    }
    catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  private void createCacheClient(String host)
  {
    try {
      final String durableClientId = "DurableResponseMatrixDUnitTest_client";
      final int durableClientTimeout = 60; // keep the client alive for 60 s
      Properties props = getClientDistributedSystemProperties(durableClientId,
          durableClientTimeout);
      new DurableResponseMatrixDUnitTest("temp").createCache(props);
      Pool p = PoolManager.createFactory()
        .addServer(host, PORT1.intValue())
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(1)
        .setReadTimeout(10000)
        .setMinConnections(2)
        // .setRetryInterval(2000)
        .create("DurableResponseMatrixDUnitTestPool");

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setPoolName(p.getName());

      RegionAttributes attrs = factory.create();
      Region r = cache.createRegion(REGION_NAME, attrs);
      assertNotNull(r);

      cache.readyForEvents();

    }
    catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }

  }

  public static Integer createServerCache() throws Exception
  {
    Properties props = new Properties();
    new DurableResponseMatrixDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    Region r = cache.createRegion(REGION_NAME, attrs);
    assertNotNull(r);
    CacheServer server1 = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  private Properties getClientDistributedSystemProperties(
      String durableClientId, int durableClientTimeout)
  {
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.setProperty(DistributionConfig.LOCATORS_NAME, "");
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME,
        durableClientId);
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME,
        String.valueOf(durableClientTimeout));
    return properties;
  }

  @Override
  protected final void preTearDown() throws Exception {
    // close the clients first
    closeCache();
    // then close the servers
    server1.invoke(DurableResponseMatrixDUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
