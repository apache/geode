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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.cache.Region.Entry;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Tests that the Matris defined in <code>ServerResponseMatrix</code> is applied or not
 *
 * @since GemFire 5.1
 */
@Category({ClientSubscriptionTest.class})
public class DurableResponseMatrixDUnitTest extends JUnit4DistributedTestCase {

  protected static Cache cache = null;

  VM server1 = null;

  private static Integer PORT1;

  private static final String REGION_NAME = "DurableResponseMatrixDUnitTest_region";

  public static final String KEY = "KeyMatrix1";

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    // start servers first
    PORT1 = ((Integer) server1.invoke(() -> DurableResponseMatrixDUnitTest.createServerCache()));
    createCacheClient(NetworkUtils.getServerHostName(server1.getHost()));
    // Disconnecting the client can cause this
    IgnoredException.addIgnoredException("Connection reset||Unexpected IOException");
  }

  @Test
  public void testRegisterInterestResponse_NonExistent_Invalid() throws Exception {
    server1.invoke(() -> DurableResponseMatrixDUnitTest.invalidateEntry(KEY));
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertFalse(r.containsValueForKey(KEY)); // invalidate
    assertEquals(null, r.getEntry(KEY).getValue()); // invalidate
  }

  @Test
  public void testRegisterInterestResponse_NonExistent_Valid() throws Exception {
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }

  @Ignore("TODO: test is broken and disabled")
  @Test
  public void testRegisterInterestResponse_Valid_Invalid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "ValueMatrix1");
    server1.invoke(() -> DurableResponseMatrixDUnitTest.invalidateEntry(KEY));
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }

  @Test
  public void testRegisterInterestResponse_Valid_Valid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "ValueMatrix1");
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }

  @Test
  public void testRegisterInterestResponse_Invalid_Invalid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.invalidateEntry(KEY));
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals(null, r.getEntry(KEY).getValue());
  }

  @Ignore("TODO: test is broken and disabled")
  @Test
  public void testRegisterInterestResponse_Invalid_Valid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }

  @Test
  public void testRegisterInterestResponse_Destroyed_Invalid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.destroy(KEY);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.invalidateEntry(KEY));
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertFalse(r.containsValueForKey(KEY)); // invalidate
    assertEquals(null, r.getEntry(KEY).getValue()); // invalidate
  }

  @Test
  public void testRegisterInterestResponse_Destroyed_Valid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.destroy(KEY);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals("ValueMatrix1", r.getEntry(KEY).getValue());
  }

  @Test
  public void testRegisterInterest_Destroy_Concurrent() throws Exception {
    PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void beforeInterestRegistration() {
        Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        r.put(KEY, "AgainDummyValue");
        r.destroy(KEY);
        PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
      }
    });

    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));

    r.registerInterest(KEY, InterestResultPolicy.KEYS_VALUES);
    assertEquals(null, r.getEntry(KEY));
  }

  private void waitForValue(final Region r, final Object key, final Object expected) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        Entry entry = r.getEntry(KEY);
        if (expected == null) {
          if (!r.containsValueForKey(key)) {
            return true; // success!
          }
        } else {
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
    GeodeAwaitility.await().untilAsserted(ev);
  }

  @Test
  public void testNotification_NonExistent_Create() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    waitForValue(r, KEY, "ValueMatrix1");
  }

  @Test
  public void testNotification_NonExistent_Update() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix2"));
    waitForValue(r, KEY, "ValueMatrix2");
  }

  @Test
  public void testNotification_NonExistent_Invalid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.invalidateEntryOnly(KEY));
    waitForValue(r, KEY, null); // invalidate
  }

  @Test
  public void testNotification_NonExistent_Destroy() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.destroyEntry(KEY));
    waitForValue(r, KEY, null); // destroyed
  }

  @Test
  public void testNotification_Valid_Create() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    waitForValue(r, KEY, "ValueMatrix1");
  }

  @Test
  public void testNotification_Valid_Update() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix2"));
    waitForValue(r, KEY, "ValueMatrix2");
  }

  @Ignore("TODO: test is broken and disabled")
  @Test
  public void testNotification_Valid_Invalid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.invalidateEntryOnly(KEY));
    waitForValue(r, KEY, null); // invalidate
  }

  @Test
  public void testNotification_Valid_Destroy() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.put(KEY, "DummyValue");
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.destroyEntry(KEY));
    waitForValue(r, KEY, null); // destroyed
  }

  @Test
  public void testNotification_Invalid_Create() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    waitForValue(r, KEY, "ValueMatrix1");
  }

  @Test
  public void testNotification_Invalid_Update() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    waitForValue(r, KEY, "ValueMatrix1");
  }

  @Ignore("TODO: test is broken and disabled")
  @Test
  public void testNotification_Invalid_Invalid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.invalidateEntryOnly(KEY));
    waitForValue(r, KEY, null); // invalidate
  }

  @Test
  public void testNotification_Invalid_Destroy() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.destroyEntry(KEY));
    waitForValue(r, KEY, null); // destroyed
  }

  @Test
  public void testNotification_LocalInvalid_Create() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    localInvalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    waitForValue(r, KEY, "ValueMatrix1");
  }

  @Test
  public void testNotification_LocalInvalid_Update() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    localInvalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.updateEntry(KEY, "ValueMatrix1"));
    waitForValue(r, KEY, "ValueMatrix1");
  }

  @Ignore("TODO: test is broken and disabled")
  @Test
  public void testNotification_LocalInvalid_Invalid() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    localInvalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.invalidateEntryOnly(KEY));
    waitForValue(r, KEY, null); // invalidate
  }

  @Test
  public void testNotification_LocalInvalid_Destroy() throws Exception {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    invalidateEntry(KEY);
    r.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    server1.invoke(() -> DurableResponseMatrixDUnitTest.destroyEntry(KEY));
    waitForValue(r, KEY, null); // destroyed
  }

  public static void updateEntry(String key, String value) throws Exception {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.put(key, value);
    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  public static void destroyEntry(String key) throws Exception {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.destroy(key);
    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  public static void invalidateEntryOnly(String key) throws Exception {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.invalidate(key);
    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  public static void invalidateEntry(String key) throws Exception {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.put(key, "DummyValue");
      r.invalidate(key);
    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  public static void localInvalidateEntry(String key) throws Exception {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.put(key, "DummyValue");
      r.localInvalidate(key);
    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  private void createCache(Properties props) {
    try {
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  private void createCacheClient(String host) {
    try {
      final String durableClientId = "DurableResponseMatrixDUnitTest_client";
      final int durableClientTimeout = 60; // keep the client alive for 60 s
      Properties props =
          getClientDistributedSystemProperties(durableClientId, durableClientTimeout);
      new DurableResponseMatrixDUnitTest().createCache(props);
      Pool p =
          PoolManager.createFactory().addServer(host, PORT1.intValue()).setSubscriptionEnabled(true)
              .setSubscriptionRedundancy(1).setReadTimeout(10000).setMinConnections(2)
              // .setRetryInterval(2000)
              .create("DurableResponseMatrixDUnitTestPool");

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setPoolName(p.getName());

      RegionAttributes attrs = factory.create();
      Region r = cache.createRegion(REGION_NAME, attrs);
      assertNotNull(r);

      cache.readyForEvents();

    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }

  }

  public static Integer createServerCache() throws Exception {
    Properties props = new Properties();
    new DurableResponseMatrixDUnitTest().createCache(props);
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

  private Properties getClientDistributedSystemProperties(String durableClientId,
      int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return properties;
  }

  @Override
  public final void preTearDown() throws Exception {
    // close the clients first
    closeCache();
    // then close the servers
    server1.invoke(() -> DurableResponseMatrixDUnitTest.closeCache());
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
