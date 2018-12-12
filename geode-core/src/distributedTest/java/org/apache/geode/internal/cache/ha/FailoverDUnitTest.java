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

import static org.apache.geode.cache.client.PoolManager.find;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;

import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.cache.tier.sockets.ConflationDUnitTestHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Dunit test to verify HA feature. Have 2 nodes S1 & S2. Client is connected to S1 & S2 with S1 as
 * the primary end point. Do some puts on S1 .The expiry is on high side. Stop S1 , the client is
 * failing to S2.During fail over duration do some puts on S1. The client on failing to S2 may
 * receive duplicate events but should not miss any events.
 */
@Category({ClientSubscriptionTest.class})
public class FailoverDUnitTest extends JUnit4DistributedTestCase {

  protected static Cache cache = null;
  // server
  private static VM vm0 = null;
  private static VM vm1 = null;
  protected static VM primary = null;

  private static int PORT1;
  private static int PORT2;

  private static final String regionName = "interestRegion";

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);

    // start servers first
    vm0.invoke(() -> ConflationDUnitTestHelper.unsetIsSlowStart());
    vm1.invoke(() -> ConflationDUnitTestHelper.unsetIsSlowStart());
    PORT1 = ((Integer) vm0.invoke(() -> FailoverDUnitTest.createServerCache())).intValue();
    PORT2 = ((Integer) vm1.invoke(() -> FailoverDUnitTest.createServerCache())).intValue();

    CacheServerTestUtil.disableShufflingOfEndpoints();
    createClientCache(NetworkUtils.getServerHostName(host), new Integer(PORT1), new Integer(PORT2));
    { // calculate the primary vm
      waitForPrimaryAndBackups(1);
      PoolImpl pool = (PoolImpl) PoolManager.find("FailoverPool");
      if (pool.getPrimaryPort() == PORT1) {
        primary = vm0;
      } else {
        assertEquals(PORT2, pool.getPrimaryPort());
        primary = vm1;
      }
    }
  }

  @Test
  public void testFailover() {
    createEntries();
    waitForPrimaryAndBackups(1);
    registerInterestList();
    primary.invoke(() -> FailoverDUnitTest.put());
    verifyEntries();
    setClientServerObserver();
    primary.invoke(() -> FailoverDUnitTest.stopServer());
    verifyEntriesAfterFailover();
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String hostName, Integer port1, Integer port2)
      throws Exception {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new FailoverDUnitTest().createCache(props);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ClientServerTestCase.configureConnectionPoolWithName(factory, hostName,
        new int[] {PORT1, PORT2}, true, -1, 2, null, "FailoverPool");
    factory.setCacheListener(new CacheListenerAdapter() {
      public void afterUpdate(EntryEvent event) {
        synchronized (this) {
          cache.getLogger().info("Event Received : key..." + event.getKey());
          cache.getLogger().info("Event Received : value..." + event.getNewValue());
        }
      }
    });
    cache.createRegion(regionName, factory.create());
  }

  public static Integer createServerCache() throws Exception {
    new FailoverDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  public void waitForPrimaryAndBackups(final int numBackups) {
    final PoolImpl pool = (PoolImpl) find("FailoverPool");
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        if (pool.getPrimary() == null) {
          return false;
        }
        if (pool.getRedundants().size() < numBackups) {
          return false;
        }
        return true;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    assertNotNull(pool.getPrimary());
    assertTrue("backups=" + pool.getRedundants() + " expected=" + numBackups,
        pool.getRedundants().size() >= numBackups);
  }

  public static void registerInterestList() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("key-1");
      r.registerInterest("key-2");
      r.registerInterest("key-3");
      r.registerInterest("key-4");
      r.registerInterest("key-5");
    } catch (Exception ex) {
      Assert.fail("failed while registering keys k1 to k5", ex);
    }
  }

  public static void createEntries() {
    try {

      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.create("key-1", "key-1");
      r.create("key-2", "key-2");
      r.create("key-3", "key-3");
      r.create("key-4", "key-4");
      r.create("key-5", "key-5");
    } catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  public static void stopServer() {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer) iter.next();
        server.stop();
      }
    } catch (Exception e) {
      fail("failed while stopServer()", e);
    }
  }

  public static void put() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.put("key-1", "value-1");
      r.put("key-2", "value-2");
      r.put("key-3", "value-3");

    } catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  public void verifyEntries() {
    final Region r = cache.getRegion("/" + regionName);
    assertNotNull(r);
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return !r.getEntry("key-3").getValue().equals("key-3");
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);

    assertEquals("value-1", r.getEntry("key-1").getValue());
    assertEquals("value-2", r.getEntry("key-2").getValue());
    assertEquals("value-3", r.getEntry("key-3").getValue());
  }

  public static void setClientServerObserver() {
    PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void beforePrimaryIdentificationFromBackup() {
        primary.invoke(() -> FailoverDUnitTest.putDuringFailover());
        PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
      }
    });
  }

  public static void putDuringFailover() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.put("key-4", "value-4");
      r.put("key-5", "value-5");

    } catch (Exception ex) {
      Assert.fail("failed while r.putDuringFailover()", ex);
    }
  }

  public void verifyEntriesAfterFailover() {
    final Region r = cache.getRegion("/" + regionName);
    assertNotNull(r);
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return !r.getEntry("key-5").getValue().equals("key-5");
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    assertEquals("value-5", r.getEntry("key-5").getValue());
    assertEquals("value-4", r.getEntry("key-4").getValue());
  }

  @Override
  public final void preTearDown() throws Exception {
    // close the clients first
    closeCache();
    // then close the servers
    vm0.invoke(() -> FailoverDUnitTest.closeCache());
    vm1.invoke(() -> FailoverDUnitTest.closeCache());
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
      cache = null;
    }
  }
}
