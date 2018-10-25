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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOVE_UNRESPONSIVE_CLIENT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.SocketException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
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
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class HASlowReceiverDUnitTest extends JUnit4DistributedTestCase {

  protected static Cache cache = null;

  private static VM serverVM1 = null;

  protected static VM serverVM2 = null;

  protected static VM clientVM = null;

  private int PORT0;

  private int PORT1;

  private int PORT2;

  private static final String regionName = "HASlowReceiverDUnitTest";

  protected static LogWriter logger = null;

  static PoolImpl pool = null;

  private static boolean isUnresponsiveClientRemoved = false;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    serverVM1 = host.getVM(1);
    serverVM2 = host.getVM(2);
    clientVM = host.getVM(3);

    PORT0 = createServerCache().intValue();
    PORT1 =
        ((Integer) serverVM1.invoke(() -> HASlowReceiverDUnitTest.createServerCache())).intValue();
    PORT2 =
        ((Integer) serverVM2.invoke(() -> HASlowReceiverDUnitTest.createServerCache())).intValue();
  }

  @Override
  public final void preTearDown() throws Exception {
    clientVM.invoke(() -> HASlowReceiverDUnitTest.closeCache());

    // then close the servers
    closeCache();
    serverVM1.invoke(() -> HASlowReceiverDUnitTest.closeCache());
    serverVM2.invoke(() -> HASlowReceiverDUnitTest.closeCache());
    disconnectAllFromDS();
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static Integer createServerCache() throws Exception {
    return createServerCache(null);
  }

  public static Integer createServerCache(String ePolicy) throws Exception {
    return createServerCache(ePolicy, new Integer(1));
  }

  public static Integer createServerCache(String ePolicy, Integer cap) throws Exception {

    Properties prop = new Properties();
    prop.setProperty(REMOVE_UNRESPONSIVE_CLIENT, "true");
    new HASlowReceiverDUnitTest().createCache(prop);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    logger = cache.getLogger();

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.setMaximumMessageCount(200);
    if (ePolicy != null) {
      server1.getClientSubscriptionConfig().setEvictionPolicy(ePolicy);
      server1.getClientSubscriptionConfig().setCapacity(cap.intValue());
    }
    server1.start();
    return new Integer(server1.getPort());
  }

  public static void createClientCache(String host, Integer port1, Integer port2, Integer port3,
      Integer rLevel, Boolean addListener) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HASlowReceiverDUnitTest().createCache(props);

    AttributesFactory factory = new AttributesFactory();
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer("localhost", port1)
        .addServer("localhost", port2).addServer("localhost", port3).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(rLevel.intValue()).setThreadLocalConnections(true)
        .setMinConnections(6).setReadTimeout(20000).setPingInterval(1000).setRetryAttempts(5)
        .create("HASlowReceiverDUnitTestPool");

    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    if (addListener.booleanValue()) {
      factory.addCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterUpdate(EntryEvent event) {

          if (event.getNewValue().equals("v20")) {
            try {
              Thread.sleep(120000);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }
      });
    }
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    pool = p;
  }

  public static void createClientCache(String host, Integer port1, Integer port2, Integer port3,
      Integer rLevel) throws Exception {
    createClientCache(host, port1, port2, port3, rLevel, Boolean.TRUE);
  }

  public static void registerInterest() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("ALL_KEYS");
    } catch (Exception ex) {
      Assert.fail("failed in registerInterestListAll", ex);
    }
  }

  public static void putEntries() {
    try {

      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      for (long i = 0; i < 300; i++) {
        r.put("k" + (i % 10), "v" + i);
        r.put("k" + (i % 10), new byte[1000]);
      }
    } catch (Exception ex) {
      Assert.fail("failed in putEntries()", ex);
    }
  }

  public static void createEntries(Long num) {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      for (long i = 0; i < num.longValue(); i++) {
        r.create("k" + i, "v" + i);
      }
    } catch (Exception ex) {
      Assert.fail("failed in createEntries(Long)", ex);
    }
  }

  public static void checkRedundancyLevel(final Integer redundantServers) {
    await().untilAsserted(() -> {
      // check for slow client queue is removed or not.
      assertTrue(
          "Expected redundant count (" + pool.getRedundantNames().size() + ") to become "
              + redundantServers.intValue(),
          pool.getRedundantNames().size() == redundantServers.intValue());
    });
  }

  // Test slow client
  @Test
  public void testSlowClient() throws Exception {
    setBridgeObserverForAfterQueueDestroyMessage();
    Host host = Host.getHost(0);
    clientVM.invoke(
        () -> HASlowReceiverDUnitTest.createClientCache(NetworkUtils.getServerHostName(host),
            new Integer(PORT0), new Integer(PORT1), new Integer(PORT2), new Integer(2)));
    clientVM.invoke(() -> HASlowReceiverDUnitTest.registerInterest());
    // add expected socket exception string
    final IgnoredException ex1 =
        IgnoredException.addIgnoredException(SocketException.class.getName());
    final IgnoredException ex2 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());

    putEntries();

    await().untilAsserted(() -> {
      // check for slow client queue is removed or not.
      assertTrue("isUnresponsiveClientRemoved is false, but should be true " + "after 60 seconds",
          isUnresponsiveClientRemoved);
    });

    // verify that we get reconnected
    clientVM.invoke(() -> HASlowReceiverDUnitTest.checkRedundancyLevel(new Integer(2)));

    ex1.remove();
    ex2.remove();
  }

  public static void setBridgeObserverForAfterQueueDestroyMessage() throws Exception {
    PoolImpl.AFTER_QUEUE_DESTROY_MESSAGE_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      @Override
      public void afterQueueDestroyMessage() {
        clientVM.invoke(() -> HASlowReceiverDUnitTest.checkRedundancyLevel(new Integer(0)));
        isUnresponsiveClientRemoved = true;
        PoolImpl.AFTER_QUEUE_DESTROY_MESSAGE_FLAG = false;
      }
    });
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

}
