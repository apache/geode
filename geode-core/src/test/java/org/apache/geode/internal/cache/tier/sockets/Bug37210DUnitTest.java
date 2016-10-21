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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This tests the fix for bug 73210. Reason for the bug was that HARegionQueue's destroy was not
 * being called on CacheClientProxy's closure. As a result, stats were left open.
 */
@Category(DistributedTest.class)
public class Bug37210DUnitTest extends JUnit4DistributedTestCase {

  /** the cache server */
  VM server = null;

  /** the cache client */
  VM client = null;

  /** the cache */
  private static Cache cache = null;

  /** port for the cache server */
  private int PORT;

  /** name of the test region */
  private static final String REGION_NAME = "Bug37210DUnitTest_Region";

  /**
   * Creates the cache server and sets the port
   * 
   * @throws Exception - thrown if any problem occurs in initializing the test
   */
  @Override
  public final void postSetUp() throws Exception {
    IgnoredException.addIgnoredException("java.io.IOException");

    final Host host = Host.getHost(0);
    server = host.getVM(0);
    client = host.getVM(2);
    PORT = ((Integer) server.invoke(() -> Bug37210DUnitTest.createServerCache())).intValue();
  }

  /**
   * Create the cache
   * 
   * @param props - properties for DS
   * @return the cache instance
   * @throws Exception - thrown if any problem occurs in cache creation
   */
  private Cache createCache(Properties props) throws Exception {
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
   * close the cache instances in server and client during tearDown
   * 
   * @throws Exception thrown if any problem occurs in closing cache
   */
  @Override
  public final void preTearDown() throws Exception {
    // close client
    client.invoke(() -> Bug37210DUnitTest.closeCache());

    // close server
    server.invoke(() -> Bug37210DUnitTest.closeCache());
  }

  /**
   * This test does the following:<br>
   * 1)Create the client<br>
   * 2)Do some operations from the cache-server<br>
   * 3)Stop the primary cache-server<br>
   * 4)Explicity close the CacheClientProxy on the server. <br>
   * 5)Verify that HARegionQueue stats are closed and entry for the haregion is removed from
   * dispatchedMessagesMap.
   * 
   * @throws Exception - thrown if any problem occurs in test execution
   */
  @Test
  public void testHAStatsCleanup() throws Exception {
    Host host = Host.getHost(0);
    LogWriterUtils.getLogWriter().info("testHAStatsCleanup : BEGIN");
    IgnoredException.addIgnoredException("java.net.SocketException");
    IgnoredException.addIgnoredException("Unexpected IOException");
    client.invoke(() -> Bug37210DUnitTest.createClientCache(NetworkUtils.getServerHostName(host),
        new Integer(PORT)));
    server.invoke(() -> Bug37210DUnitTest.doEntryOperations());

    server.invoke(() -> Bug37210DUnitTest.closeCacheClientProxyAndVerifyStats());
    client.invoke(() -> Bug37210DUnitTest.closeCache());
    // we don't send close response thus needs to wait for client termination
    Thread.currentThread().sleep(1000);
    server.invoke(() -> Bug37210DUnitTest.closeCacheClientProxyAndVerifyStats2());
    LogWriterUtils.getLogWriter().info("testHAStatsCleanup : END");
  }

  /**
   * Creates and starts the cache-server
   * 
   * @return - the port on which cache-server is running
   * @throws Exception - thrown if any problem occurs in cache/server creation
   */
  public static Integer createServerCache() throws Exception {
    Bug37210DUnitTest test = new Bug37210DUnitTest();
    Properties props = new Properties();
    cache = test.createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    RegionAttributes attrs = factory.create();

    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(false);
    server.setSocketBufferSize(32768);
    server.setMaximumTimeBetweenPings(1000000);
    server.start();
    LogWriterUtils.getLogWriter().info("Server started at PORT = " + port);
    return new Integer(server.getPort());
  }

  /**
   * Initializes the cache client
   * 
   * @param port - port for the primary cache-server
   * 
   * @throws Exception-thrown if any problem occurs in initializing the client
   */
  public static void createClientCache(String host, Integer port) throws Exception {
    Bug37210DUnitTest test = new Bug37210DUnitTest();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    cache = test.createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port.intValue())
        .setSubscriptionEnabled(true).setThreadLocalConnections(true).setReadTimeout(10000)
        .setSocketBufferSize(32768).setMinConnections(3).setSubscriptionRedundancy(-1)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("Bug37210UnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);

    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    region.registerInterest("ALL_KEYS");
  }

  /**
   * Close the CacheClientProxy of the client on the server and verify that ha-stats are closed and
   * the entry for the region is removed from dispatchedMessagesMap.
   * 
   */
  public static void closeCacheClientProxyAndVerifyStats() {
    assertEquals("More than one BridgeServers found ", 1, cache.getCacheServers().size());
    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bs);
    assertNotNull(bs.getAcceptor());
    assertNotNull(bs.getAcceptor().getCacheClientNotifier());
    Iterator proxies = bs.getAcceptor().getCacheClientNotifier().getClientProxies().iterator();
    assertTrue("No proxy found", proxies.hasNext());
    CacheClientProxy proxy = (CacheClientProxy) proxies.next();
    Map dispatchedMsgMap = HARegionQueue.getDispatchedMessagesMapForTesting();
    HARegionQueue rq = proxy.getHARegionQueue();
    Object value = dispatchedMsgMap.get(rq.getRegion().getName());
    proxy.close();

    assertTrue("HARegionQueue stats were not closed on proxy.close()",
        rq.getStatistics().isClosed());

  }

  public static void closeCacheClientProxyAndVerifyStats2() {
    Map dispatchedMsgMap = HARegionQueue.getDispatchedMessagesMapForTesting();
    assertTrue(
        "HARegionQueue.dispatchedMessagesMap contains entry for the region even after proxy.close()",
        dispatchedMsgMap.size() == 0);
  }

  /**
   * Do some PUT operations
   * 
   * @throws Exception - thrown if any exception occurs in doing PUTs
   */
  public static void doEntryOperations() throws Exception {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    String keyPrefix = "server-";
    for (int i = 0; i < 10; i++) {
      r1.put(keyPrefix + i, keyPrefix + "val-" + i);
    }
  }

  /**
   * Close the cache
   * 
   */
  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

}
