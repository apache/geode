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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This is Dunit test for bug 36109. This test has a cache-client having a primary and a secondary
 * cache-server as its endpoint. Primary does some operations and is stopped, the client fails over
 * to secondary and does some operations and it is verified that the 'invalidates' stats at the
 * client is same as the total number of operations done by both primary and secondary. The bug was
 * appearing because invalidate stats was part of Endpoint which used to get closed during fail over
 * , with the failed endpoint getting closed. This bug has been fixed by moving the invalidate stat
 * to be part of our implementation.
 */
@Category({ClientSubscriptionTest.class})
@Ignore("Test was disabled by renaming to DisabledTest")
public class StatsBugDUnitTest extends JUnit4DistributedTestCase {

  /** primary cache server */
  VM primary = null;

  /** secondary cache server */
  VM secondary = null;

  /** the cache client */
  VM client1 = null;

  /** the cache */
  private static Cache cache = null;

  /** port for the primary cache server */
  private static int PORT1;

  /** port for the secondary cache server */
  private static int PORT2;

  /** name of the test region */
  private static final String REGION_NAME = StatsBugDUnitTest.class.getSimpleName() + "_Region";

  /** brige-writer instance( used to get connection proxy handle) */
  private static PoolImpl pool = null;

  /** total number of cache servers */
  private static final int TOTAL_SERVERS = 2;

  /** number of puts done by each server */
  private static final int PUTS_PER_SERVER = 10;

  /** prefix added to the keys of events generated on primary */
  private static final String primaryPrefix = "primary_";

  /** prefix added to the keys of events generated on secondary */
  private static final String secondaryPrefix = "secondary_";

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    primary = host.getVM(0);
    secondary = host.getVM(1);
    client1 = host.getVM(2);
    PORT1 = primary.invoke(() -> StatsBugDUnitTest.createServerCache()).intValue();
    PORT2 = secondary.invoke(() -> StatsBugDUnitTest.createServerCache()).intValue();
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

  @Override
  public final void preTearDown() throws Exception {
    // close client
    client1.invoke(() -> StatsBugDUnitTest.closeCache());

    // close server
    primary.invoke(() -> StatsBugDUnitTest.closeCache());
    secondary.invoke(() -> StatsBugDUnitTest.closeCache());
  }

  /**
   * This test does the following:<br>
   * 1)Create and populate the client<br>
   * 2)Do some operations from the primary cache-server<br>
   * 3)Stop the primary cache-server<br>
   * 4)Wait some time to allow client to failover to secondary and do some operations from
   * secondary<br>
   * 5)Verify that the invalidates stats at the client accounts for the operations done by both,
   * primary and secondary.
   */
  @Test
  public void testBug36109() throws Exception {
    LogWriterUtils.getLogWriter().info("testBug36109 : BEGIN");
    client1.invoke(() -> StatsBugDUnitTest.createClientCacheForInvalidates(
        NetworkUtils.getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2)));
    client1.invoke(() -> StatsBugDUnitTest.prepopulateClient());
    primary.invoke(() -> StatsBugDUnitTest.doEntryOperations(primaryPrefix));
    Wait.pause(3000);
    primary.invoke(() -> StatsBugDUnitTest.stopServer());
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }

    secondary.invoke(() -> StatsBugDUnitTest.doEntryOperations(secondaryPrefix));
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }

    client1.invoke(() -> StatsBugDUnitTest.verifyNumInvalidates());
    LogWriterUtils.getLogWriter().info("testBug36109 : END");
  }

  /**
   * Creates and starts the cache-server
   *
   * @return - the port on which cache-server is running
   * @throws Exception - thrown if any problem occurs in cache/server creation
   */
  public static Integer createServerCache() throws Exception {
    StatsBugDUnitTest test = new StatsBugDUnitTest();
    Properties props = new Properties();
    cache = test.createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    RegionAttributes attrs = factory.create();

    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    int port = getRandomAvailableTCPPort();
    server.setPort(port);
    server.setNotifyBySubscription(false);
    server.setSocketBufferSize(32768);
    server.start();
    LogWriterUtils.getLogWriter().info("Server started at PORT = " + port);
    return new Integer(port);
  }

  /**
   * Initializes the cache client
   *
   * @param port1 - port for the primary cache-server
   * @param port2 for the secondary cache-server
   * @throws Exception-thrown if any problem occurs in initializing the client
   */
  public static void createClientCache(String host, Integer port1, Integer port2) throws Exception {
    StatsBugDUnitTest test = new StatsBugDUnitTest();
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    pool = (PoolImpl) ClientServerTestCase.configureConnectionPool(factory, host,
        new int[] {port1.intValue(), port2.intValue()}, true, -1, 3, null);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    region.registerInterest("ALL_KEYS");
    LogWriterUtils.getLogWriter().info("Client cache created");
  }

  /**
   * Initializes the cache client
   *
   * @param port1 - port for the primary cache-server
   * @param port2 for the secondary cache-server
   * @throws Exception-thrown if any problem occurs in initializing the client
   */
  public static void createClientCacheForInvalidates(String host, Integer port1, Integer port2)
      throws Exception {
    StatsBugDUnitTest test = new StatsBugDUnitTest();
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    pool = (PoolImpl) ClientServerTestCase.configureConnectionPool(factory, host,
        new int[] {port1.intValue(), port2.intValue()}, true, -1, 3, null);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    region.registerInterest("ALL_KEYS", false, false);
    LogWriterUtils.getLogWriter().info("Client cache created");
  }

  /**
   * Verify that the invalidates stats at the client accounts for the operations done by both,
   * primary and secondary.
   *
   */
  public static void verifyNumInvalidates() {
    long invalidatesRecordedByStats = pool.getInvalidateCount();
    LogWriterUtils.getLogWriter()
        .info("invalidatesRecordedByStats = " + invalidatesRecordedByStats);

    int expectedInvalidates = TOTAL_SERVERS * PUTS_PER_SERVER;
    LogWriterUtils.getLogWriter().info("expectedInvalidates = " + expectedInvalidates);

    if (invalidatesRecordedByStats != expectedInvalidates) {
      fail("Invalidates received by client(" + invalidatesRecordedByStats
          + ") does not match with the number of operations(" + expectedInvalidates
          + ") done at server");
    }
  }

  /**
   * Stops the cache server
   *
   */
  public static void stopServer() {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer) iter.next();
        server.stop();
      }
    } catch (Exception e) {
      fail("failed while stopServer()" + e);
    }
  }

  /**
   * create properties for a loner VM
   */
  private static Properties createProperties1() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    return props;
  }


  /**
   * Do PUT operations
   *
   * @param keyPrefix - string prefix for the keys for all the entries do be done
   * @throws Exception - thrown if any exception occurs in doing PUTs
   */
  public static void doEntryOperations(String keyPrefix) throws Exception {
    Region r1 = cache.getRegion(SEPARATOR + REGION_NAME);
    for (int i = 0; i < PUTS_PER_SERVER; i++) {
      r1.put(keyPrefix + i, keyPrefix + "val-" + i);
    }
  }

  /**
   * Prepopulate the client with the entries that will be done by cache-servers
   *
   */
  public static void prepopulateClient() throws Exception {
    doEntryOperations(primaryPrefix);
    doEntryOperations(secondaryPrefix);
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
