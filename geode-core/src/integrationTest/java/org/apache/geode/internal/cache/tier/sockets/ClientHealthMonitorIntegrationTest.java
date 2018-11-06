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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * This is a functional-test for <code>ClientHealthMonitor</code>.
 */
@Category({ClientServerTest.class})
public class ClientHealthMonitorIntegrationTest {
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  /**
   * Default to 0; override in sub tests to add thread pool
   */
  protected int getMaxThreads() {
    return 0;
  }

  /**
   * connection proxy object for the client
   */
  PoolImpl proxy = null;

  /**
   * the distributed system instance for the test
   */
  DistributedSystem system;

  /**
   * the cache instance for the test
   */
  Cache cache;

  /**
   * name of the region created
   */
  final String regionName = "region1";

  private static int PORT;

  /**
   * Close the cache and disconnects from the distributed system
   */
  @After
  public void tearDown() throws Exception {
    if (this.cache != null) {
      this.cache.close();
    }
    if (this.system != null) {
      this.system.disconnect();
    }
    ClientHealthMonitor.shutdownInstance();
  }

  /**
   * Initializes proxy object and creates region for client
   */
  private void createProxyAndRegionForClient() throws Exception {
    PoolFactory pf = PoolManager.createFactory();
    proxy = (PoolImpl) pf.addServer("localhost", PORT).setThreadLocalConnections(true)
        .setReadTimeout(10000).setPingInterval(10000).setMinConnections(0).create("junitPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    cache.createVMRegion(regionName, factory.createRegionAttributes());
  }

  private static final int TIME_BETWEEN_PINGS = 2500;

  /**
   * Creates and starts the server instance
   */
  private int createServer() throws Exception {
    CacheServer server = null;
    Properties p = new Properties();
    // make it a loner
    p.put(MCAST_PORT, "0");
    p.put(LOCATORS, "");

    this.system = DistributedSystem.connect(p);
    this.cache = CacheFactory.create(system);
    server = this.cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setMaximumTimeBetweenPings(TIME_BETWEEN_PINGS);
    server.setMaxThreads(getMaxThreads());
    server.setPort(port);
    server.start();
    return server.getPort();
  }

  @Test
  public void settingMonitorIntervalViaProperty() {
    int monitorInterval = 10;
    System.setProperty(ClientHealthMonitor.CLIENT_HEALTH_MONITOR_INTERVAL_PROPERTY,
        String.valueOf(monitorInterval));

    assertEquals(monitorInterval,
        ClientHealthMonitor
            .getInstance(mock(InternalCache.class), 0, mock(CacheClientNotifierStats.class))
            .getMonitorInterval());
  }

  @Test
  public void monitorIntervalDefaultsWhenNotSet() {
    assertNotNull(ClientHealthMonitor
        .getInstance(mock(InternalCache.class), 0, mock(CacheClientNotifierStats.class))
        .getMonitorInterval());
  }

  @Test
  public void monitorIntervalDefaultsWhenInvalidValue() {
    String monitorInterval = "this isn't a number";
    System.setProperty(ClientHealthMonitor.CLIENT_HEALTH_MONITOR_INTERVAL_PROPERTY,
        monitorInterval);

    assertNotNull(ClientHealthMonitor
        .getInstance(mock(InternalCache.class), 0, mock(CacheClientNotifierStats.class))
        .getMonitorInterval());
  }

  /**
   * This test performs the following:<br>
   * 1)create server<br>
   * 2)initialize proxy object and create region for client<br>
   * 3)perform a PUT on client by acquiring Connection through proxy<br>
   * 4)stop server monitor threads in client to ensure that server treats this as dead client <br>
   * 5)wait for some time to allow server to clean up the dead client artifacts<br>
   * 6)again perform a PUT on client through same Connection and verify after the put that the
   * Connection object used was new one.
   */
  @Test
  public void testDeadClientRemovalByServer() throws Exception {
    System.setProperty(ClientHealthMonitor.CLIENT_HEALTH_MONITOR_INTERVAL_PROPERTY, "100");
    PORT = createServer();
    createProxyAndRegionForClient();
    StatisticsType statisticsType = this.system.findType("CacheServerStats");
    final Statistics statistics = this.system.findStatisticsByType(statisticsType)[0];
    assertEquals(0, statistics.getInt("currentClients"));
    assertEquals(0, statistics.getInt("currentClientConnections"));
    this.system.getLogWriter()
        .info("beforeAcquireConnection clients=" + statistics.getInt("currentClients") + " cnxs="
            + statistics.getInt("currentClientConnections"));
    Connection connection1 = proxy.acquireConnection();
    this.system.getLogWriter()
        .info("afterAcquireConnection clients=" + statistics.getInt("currentClients") + " cnxs="
            + statistics.getInt("currentClientConnections"));
    this.system.getLogWriter().info("acquired connection " + connection1);

    await().pollDelay(0, TimeUnit.MILLISECONDS)
        .until(() -> statistics.getInt("currentClients") == 1);

    assertEquals(1, statistics.getInt("currentClients"));
    assertEquals(1, statistics.getInt("currentClientConnections"));
    ServerRegionProxy srp = new ServerRegionProxy("region1", proxy);

    srp.putOnForTestsOnly(connection1, "key-1", "value-1", new EventID(new byte[] {1}, 1, 1), null);
    this.system.getLogWriter().info("did put 1");

    await().pollDelay(0, TimeUnit.MILLISECONDS)
        .until(() -> statistics.getInt("currentClients") == 0);

    this.system.getLogWriter().info("currentClients=" + statistics.getInt("currentClients")
        + " currentClientConnections=" + statistics.getInt("currentClientConnections"));
    assertEquals(0, statistics.getInt("currentClients"));
    assertEquals(0, statistics.getInt("currentClientConnections"));

    // the connection should now fail since the server timed it out
    try {
      srp.putOnForTestsOnly(connection1, "key-1", "fail", new EventID(new byte[] {1}, 1, 2), null);
      fail("expected EOF");
    } catch (ServerConnectivityException expected) {
    }
  }
}
