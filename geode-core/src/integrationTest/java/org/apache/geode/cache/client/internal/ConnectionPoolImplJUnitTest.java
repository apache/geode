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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ConnectionPoolImplJUnitTest {

  private static final String expectedRedundantErrorMsg =
      "Could not find any server to host redundant client queue.";
  private static final String expectedPrimaryErrorMsg =
      "Could not find any server to host primary client queue.";

  private Cache cache;
  private int port;

  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    cache = CacheFactory.create(DistributedSystem.connect(props));
    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
  }

  @After
  public void tearDown() {
    if (cache != null && !cache.isClosed()) {
      unsetQueueError();
      cache.close();
    }
  }

  private void setQueueError() {
    final String addExpectedPEM =
        "<ExpectedException action=add>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final String addExpectedREM =
        "<ExpectedException action=add>" + expectedRedundantErrorMsg + "</ExpectedException>";

    cache.getLogger().info(addExpectedPEM);
    cache.getLogger().info(addExpectedREM);

  }

  private void unsetQueueError() {
    final String removeExpectedPEM =
        "<ExpectedException action=remove>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final String removeExpectedREM =
        "<ExpectedException action=remove>" + expectedRedundantErrorMsg + "</ExpectedException>";

    cache.getLogger().info(removeExpectedPEM);
    cache.getLogger().info(removeExpectedREM);
  }

  @Test
  public void testDefaults() throws Exception {
    setQueueError();
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port);

    PoolImpl pool = (PoolImpl) cpf.create("myfriendlypool");

    // check defaults
    assertEquals(PoolFactory.DEFAULT_SOCKET_CONNECT_TIMEOUT, pool.getSocketConnectTimeout());
    assertEquals(PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT, pool.getFreeConnectionTimeout());
    assertEquals(PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, pool.getServerConnectionTimeout());
    assertEquals(PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE, pool.getSocketBufferSize());
    assertEquals(PoolFactory.DEFAULT_READ_TIMEOUT, pool.getReadTimeout());
    assertEquals(PoolFactory.DEFAULT_MIN_CONNECTIONS, pool.getMinConnections());
    assertEquals(PoolFactory.DEFAULT_MAX_CONNECTIONS, pool.getMaxConnections());
    assertEquals(PoolFactory.DEFAULT_RETRY_ATTEMPTS, pool.getRetryAttempts());
    assertEquals(PoolFactory.DEFAULT_IDLE_TIMEOUT, pool.getIdleTimeout());
    assertEquals(PoolFactory.DEFAULT_PING_INTERVAL, pool.getPingInterval());
    assertEquals(PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS, pool.getThreadLocalConnections());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ENABLED, pool.getSubscriptionEnabled());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_REDUNDANCY, pool.getSubscriptionRedundancy());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT,
        pool.getSubscriptionMessageTrackingTimeout());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ACK_INTERVAL, pool.getSubscriptionAckInterval());
    assertEquals(PoolFactory.DEFAULT_SERVER_GROUP, pool.getServerGroup());
    // check non default
    assertEquals("myfriendlypool", pool.getName());
    assertEquals(1, pool.getServers().size());
    assertEquals(0, pool.getLocators().size());
    {
      InetSocketAddress addr = (InetSocketAddress) pool.getServers().get(0);
      assertEquals(port, addr.getPort());
      assertEquals("localhost", addr.getHostName());
    }

  }

  @Test
  public void testProperties() throws Exception {
    int readTimeout = 234234;
    int socketTimeout = 123123;

    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port).setSocketConnectTimeout(socketTimeout)
        .setReadTimeout(readTimeout).setThreadLocalConnections(true);

    PoolImpl pool = (PoolImpl) cpf.create("myfriendlypool");

    // check non default
    assertEquals("myfriendlypool", pool.getName());
    assertEquals(socketTimeout, pool.getSocketConnectTimeout());
    assertEquals(readTimeout, pool.getReadTimeout());
    assertEquals(true, pool.getThreadLocalConnections());
    assertEquals(1, pool.getServers().size());
    assertEquals(0, pool.getLocators().size());
    {
      InetSocketAddress addr = (InetSocketAddress) pool.getServers().get(0);
      assertEquals(port, addr.getPort());
      assertEquals("localhost", addr.getHostName());
    }
    LiveServerPinger lsp = new LiveServerPinger(pool);
    long NANOS_PER_MS = 1000000L;
    assertEquals(((pool.getPingInterval() + 1) / 2) * NANOS_PER_MS, lsp.pingIntervalNanos);
  }

  @Test
  public void testCacheClose() throws Exception {
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addLocator("localhost", AvailablePortHelper.getRandomAvailableTCPPort());
    Pool pool1 = cpf.create("pool1");
    Pool pool2 = cpf.create("pool2");
    cache.close();

    assertTrue(pool1.isDestroyed());
    assertTrue(pool2.isDestroyed());
  }

  @Test
  public void testExecuteOp() throws Exception {
    CacheServer server1 = cache.addCacheServer();
    CacheServer server2 = cache.addCacheServer();
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int port1 = ports[0];
    int port2 = ports[1];
    server1.setPort(port1);
    server2.setPort(port2);

    server1.start();
    server2.start();

    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port2);
    cpf.addServer("localhost", port1);
    PoolImpl pool = (PoolImpl) cpf.create("pool1");

    ServerLocation location1 = new ServerLocation("localhost", port1);
    ServerLocation location2 = new ServerLocation("localhost", port2);

    Op testOp = new Op() {
      int attempts = 0;

      @Override
      public Object attempt(Connection cnx) throws Exception {
        if (attempts == 0) {
          attempts++;
          throw new SocketTimeoutException();
        } else {
          return cnx.getServer();
        }

      }
    };

    // TODO - set retry attempts, and throw in some assertions
    // about how many times we retry

    ServerLocation usedServer = (ServerLocation) pool.execute(testOp);
    assertTrue("expected " + location1 + " or " + location2 + ", got " + usedServer,
        location1.equals(usedServer) || location2.equals(usedServer));

    testOp = new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        throw new SocketTimeoutException();
      }
    };

    try {
      usedServer = (ServerLocation) pool.execute(testOp);
      fail("Should have failed");
    } catch (ServerConnectivityException expected) {
      // do nothing
    }
  }

  @Test
  public void testCreatePool() throws Exception {
    CacheServer server1 = cache.addCacheServer();
    int port1 = port;
    server1.setPort(port1);

    server1.start();

    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port1);
    cpf.setSubscriptionEnabled(true);
    cpf.setSubscriptionRedundancy(0);
    PoolImpl pool = (PoolImpl) cpf.create("pool1");

    ServerLocation location1 = new ServerLocation("localhost", port1);

    Op testOp = new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        return cnx.getServer();
      }
    };

    assertEquals(location1, pool.executeOnPrimary(testOp));
    assertEquals(location1, pool.executeOnQueuesAndReturnPrimaryResult(testOp));
  }

  @Test
  public void testCalculateRetryFromThrownException() throws Exception {
    int readTimeout = 234234;
    int socketTimeout = 123123;
    int port1 = 10000;
    int retryAttempts = 0;

    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port).setSocketConnectTimeout(socketTimeout)
        .setReadTimeout(readTimeout).setThreadLocalConnections(true);

    ServerLocation location1 = new ServerLocation("fakehost", port1);

    PoolImpl pool = (PoolImpl) cpf.create("testpool");
    try {
      pool.acquireConnection(location1);
      fail("expected a ServerConnectivityException");
    } catch (Exception e) {
      retryAttempts = pool.calculateRetryAttempts(e);
    }
    assertEquals(1, retryAttempts);
  }
}
