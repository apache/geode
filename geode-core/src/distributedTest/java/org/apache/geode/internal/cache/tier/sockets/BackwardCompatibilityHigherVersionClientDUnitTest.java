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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.cache.client.internal.ClientSideHandshakeImpl;
import org.apache.geode.cache.client.internal.ConnectionFactoryImpl;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.tier.ConnectionProxy;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

/**
 * Test to verify that server responds to a higher versioned client.
 */
@Category({ClientServerTest.class})
public class BackwardCompatibilityHigherVersionClientDUnitTest extends JUnit4DistributedTestCase {
  // On Windows, when the server closes the socket, the client can't read the server's reply. The
  // client quietly ignores the server's reply rather than throwing the required exception.
  @ClassRule
  public static IgnoreOnWindowsRule ignoreOnWindowsRule = new IgnoreOnWindowsRule();

  /** the cache */
  private static Cache cache = null;

  private VM server1 = null;

  private static VM client1 = null;

  /** name of the test region */
  private static final String REGION_NAME =
      "BackwardCompatibilityHigherVersionClientDUnitTest_Region";

  static int CLIENT_ACK_INTERVAL = 5000;

  private static final String k1 = "k1";

  private static final String k2 = "k2";

  private static final String client_k1 = "client-k1";

  private static final String client_k2 = "client-k2";

  private static final short currentClientVersion = ConnectionProxy.VERSION.ordinal();

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    client1 = host.getVM(1);
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    addExceptions();
  }

  public static void createClientCache(String host, Integer port1) throws Exception {
    new BackwardCompatibilityHigherVersionClientDUnitTest();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new BackwardCompatibilityHigherVersionClientDUnitTest().createCache(props);
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, port1.intValue())
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(1).setMinConnections(1)
        .setFreeConnectionTimeout(200000).setReadTimeout(200000).setPingInterval(10000)
        .setRetryAttempts(1).setSubscriptionAckInterval(CLIENT_ACK_INTERVAL)
        .create("BackwardCompatibilityHigherVersionClientDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  public static Integer createServerCache() throws Exception {
    new BackwardCompatibilityHigherVersionClientDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = getRandomAvailableTCPPort();
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());

  }

  @Override
  public final void postTearDown() throws Exception {
    client1.invoke(
        () -> BackwardCompatibilityHigherVersionClientDUnitTest.unsetHandshakeVersionForTesting());
    client1.invoke(
        () -> BackwardCompatibilityHigherVersionClientDUnitTest.unsetConnectionToServerFailed());

    // close the clients first
    client1.invoke(() -> BackwardCompatibilityHigherVersionClientDUnitTest.closeCache());
    // then close the servers
    server1.invoke(() -> BackwardCompatibilityHigherVersionClientDUnitTest.closeCache());
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      removeExceptions();
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * Verify that server responds to a higher versioned client.
   */
  @Test
  public void testHigherVersionedClient() {
    Integer port1 = (server1
        .invoke(() -> BackwardCompatibilityHigherVersionClientDUnitTest.createServerCache()));

    client1.invoke(
        () -> BackwardCompatibilityHigherVersionClientDUnitTest.setHandshakeVersionForTesting());

    assertThatThrownBy(() -> client1.invoke(() -> BackwardCompatibilityHigherVersionClientDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), port1)))
            .getCause().isInstanceOf(ServerRefusedConnectionException.class)
            .hasMessageContaining("refused connection: Peer or client version with ordinal");

    client1.invoke(
        () -> BackwardCompatibilityHigherVersionClientDUnitTest.verifyConnectionToServerFailed());
  }

  /*
   * Prepare to write invalid version ordinal byte (current+1) from client to server during
   * handshake.
   */
  public static void setHandshakeVersionForTesting() throws Exception {
    ClientSideHandshakeImpl.setVersionForTesting((short) (currentClientVersion + 1));
  }

  public static void addExceptions() throws Exception {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger().info("<ExpectedException action=add>" + "UnsupportedVersionException"
          + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=add>" + "ServerRefusedConnectionException"
          + "</ExpectedException>");
      cache.getLogger()
          .info("<ExpectedException action=add>" + "SocketException" + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=add>"
          + "Could not initialize a primary queue" + "</ExpectedException>");
    }
  }

  public static void removeExceptions() {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger().info("<ExpectedException action=remove>" + "UnsupportedVersionException"
          + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=remove>"
          + "ServerRefusedConnectionException" + "</ExpectedException>");
      cache.getLogger()
          .info("<ExpectedException action=remove>" + "SocketException" + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=remove>"
          + "Could not initialize a primary queue" + "</ExpectedException>");
    }
  }

  /*
   * Verify that Client failed when connecting to Server
   */
  public static void verifyConnectionToServerFailed() throws Exception {
    assertTrue("Higher version Client connected successfully to Server",
        ConnectionFactoryImpl.testFailedConnectionToServer == true);
  }

  /*
   * Prepare to revert back to writing valid version ordinal byte from client to server during
   * handshake.
   */
  public static void unsetHandshakeVersionForTesting() throws Exception {
    ClientSideHandshakeImpl.setVersionForTesting(currentClientVersion);
  }

  /*
   * Restore ConnectionFactoryImpl.testFailedConnectionToServer to its default value.
   */
  public static void unsetConnectionToServerFailed() throws Exception {
    ConnectionFactoryImpl.testFailedConnectionToServer = false;
  }

  public static void destroyRegion() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.destroyRegion();
    } catch (Exception ex) {
      Assert.fail("failed while destroy region ", ex);
    }
  }

}
