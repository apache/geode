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

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.ConnectionFactoryImpl;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;

/**
 * @author Pallavi
 * 
 * Test to verify that server responds to a higher versioned client.
 */

public class BackwardCompatibilityHigherVersionClientDUnitTest extends
    DistributedTestCase {
  /** the cache */
  private static Cache cache = null;

  private static VM server1 = null;

  private static VM client1 = null;

  /** name of the test region */
  private static final String REGION_NAME = "BackwardCompatibilityHigherVersionClientDUnitTest_Region";

  static int CLIENT_ACK_INTERVAL = 5000;

  private static final String k1 = "k1";

  private static final String k2 = "k2";

  private static final String client_k1 = "client-k1";

  private static final String client_k2 = "client-k2";

  private static short currentClientVersion = ConnectionProxy.VERSION.ordinal();

  /** constructor */
  public BackwardCompatibilityHigherVersionClientDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
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

  public static void createClientCache(String host, Integer port1)
      throws Exception {
    new BackwardCompatibilityHigherVersionClientDUnitTest("temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new BackwardCompatibilityHigherVersionClientDUnitTest("temp")
        .createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer(host,
        port1.intValue()).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(1).setThreadLocalConnections(true)
        .setMinConnections(1).setFreeConnectionTimeout(200000).setReadTimeout(
            200000).setPingInterval(10000).setRetryAttempts(1)
        .setSubscriptionAckInterval(CLIENT_ACK_INTERVAL).create(
            "BackwardCompatibilityHigherVersionClientDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  public static Integer createServerCache() throws Exception {
    new BackwardCompatibilityHigherVersionClientDUnitTest("temp")
        .createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());

  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    client1.invoke(BackwardCompatibilityHigherVersionClientDUnitTest.class,
        "unsetHandshakeVersionForTesting");
    client1.invoke(BackwardCompatibilityHigherVersionClientDUnitTest.class,
        "unsetConnectionToServerFailed");

    // close the clients first
    client1.invoke(BackwardCompatibilityHigherVersionClientDUnitTest.class,
        "closeCache");
    // then close the servers
    server1.invoke(BackwardCompatibilityHigherVersionClientDUnitTest.class,
        "closeCache");
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
  public void testHigherVersionedClient() {
    Integer port1 = ((Integer)server1.invoke(
        BackwardCompatibilityHigherVersionClientDUnitTest.class,
        "createServerCache"));

    client1.invoke(BackwardCompatibilityHigherVersionClientDUnitTest.class,
        "setHandshakeVersionForTesting");
    client1.invoke(BackwardCompatibilityHigherVersionClientDUnitTest.class,
        "createClientCache", new Object[] {
            getServerHostName(server1.getHost()), port1 });
    client1.invoke(BackwardCompatibilityHigherVersionClientDUnitTest.class,
        "verifyConnectionToServerFailed");
  }

  /* 
   * Prepare to write invalid version ordinal byte (current+1) from client to server 
   * during handshake.
   */
  public static void setHandshakeVersionForTesting() throws Exception {
    HandShake.setVersionForTesting((short)(currentClientVersion + 1));
  }

  public static void addExceptions() throws Exception {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger().info(
          "<ExpectedException action=add>" + "UnsupportedVersionException"
              + "</ExpectedException>");
      cache.getLogger().info(
          "<ExpectedException action=add>" + "ServerRefusedConnectionException"
              + "</ExpectedException>");
      cache.getLogger().info(
          "<ExpectedException action=add>" + "SocketException"
              + "</ExpectedException>");
      cache.getLogger()
          .info(
              "<ExpectedException action=add>"
                  + "Could not initialize a primary queue"
                  + "</ExpectedException>");
    }
  }

  public static void removeExceptions() {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger().info(
          "<ExpectedException action=remove>" + "UnsupportedVersionException"
              + "</ExpectedException>");
      cache.getLogger().info(
          "<ExpectedException action=remove>"
              + "ServerRefusedConnectionException" + "</ExpectedException>");
      cache.getLogger().info(
          "<ExpectedException action=remove>" + "SocketException"
              + "</ExpectedException>");
      cache.getLogger()
          .info(
              "<ExpectedException action=remove>"
                  + "Could not initialize a primary queue"
                  + "</ExpectedException>");
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
   * Prepare to revert back to writing valid version ordinal byte from
   * client to server during handshake.
   */
  public static void unsetHandshakeVersionForTesting() throws Exception {
    HandShake.setVersionForTesting(currentClientVersion);
  }

  /*
   * Restore ConnectionFactoryImpl.testFailedConnectionToServer to its default
   * value.
   */
  public static void unsetConnectionToServerFailed() throws Exception {
    ConnectionFactoryImpl.testFailedConnectionToServer = false;
  }

  public static void destroyRegion() {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);
      r.destroyRegion();
    }
    catch (Exception ex) {
      fail("failed while destroy region ", ex);
    }
  }

}
