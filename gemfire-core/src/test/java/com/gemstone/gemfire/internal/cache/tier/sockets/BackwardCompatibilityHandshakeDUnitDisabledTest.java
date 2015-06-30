/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * @author Pallavi
 * 
 * Test to verify handshake with equal or lower version clients. Both equal and
 * lower version clients should be able to connect to the server successfully.
 */
public class BackwardCompatibilityHandshakeDUnitDisabledTest extends
    DistributedTestCase {

  /** the cache */
  private static Cache cache = null;

  private static VM server1 = null;

  private static VM client1 = null;

  private static VM client2 = null;

  /** name of the test region */
  private static final String REGION_NAME = "BackwardCompatibilityHandshakeDUnitTest_Region";

  static int CLIENT_ACK_INTERVAL = 5000;

  private static short clientVersionForTesting = Version.TEST_VERSION.ordinal();

  private static short currentClientVersion = ConnectionProxy.VERSION.ordinal();

  /** constructor */
  public BackwardCompatibilityHandshakeDUnitDisabledTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    client1 = host.getVM(1);
    client2 = host.getVM(2);

  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, Integer port1)
      throws Exception {
    new BackwardCompatibilityHandshakeDUnitDisabledTest("temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new BackwardCompatibilityHandshakeDUnitDisabledTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer(host,
        port1.intValue()).setSubscriptionEnabled(true)
        .setThreadLocalConnections(true).setMinConnections(1).setReadTimeout(
            20000).setPingInterval(10000).setRetryAttempts(1)
        .setSubscriptionAckInterval(CLIENT_ACK_INTERVAL).create(
            "BackwardCompatibilityHandshakeDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    region.registerInterest("ALL_KEYS");
  }

  public static Integer createServerCache(String serverHostName)
      throws Exception {
    new BackwardCompatibilityHandshakeDUnitDisabledTest("temp")
        .createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    BridgeServer server1 = cache.addBridgeServer();
    server1.setBindAddress(serverHostName);
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    client2.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class,
        "unsetHandshakeVersionForTesting");
    client2.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class,
        "unsetTestVersionAfterHandshake");
    // close the clients first
    client1.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class, "closeCache");
    client2.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class, "closeCache");
    // then close the servers
    server1.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class, "closeCache");
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /*
   * Test to verify handshake with equal or lower version clients. Both equal
   * and lower version clients should be able to connect to the server
   * successfully.
   */
  public void testHandShake() {
    server1.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class,
        "setTestCommands");
    String serverHostName = getServerHostName(server1.getHost());
    Integer port1 = ((Integer)server1.invoke(
        BackwardCompatibilityHandshakeDUnitDisabledTest.class, "createServerCache",
        new Object[] { serverHostName }));

    client1.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class,
        "createClientCache", new Object[] { serverHostName, port1 });

    client2.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class,
        "setHandshakeVersionForTesting");
    client2.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class,
        "setTestVersionAfterHandshake");
    client2.invoke(BackwardCompatibilityHandshakeDUnitDisabledTest.class,
        "createClientCache", new Object[] { serverHostName, port1 });
  }

  /*
   * Add test command to CommandInitializer.ALL_COMMANDS.
   */
  public static void setTestCommands() throws Exception {
    getLogWriter().info("setTestCommands invoked");
    Map testCommands = new HashMap();
    testCommands.putAll((Map)CommandInitializer.ALL_COMMANDS
        .get(Version.GFE_57));
    CommandInitializer.testSetCommands(testCommands);
    getLogWriter().info("end of setTestCommands");
  }

  /*
   * Prepare to write TEST_VERSION byte from client to server during
   * handshake.
   */
  public static void setHandshakeVersionForTesting() throws Exception {
    HandShake.setVersionForTesting(clientVersionForTesting);
  }

  /*
   * Prepare to test that Server detected and created ClientHandshake as
   * requested.
   */
  public static void setTestVersionAfterHandshake() throws Exception {
    ServerConnection.TEST_VERSION_AFTER_HANDSHAKE_FLAG = true;
    ServerConnection.testVersionAfterHandshake = clientVersionForTesting;
  }

  /*
   * Prepare to revert back version byte to current client version for
   * client-to-server handshake.
   */
  public static void unsetHandshakeVersionForTesting() throws Exception {
    HandShake.setVersionForTesting(currentClientVersion);
  }

  /*
   * Prepare to revert back Server testing of ClientHandshake as requested.
   */
  public static void unsetTestVersionAfterHandshake() throws Exception {
    ServerConnection.TEST_VERSION_AFTER_HANDSHAKE_FLAG = false;
    ServerConnection.testVersionAfterHandshake = (currentClientVersion);
  }
  /*
   * public static void destroyRegion() { try { Region r = cache.getRegion("/" +
   * REGION_NAME); assertNotNull(r); r.destroyRegion(); } catch (Exception ex) {
   * fail("failed while destroy region ", ex); } }
   */
}
