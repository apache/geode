/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Properties;
import java.util.HashMap;
import java.util.Map;

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
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.TestPut;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * @author Pallavi
 * 
 * Test to verify that server serves different versioned clients with their
 * respective client-versions of command .
 */
public class BackwardCompatibilityCommandDUnitDisabledTest extends DistributedTestCase {
  /** the cache */
  private static Cache cache = null;

  private static VM server1 = null;

  private static VM client1 = null;

  private static VM client2 = null;

  /** name of the test region */
  private static final String REGION_NAME = "BackwardCompatibilityCommandDUnitTest_Region";

  static int CLIENT_ACK_INTERVAL = 5000;

  private static final String k1 = "k1";

  private static final String k2 = "k2";

  private static final String client_k1 = "client-k1";

  private static final String client_k2 = "client-k2";

  public static boolean TEST_PUT_COMMAND_INVOKED = false;

  /** constructor */
  public BackwardCompatibilityCommandDUnitDisabledTest(String name) {
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
    new BackwardCompatibilityCommandDUnitDisabledTest("temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new BackwardCompatibilityCommandDUnitDisabledTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer(host,
        port1.intValue()).setSubscriptionEnabled(false)
        .setThreadLocalConnections(true).setMinConnections(1).setReadTimeout(
            20000).setPingInterval(10000).setRetryAttempts(1)
        .create("BackwardCompatibilityCommandDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    //region.registerInterest("ALL_KEYS");
  }

  public static Integer createServerCache(String hostName) throws Exception {
    new BackwardCompatibilityCommandDUnitDisabledTest("temp")
        .createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    BridgeServer server1 = cache.addBridgeServer();
    server1.setBindAddress(hostName);
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    // close the clients first
    client2.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class,
        "unsetHandshakeVersionForTesting");
    client1.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class, "closeCache");
    client2.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class, "closeCache");
    // then close the servers
    server1.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class, "closeCache");
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /*
   * Test to verify that server serves different versioned clients with their
   * respective client-versions of command .
   */
  public void testCommand() {
    server1.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class,
        "setTestCommands");
    String serverHostName = getServerHostName(server1.getHost());
    Integer port1 = ((Integer)server1.invoke(
        BackwardCompatibilityCommandDUnitDisabledTest.class, "createServerCache",
        new Object[] { serverHostName }));

    client1.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class,
        "createClientCache", new Object[] { serverHostName, port1 });

    client2.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class,
        "setHandshakeVersionForTesting");
    client2.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class,
        "createClientCache", new Object[] { serverHostName, port1 });

    client1.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class, "put");
    server1.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class,
        "checkTestPutCommandNotInvoked");

    client2.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class, "put");
    server1.invoke(BackwardCompatibilityCommandDUnitDisabledTest.class,
        "checkTestPutCommandInvoked");
  }

  /*
   * Add test command to CommandInitializer.ALL_COMMANDS.
   */
  public static void setTestCommands() throws Exception {
    getLogWriter().info("setTestCommands invoked");
    Map testCommands = new HashMap();
    testCommands.putAll((Map)CommandInitializer.ALL_COMMANDS
        .get(AcceptorImpl.VERSION));
    testCommands.put(new Integer(MessageType.PUT), new TestPut());
    CommandInitializer.testSetCommands(testCommands);
    getLogWriter().info("end of setTestCommands");
  }

  /*
   * Prepare to write TEST_VERSION byte from client to server during handshake.
   */
  public static void setHandshakeVersionForTesting() throws Exception {
    HandShake.setVersionForTesting(Version.TEST_VERSION.ordinal());
  }

  public static void put() {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r1);

      r1.put(k1, client_k1);
      assertEquals(r1.getEntry(k1).getValue(), client_k1);
      r1.put(k2, client_k2);
      assertEquals(r1.getEntry(k2).getValue(), client_k2);
    }
    catch (Exception ex) {
      fail("failed while put", ex);
    }
  }

  /*
   * Prepare to write revert back to original version from client to server
   * during handshake.
   */
  public static void unsetHandshakeVersionForTesting() throws Exception {
    HandShake.setVersionForTesting(ConnectionProxy.VERSION.ordinal());
  }

  /*
   * Check that TestPut command did not get invoked at server.
   */
  public static void checkTestPutCommandNotInvoked() {
    assertTrue("TestPut command got invoked for GFE57 versioned client",
        !TEST_PUT_COMMAND_INVOKED);
  }

  /*
   * Check whether TestPut command got invoked at server.
   */
  public static void checkTestPutCommandInvoked() {
    assertTrue(
        "TestPut command did not get invoked for higher versioned client",
        TEST_PUT_COMMAND_INVOKED);
  }

}
