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
import static org.apache.geode.internal.cache.tier.sockets.DeltaToRegionRelationCQRegistrationDUnitTest.getClientProxy;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
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
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.categories.SerializationTest;

/**
 * This tests the flag setting for region ( DataPolicy as Empty ) for Delta propogation for a client
 * while registering CQ
 */
@Category({ClientSubscriptionTest.class, SerializationTest.class})
public class DeltaToRegionRelationCQRegistrationDUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  private VM server = null;
  private VM client = null;
  private VM server2 = null;
  private VM client2 = null;

  private int PORT1;
  private int PORT2;

  private static PoolImpl p = null;

  /*
   * name of the region with data policy empty
   */
  private static final String REGION_NAME1 = "DeltaToRegionRelationCQRegistration_region1";

  /*
   * name of the region whose data policy is not empty
   */
  private static final String REGION_NAME2 = "DeltaToRegionRelationCQRegistration_region2";

  /*
   * to detect primary server
   */
  private static Integer primary = null;

  /*
   * cq 1
   */
  private static final String CQ1 = "SELECT * FROM " + Region.SEPARATOR + REGION_NAME1;

  /*
   * cq 2
   */
  private static final String CQ2 = "SELECT * FROM " + Region.SEPARATOR + REGION_NAME2;

  /*
   * cq 3
   */
  private static final String CQ3 = "SELECT ALL * FROM " + Region.SEPARATOR + REGION_NAME1;

  /*
   * cq 4
   */
  private static final String CQ4 = "SELECT ALL * FROM " + Region.SEPARATOR + REGION_NAME2;

  private static final String cqName1 = "cqNameFirst";
  private static final String cqName2 = "cqNameSecond";
  private static final String cqName3 = "cqNamethird";
  private static final String cqName4 = "cqNameFourth";

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    Wait.pause(5000);
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    client = host.getVM(1);
    server2 = host.getVM(2);
    client2 = host.getVM(3);
  }

  /**
   * This test does the following for single cq registration(<b>with out initial resultset</b>):<br>
   * 1)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy
   * <br>
   * 2)Verifies no create happens for region with data policy other then Empty; in map <br>
   * 3)Verifies multiple and different interest registration (key/list) should not create multiple
   * entries in map <br>
   */
  @Test
  public void testDeltaToRegionForRegisterCQ() {

    intialSetUp();

    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName1, CQ1,
        new Boolean(false)));

    // Register CQ on region with data policy other then EMPTY
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName2, CQ2,
        new Boolean(false)));

    // validation on server side
    server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.validationOnServer());

    // check for multiple time cq registration
    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName3, CQ3,
        new Boolean(false)));

    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName4, CQ4,
        new Boolean(true)));

    // validation on server side
    server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.validationOnServer());

    tearDownforSimpleCase();
  }

  /**
   * This test does the following for single cq registration(<b>with initial resultset</b>):<br>
   * 1)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy
   * <br>
   * 2)Verifies no create happens for region with data policy other then Empty; in map <br>
   * 3)Verifies multiple and different interest registration (key/list) should not create multiple
   * entries in map <br>
   */
  @Test
  public void testDeltaToRegionForRegisterCQIR() {

    intialSetUp();

    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName1, CQ1,
        new Boolean(true)));

    // Register CQ on region with data policy other then EMPTY
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName2, CQ2,
        new Boolean(true)));

    // validation on server side
    server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.validationOnServer());

    // check for multiple time registration Interest

    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName3, CQ3,
        new Boolean(true)));

    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName4, CQ4,
        new Boolean(false)));

    // validation on server side
    server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.validationOnServer());

    tearDownforSimpleCase();
  }

  /**
   * This test does the following for single cq registration from pool (<b>with initial
   * resultset</b>):<br>
   * 1)Verifies create entry for region in map stored in CacheCleintProxy <br>
   * 2)Verifies multiple and different interest registration (key/list) should not create multiple
   * entries in map <br>
   */
  @Test
  public void testDeltaToRegionForRegisterCQIRThroughPool() {

    intialSetUpClientWithNoRegion();

    // Register CQ on region
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCqThroughPool(cqName1,
        CQ1, new Boolean(true)));

    // Register CQ on region
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCqThroughPool(cqName2,
        CQ2, new Boolean(true)));

    // validation on server side
    server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest
        .validationOnServerForCqRegistrationFromPool());

    // check for multiple time registration cq

    // Register CQ on region
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCqThroughPool(cqName3,
        CQ3, new Boolean(true)));

    // Register CQ on region
    // CQ registration
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCqThroughPool(cqName4,
        CQ4, new Boolean(false)));

    // validation on server side
    server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest
        .validationOnServerForCqRegistrationFromPool());

    tearDownforSimpleCase();
  }


  /**
   * This test does the following for cq registration when <b>failover</b>(<b>cq
   * registration</b>):<br>
   * 1)Verifies when primary goes down, cq registration happen <br>
   * 2)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy
   * <br>
   * 3)Verifies no create happens for region with data policy other then Empty; in map <br>
   */
  @Test
  public void testDeltaToRegionForRegisterCQFailover() {

    intialSetUpForFailOver();
    // Register CQ on region with data policy as EMPTY
    client2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName1, CQ1,
        new Boolean(false)));
    // Register CQ on region with data policy other then EMPTY
    client2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName2, CQ2,
        new Boolean(false)));

    validationForFailOver();
    tearDownForFailOver();
  }

  /**
   * This test does the following for cq registration with initial result set when
   * <b>failover</b>(<b>cq registration</b>):<br>
   * 1)Verifies when primary goes down, cq registration happen <br>
   * 2)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy
   * <br>
   * 3)Verifies no create happens for region with data policy other then Empty; in map <br>
   */
  @Test
  public void testDeltaToRegionForRegisterCQIRFaliover() {
    intialSetUpForFailOver();
    // Register CQ on region with data policy as EMPTY
    client2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName1, CQ1,
        new Boolean(true)));

    // Register CQ on region with data policy other then EMPTY
    client2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCq(cqName2, CQ2,
        new Boolean(true)));

    validationForFailOver();
    tearDownForFailOver();
  }

  /**
   * This test does the following for cq registration with initial result set through pool when
   * <b>failover</b>(<b>cq registration</b>):<br>
   * 1)Verifies when primary goes down, cq registration happen <br>
   * 2)Verifies create entry for region in map stored in CacheCleintProxy <br>
   */
  @Test
  public void testDeltaToRegionForRegisterCQIRFromPoolFaliover() {
    intialSetUpNoRegiononClientForFailOver();
    // Register CQ on region
    client2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCqThroughPool(cqName1,
        CQ1, new Boolean(true)));

    // Register CQ on region
    client2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.registerCqThroughPool(cqName2,
        CQ2, new Boolean(true)));

    validationForCQFiredFromPoolFailOver();
    tearDownForFailOver();
  }

  /*
   * register cq
   */
  public static void registerCq(String name, String Cquery, Boolean cqWithIR) {
    QueryService cqService = null;
    // get cq service
    try {
      cqService = cache.getQueryService();
    } catch (Exception cqe) {
      cqe.printStackTrace();
      fail("Failed to getCqService.");
    }

    // Create CQ Attributes.
    // do not attach any listiner lets see its response
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqAttributes cqa = cqf.create();

    // Create and Execute CQ.
    try {
      CqQuery cq1 = cqService.newCq(name, Cquery, cqa);
      assertTrue("newCq() state mismatch", cq1.getState().isStopped());
      if (cqWithIR)
        cq1.executeWithInitialResults();
      else
        cq1.execute();
    } catch (Exception ex) {
      fail("Failed to create CQ " + cqName1, ex);
    }

    CqQuery cQuery = cqService.getCq(name);
    if (cQuery == null) {
      fail("Failed to get CqQuery for CQ : " + cqName1);
    }
  }

  /*
   * register cq from pool
   */
  public static void registerCqThroughPool(String name, String Cquery, Boolean cqWithIR) {
    QueryService cqService = null;
    // get cq service
    try {
      cqService = p.getQueryService();
    } catch (Exception cqe) {
      cqe.printStackTrace();
      fail("Failed to getCqService.");
    }

    // Create CQ Attributes.
    // do not attach any listiner lets see its response
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqAttributes cqa = cqf.create();

    // Create and Execute CQ.
    try {
      CqQuery cq1 = cqService.newCq(name, Cquery, cqa);
      assertTrue("newCq() state mismatch", cq1.getState().isStopped());
      if (cqWithIR)
        cq1.executeWithInitialResults();
      else
        cq1.execute();
    } catch (Exception ex) {
      fail("Failed to create CQ " + cqName1, ex);
    }

    CqQuery cQuery = cqService.getCq(name);
    if (cQuery == null) {
      fail("Failed to get CqQuery for CQ : " + cqName1);
    }
  }

  public static void validationOnServer() throws Exception {
    checkNumberOfClientProxies(1);
    CacheClientProxy proxy = getClientProxy();
    assertNotNull(proxy);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return getClientProxy()
            .getRegionsWithEmptyDataPolicy().containsKey(SEPARATOR + REGION_NAME1);
      }

      public String description() {
        return "Wait Expired";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    assertTrue(REGION_NAME1 + " not present in cache client proxy : Delta is enable",
        proxy.getRegionsWithEmptyDataPolicy()
            .containsKey(SEPARATOR + REGION_NAME1)); /*
                                                      * Empty data policy
                                                      */
    assertFalse(REGION_NAME2 + " present in cache client proxy : Delta is disable",
        proxy.getRegionsWithEmptyDataPolicy()
            .containsKey(SEPARATOR + REGION_NAME2)); /*
                                                      * other then Empty data policy
                                                      */
    assertTrue("Multiple entries for a region", proxy.getRegionsWithEmptyDataPolicy().size() == 1);
    assertTrue("Wrong ordinal stored for empty data policy",
        ((Integer) proxy.getRegionsWithEmptyDataPolicy().get(SEPARATOR + REGION_NAME1))
            .intValue() == 0);

  }

  public static void validationOnServerForCqRegistrationFromPool() throws Exception {
    checkNumberOfClientProxies(1);
    CacheClientProxy proxy = getClientProxy();
    assertNotNull(proxy);

    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return getClientProxy()
            .getRegionsWithEmptyDataPolicy().containsKey(SEPARATOR + REGION_NAME1)
            && getClientProxy()
                .getRegionsWithEmptyDataPolicy().containsKey(SEPARATOR + REGION_NAME2);
      }

      public String description() {
        return "Wait Expired";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    assertTrue("Multiple entries for a region", proxy.getRegionsWithEmptyDataPolicy().size() == 2);

    assertTrue("Wrong ordinal stored for empty data policy",
        ((Integer) proxy.getRegionsWithEmptyDataPolicy().get(SEPARATOR + REGION_NAME1))
            .intValue() == 0);

    assertTrue("Wrong ordinal stored for empty data policy",
        ((Integer) proxy.getRegionsWithEmptyDataPolicy().get(SEPARATOR + REGION_NAME2))
            .intValue() == 0);

  }

  /*
   * create server cache
   */
  public static Integer createServerCache() throws Exception {
    new DeltaToRegionRelationCQRegistrationDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    cache.createRegion(REGION_NAME2, attrs);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());

  }

  /*
   * create client cache
   */
  public static void createClientCache(String host, Integer port) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new DeltaToRegionRelationCQRegistrationDUnitTest().createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port.intValue())
        .setThreadLocalConnections(true).setMinConnections(3).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(0).setReadTimeout(10000).setSocketBufferSize(32768)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("DeltaToRegionRelationCQRegistrationTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    factory.setCloningEnabled(false);

    // region with empty data policy
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);

    assertFalse(cache.getRegion(REGION_NAME1).getAttributes().getCloningEnabled());
    factory.setDataPolicy(DataPolicy.NORMAL);
    attrs = factory.create();
    // region with non empty data policy
    cache.createRegion(REGION_NAME2, attrs);
    assertFalse(cache.getRegion(REGION_NAME2).getAttributes().getCloningEnabled());
    cache.getRegion(REGION_NAME2).getAttributesMutator().setCloningEnabled(true);
    assertTrue(cache.getRegion(REGION_NAME2).getAttributes().getCloningEnabled());
  }

  /*
   * create client cache
   */
  public static void createClientCacheWithNoRegion(String host, Integer port) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new DeltaToRegionRelationCQRegistrationDUnitTest().createCache(props);
    p = (PoolImpl) PoolManager.createFactory().addServer(host, port.intValue())
        .setThreadLocalConnections(true).setMinConnections(3).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(0).setReadTimeout(10000).setSocketBufferSize(32768)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("DeltaToRegionRelationCQRegistrationTestPool");
  }

  /*
   * create client cache and return's primary server location object (primary)
   */
  public static Integer createClientCache2(String host1, String host2, Integer port1, Integer port2)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new DeltaToRegionRelationCQRegistrationDUnitTest().createCache(props);
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host1, port1.intValue())
        .addServer(host2, port2.intValue()).setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0).setReadTimeout(10000)
        .setSocketBufferSize(32768)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("DeltaToRegionRelationCQRegistrationTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());

    // region with empty data policy
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);

    factory.setDataPolicy(DataPolicy.NORMAL);
    attrs = factory.create();
    // region with non empty data policy
    cache.createRegion(REGION_NAME2, attrs);

    return new Integer(p.getPrimaryPort());
  }

  /*
   * create client cache and return's primary server location object (primary) no region created on
   * client
   */
  public static Integer createClientCache3(String host1, String host2, Integer port1, Integer port2)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new DeltaToRegionRelationCQRegistrationDUnitTest().createCache(props);
    p = (PoolImpl) PoolManager.createFactory().addServer(host1, port1.intValue())
        .addServer(host2, port2.intValue()).setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0).setReadTimeout(10000)
        .setSocketBufferSize(32768)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("DeltaToRegionRelationCQRegistrationTestPool");
    return new Integer(p.getPrimaryPort());
  }

  /*
   * close cache
   */
  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /*
   * get cache client proxy object
   */
  public static CacheClientProxy getClientProxy() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator i = notifier.getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = (CacheClientProxy) i.next();
    }
    return proxy;
  }

  /*
   * get cache server / cache server attacted to cache
   */
  private static CacheServerImpl getBridgeServer() {
    CacheServerImpl bridgeServer = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bridgeServer);
    return bridgeServer;
  }

  /*
   * number of client proxies are presert
   */
  private static int getNumberOfClientProxies() {
    return getBridgeServer().getAcceptor().getCacheClientNotifier().getClientProxies().size();
  }

  /*
   * if expected number of proxies are not present and wait
   */
  private static void checkNumberOfClientProxies(int expected) {
    int current = getNumberOfClientProxies();
    int tries = 1;
    while (expected != current && tries++ < 60) {
      try {
        Thread.sleep(250);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt(); // reset bit
      }
      current = getNumberOfClientProxies();
    }
    assertEquals(expected, current);
  }

  /*
   * stop cache server
   */
  public static void stopCacheServer() {
    getBridgeServer().stop();
  }

  /*
   * initial setup required for testcase with out failover
   */
  public void intialSetUp() {
    PORT1 = ((Integer) server
        .invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.createServerCache())).intValue();
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server.getHost()), new Integer(PORT1)));
  }

  /*
   * initial setup required for testcase with out failover
   */
  public void intialSetUpClientWithNoRegion() {
    PORT1 = ((Integer) server
        .invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.createServerCache())).intValue();
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.createClientCacheWithNoRegion(
        NetworkUtils.getServerHostName(server.getHost()), new Integer(PORT1)));
  }

  /*
   * kind of teardown for testcase without failover
   */
  public void tearDownforSimpleCase() {
    // close the clients first
    client.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.closeCache());
    // then close the servers
    server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.closeCache());
  }

  /*
   * initial setup required for testcase with failover
   */
  public void intialSetUpForFailOver() {
    PORT1 = ((Integer) server
        .invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.createServerCache())).intValue();
    // used only in failover tests
    PORT2 = ((Integer) server2
        .invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.createServerCache())).intValue();
    primary = (Integer) client2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest
        .createClientCache2(NetworkUtils.getServerHostName(server.getHost()),
            NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT1),
            new Integer(PORT2)));
  }

  /*
   * initial setup required for testcase with failover where we don't need region on client
   */
  public void intialSetUpNoRegiononClientForFailOver() {
    PORT1 = ((Integer) server
        .invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.createServerCache())).intValue();
    // used only in failover tests
    PORT2 = ((Integer) server2
        .invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.createServerCache())).intValue();
    primary = (Integer) client2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest
        .createClientCache3(NetworkUtils.getServerHostName(server.getHost()),
            NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT1),
            new Integer(PORT2)));
  }

  /*
   * find out primary and stop it register CQ whould happened on other server
   */
  public void validationForFailOver() {
    assertTrue(" primary server is not detected ", primary.intValue() != -1);
    if (primary.intValue() == PORT1) {
      server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.validationOnServer());
      server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.stopCacheServer());
      server2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.validationOnServer());
    } else {
      server2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.validationOnServer());
      server2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.stopCacheServer());
      server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.validationOnServer());
    }
  }

  /*
   * find out primary and stop it register CQ whould happened on other server
   */
  public void validationForCQFiredFromPoolFailOver() {
    assertTrue(" primary server is not detected ", primary.intValue() != -1);
    if (primary.intValue() == PORT1) {
      server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest
          .validationOnServerForCqRegistrationFromPool());
      server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.stopCacheServer());
      server2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest
          .validationOnServerForCqRegistrationFromPool());
    } else {
      server2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest
          .validationOnServerForCqRegistrationFromPool());
      server2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.stopCacheServer());
      server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest
          .validationOnServerForCqRegistrationFromPool());
    }
  }

  /*
   * kind of teardown for testcase with failover
   */
  public void tearDownForFailOver() {
    // close the clients first
    client2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.closeCache());
    // then close the servers
    server.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.closeCache());
    server2.invoke(() -> DeltaToRegionRelationCQRegistrationDUnitTest.closeCache());
  }

  /*
   * create cache with properties
   */
  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }
}
