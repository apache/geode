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

import java.util.Iterator;
import java.util.Properties;

import javax.naming.InitialContext;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;
/**
 * This tests the flag setting for region ( DataPolicy as Empty ) for
 * Delta propogation for a client while registering CQ
 * 
 * @author aingle
 */
public class DeltaToRegionRelationCQRegistrationDUnitTest extends DistributedTestCase {
  private static Cache cache = null;

  VM server = null;

  VM client = null;
 
  VM server2 = null;
  
  VM client2 = null;
 
  private static  int PORT1 ;
  
  private static  int PORT2 ;
  
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
  private static final String CQ1 = "SELECT * FROM "+Region.SEPARATOR+REGION_NAME1;
  
  /*
   * cq 2
   */
  private static final String CQ2 = "SELECT * FROM "+Region.SEPARATOR+REGION_NAME2;
  
  /*
   * cq 3
   */
  private static final String CQ3 = "SELECT ALL * FROM "+Region.SEPARATOR+REGION_NAME1;
  
  /*
   * cq 4
   */
  private static final String CQ4 = "SELECT ALL * FROM "+Region.SEPARATOR+REGION_NAME2;
    
  private static final String cqName1="cqNameFirst";
  private static final String cqName2="cqNameSecond";
  private static final String cqName3="cqNamethird";
  private static final String cqName4="cqNameFourth";
  
  /** constructor */
  public DeltaToRegionRelationCQRegistrationDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception
  {
    disconnectAllFromDS();
    pause(5000);
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    client = host.getVM(1);
    server2 = host.getVM(2);
    client2 = host.getVM(3);
  }
  
  /**
   * This test does the following for single cq registration(<b>with out initial resultset</b>):<br>
   * 1)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 2)Verifies no create happens for region with data policy other then Empty; in map <br>
   * 3)Verifies multiple and different interest registration (key/list) should not create multiple entries in map <br>
   */
  public void testDeltaToRegionForRegisterCQ(){
    
    intialSetUp();
    
    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName1, CQ1, new Boolean(false)});
    
    // Register CQ on region with data policy other then EMPTY
    // CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName2, CQ2, new Boolean(false)});
    
    // validation on server side
    server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServer");
   
    //  check for multiple time cq registration 
    //  Register CQ on region with data policy as EMPTY
    //  CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName3, CQ3, new Boolean(false)});
    
    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName4, CQ4, new Boolean(true)});
    
    // validation on server side
    server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServer");
    
    tearDownforSimpleCase();
  }
  
  /**
   * This test does the following for single cq registration(<b>with initial resultset</b>):<br>
   * 1)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 2)Verifies no create happens for region with data policy other then Empty; in map <br>
   * 3)Verifies multiple and different interest registration (key/list) should not create multiple entries in map <br>
   */
  public void testDeltaToRegionForRegisterCQIR(){
    
    intialSetUp();
    
    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName1, CQ1, new Boolean(true)});
    
    // Register CQ on region with data policy other then EMPTY
    // CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName2, CQ2, new Boolean(true)});
    
    // validation on server side
    server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServer");
   
    //  check for multiple time registration Interest
    
    //  Register CQ on region with data policy as EMPTY
    //  CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName3, CQ3, new Boolean(true) });
    
    // Register CQ on region with data policy as EMPTY
    // CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName4, CQ4, new Boolean(false)});
    
    // validation on server side
    server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServer");
    
    tearDownforSimpleCase();
  }
  
  /**
   * This test does the following for single cq registration from pool (<b>with initial resultset</b>):<br>
   * 1)Verifies create entry for region in map stored in CacheCleintProxy <br>
   * 2)Verifies multiple and different interest registration (key/list) should not create multiple entries in map <br>
   */
  public void testDeltaToRegionForRegisterCQIRThroughPool(){
    
    intialSetUpClientWithNoRegion();
    
    // Register CQ on region
    // CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCqThroughPool",
        new Object[] {cqName1, CQ1, new Boolean(true)});
    
    // Register CQ on region 
    // CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCqThroughPool",
        new Object[] {cqName2, CQ2, new Boolean(true)});
    
    // validation on server side
    server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServerForCqRegistrationFromPool");
   
    //  check for multiple time registration cq
    
    //  Register CQ on region 
    //  CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCqThroughPool",
        new Object[] {cqName3, CQ3, new Boolean(true) });
    
    // Register CQ on region
    // CQ registration
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCqThroughPool",
        new Object[] {cqName4, CQ4, new Boolean(false)});
    
    // validation on server side
    server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServerForCqRegistrationFromPool");
    
    tearDownforSimpleCase();
  }
  
  
  /**
   * This test does the following for cq registration when <b>failover</b>(<b>cq registration</b>):<br>
   * 1)Verifies when primary goes down, cq registration happen <br>
   * 2)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 3)Verifies no create happens for region with data policy other then Empty; in map <br>
   */
  public void testDeltaToRegionForRegisterCQFailover(){
    
    intialSetUpForFailOver();
    // Register CQ on region with data policy as EMPTY
    client2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName1, CQ1, new Boolean(false)});
    // Register CQ on region with data policy other then EMPTY
    client2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName2, CQ2, new Boolean(false)});
        
    validationForFailOver();
    tearDownForFailOver();
  }
  
  /**
   * This test does the following for cq registration with initial result set when <b>failover</b>(<b>cq registration</b>):<br>
   * 1)Verifies when primary goes down, cq registration happen <br>
   * 2)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 3)Verifies no create happens for region with data policy other then Empty; in map <br>
   */
  public void testDeltaToRegionForRegisterCQIRFaliover() {
    intialSetUpForFailOver();
    // Register CQ on region with data policy as EMPTY
    client2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName1, CQ1, new Boolean(true)});

    // Register CQ on region with data policy other then EMPTY
    client2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCq",
        new Object[] {cqName2, CQ2, new Boolean(true)});

    validationForFailOver();
    tearDownForFailOver();
  }

  /**
   * This test does the following for cq registration with initial result set through pool when <b>failover</b>(<b>cq registration</b>):<br>
   * 1)Verifies when primary goes down, cq registration happen <br>
   * 2)Verifies create entry for region in map stored in CacheCleintProxy <br>
   */
  public void testDeltaToRegionForRegisterCQIRFromPoolFaliover() {
    intialSetUpNoRegiononClientForFailOver();
    // Register CQ on region
    client2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCqThroughPool",
        new Object[] {cqName1, CQ1, new Boolean(true)});
    
    //  Register CQ on region 
    client2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "registerCqThroughPool",
        new Object[] {cqName2, CQ2, new Boolean(true)});
    
    validationForCQFiredFromPoolFailOver();
    tearDownForFailOver();
  }
 
  /*
   * register cq 
   */
  public static void registerCq(String name, String Cquery, Boolean cqWithIR){
    QueryService cqService = null;
    //get cq service
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
    
//  Create and Execute CQ.
    try {
      CqQuery cq1 = cqService.newCq(name, Cquery, cqa);
      assertTrue("newCq() state mismatch", cq1.getState().isStopped());
      if(cqWithIR)
        cq1.executeWithInitialResults();
      else  
        cq1.execute();
    } catch (Exception ex){
      getLogWriter().info("CqService is :" + cqService);
      ex.printStackTrace();
      AssertionError err = new AssertionError("Failed to create CQ " + cqName1 + " . ");
      err.initCause(ex);
      throw err;
    }
    
    CqQuery cQuery = cqService.getCq(name);
    if (cQuery == null) {
      fail("Failed to get CqQuery for CQ : " + cqName1);
    }
  }

  
  /*
   * register cq from pool
   */
  public static void registerCqThroughPool(String name, String Cquery, Boolean cqWithIR){
    QueryService cqService = null;
    //get cq service
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
    
//  Create and Execute CQ.
    try {
      CqQuery cq1 = cqService.newCq(name, Cquery, cqa);
      assertTrue("newCq() state mismatch", cq1.getState().isStopped());
      if(cqWithIR)
        cq1.executeWithInitialResults();
      else  
        cq1.execute();
    } catch (Exception ex){
      getLogWriter().info("CqService is :" + cqService);
      ex.printStackTrace();
      AssertionError err = new AssertionError("Failed to create CQ " + cqName1 + " . ");
      err.initCause(ex);
      throw err;
    }
    
    CqQuery cQuery = cqService.getCq(name);
    if (cQuery == null) {
      fail("Failed to get CqQuery for CQ : " + cqName1);
    }
  }
  
  public static void validationOnServer() throws Exception{
    checkNumberOfClientProxies(1);
    CacheClientProxy proxy = getClientProxy();
    assertNotNull(proxy);
//  wait
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return DeltaToRegionRelationCQRegistrationDUnitTest.getClientProxy()
            .getRegionsWithEmptyDataPolicy().containsKey(
                Region.SEPARATOR + REGION_NAME1);
      }

      public String description() {
        return "Wait Expired";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 5 * 1000, 100, true);
    
    assertTrue(REGION_NAME1
        + " not present in cache client proxy : Delta is enable", proxy
        .getRegionsWithEmptyDataPolicy().containsKey(Region.SEPARATOR+REGION_NAME1)); /*
                                                               * Empty data
                                                               * policy
                                                               */
    assertFalse(REGION_NAME2
        + " present in cache client proxy : Delta is disable", proxy
        .getRegionsWithEmptyDataPolicy().containsKey(Region.SEPARATOR+REGION_NAME2)); /*
                                                               * other then Empty data
                                                               * policy
                                                               */
    assertTrue("Multiple entries for a region", proxy.getRegionsWithEmptyDataPolicy().size()==1);
    assertTrue("Wrong ordinal stored for empty data policy", ((Integer)proxy
        .getRegionsWithEmptyDataPolicy().get(Region.SEPARATOR+REGION_NAME1)).intValue() == 0);
  
  }
  
  public static void validationOnServerForCqRegistrationFromPool()
      throws Exception {
    checkNumberOfClientProxies(1);
    CacheClientProxy proxy = getClientProxy();
    assertNotNull(proxy);

//  wait
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return DeltaToRegionRelationCQRegistrationDUnitTest.getClientProxy()
            .getRegionsWithEmptyDataPolicy().containsKey(
                Region.SEPARATOR + REGION_NAME1)
            && DeltaToRegionRelationCQRegistrationDUnitTest.getClientProxy()
                .getRegionsWithEmptyDataPolicy().containsKey(
                    Region.SEPARATOR + REGION_NAME2);
      }

      public String description() {
        return "Wait Expired";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 5 * 1000, 100, true);
    
    assertTrue("Multiple entries for a region", proxy
        .getRegionsWithEmptyDataPolicy().size() == 2);

    assertTrue("Wrong ordinal stored for empty data policy", ((Integer)proxy
        .getRegionsWithEmptyDataPolicy().get(Region.SEPARATOR + REGION_NAME1))
        .intValue() == 0);

    assertTrue("Wrong ordinal stored for empty data policy", ((Integer)proxy
        .getRegionsWithEmptyDataPolicy().get(Region.SEPARATOR + REGION_NAME2))
        .intValue() == 0);

  }
  /*
   * create server cache
   */
  public static Integer createServerCache() throws Exception
  {
    new DeltaToRegionRelationCQRegistrationDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    cache.createRegion(REGION_NAME2, attrs);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server.setPort(port);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());

  }
  /*
   * create client cache
   */
  public static void createClientCache(String host, Integer port)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new DeltaToRegionRelationCQRegistrationDUnitTest("temp").createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port.intValue())
        .setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
        .setReadTimeout(10000).setSocketBufferSize(32768)
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
  public static void createClientCacheWithNoRegion(String host, Integer port)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new DeltaToRegionRelationCQRegistrationDUnitTest("temp").createCache(props);
    p = (PoolImpl)PoolManager.createFactory().addServer(host, port.intValue())
        .setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
        .setReadTimeout(10000).setSocketBufferSize(32768)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("DeltaToRegionRelationCQRegistrationTestPool");
  }
  /*
   * create client cache and return's primary server location object (primary)
   */
  public static Integer createClientCache2(String host1, String host2,
      Integer port1, Integer port2) throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new DeltaToRegionRelationCQRegistrationDUnitTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer(host1,
        port1.intValue()).addServer(host2, port2.intValue())
        .setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
        .setReadTimeout(10000).setSocketBufferSize(32768)
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
   * create client cache and return's primary server location object (primary)
   * no region created on client
   */
  public static Integer createClientCache3(String host1, String host2,
      Integer port1, Integer port2) throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new DeltaToRegionRelationCQRegistrationDUnitTest("temp").createCache(props);
    p = (PoolImpl)PoolManager.createFactory().addServer(host1,
        port1.intValue()).addServer(host2, port2.intValue())
        .setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
        .setReadTimeout(10000).setSocketBufferSize(32768)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("DeltaToRegionRelationCQRegistrationTestPool");
    return new Integer(p.getPrimaryPort());
  }
  
  public void tearDown2() throws Exception
  {
    // donot do any thing as we handling closing cache in test case
  }

  /*
   * close cache
   */
  public static void closeCache()
  {
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
    CacheClientNotifier notifier = getBridgeServer().getAcceptor()
        .getCacheClientNotifier();
    
    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator i = notifier.getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = (CacheClientProxy) i.next();
    }
    return proxy;
  }
  
  /*
   * get cache server / bridge server attacted to cache
   */
  private static CacheServerImpl getBridgeServer() {
    CacheServerImpl bridgeServer = (CacheServerImpl)cache.getCacheServers()
        .iterator().next();
    assertNotNull(bridgeServer);
    return bridgeServer;
  }
  /*
   * number of client proxies are presert
   */
  private static int getNumberOfClientProxies() {
    return getBridgeServer().getAcceptor().getCacheClientNotifier()
        .getClientProxies().size();
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
   * stop bridge server
   */
  public static void stopCacheServer(){
    getBridgeServer().stop();
  }
  /*
   * initial setup required for testcase with out failover
   */
  public void intialSetUp() {
    PORT1 = ((Integer)server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class,
        "createServerCache")).intValue();
    client
        .invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server.getHost()),
                new Integer(PORT1) });
  }
  
  /*
   * initial setup required for testcase with out failover
   */
  public void intialSetUpClientWithNoRegion() {
    PORT1 = ((Integer)server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class,
        "createServerCache")).intValue();
    client
        .invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "createClientCacheWithNoRegion",
            new Object[] { getServerHostName(server.getHost()),
                new Integer(PORT1) });
  }
  /*
   * kind of teardown for testcase without failover
   */
  public void tearDownforSimpleCase() {
    //  close the clients first
    client.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "closeCache");
    // then close the servers
    server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "closeCache");
  }  
  /*
   * initial setup required for testcase with failover
   */
  public void intialSetUpForFailOver() {
    PORT1 = ((Integer)server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class,
        "createServerCache")).intValue();
    // used only in failover tests
    PORT2 = ((Integer)server2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class,
        "createServerCache")).intValue();
    primary = (Integer)client2.invoke(
        DeltaToRegionRelationCQRegistrationDUnitTest.class, "createClientCache2",
        new Object[] { getServerHostName(server.getHost()),
            getServerHostName(server2.getHost()), new Integer(PORT1),
            new Integer(PORT2) });
  }
  
  /*
   * initial setup required for testcase with failover where we don't need region on client
   */
  public void intialSetUpNoRegiononClientForFailOver() {
    PORT1 = ((Integer)server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class,
        "createServerCache")).intValue();
    // used only in failover tests
    PORT2 = ((Integer)server2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class,
        "createServerCache")).intValue();
    primary = (Integer)client2.invoke(
        DeltaToRegionRelationCQRegistrationDUnitTest.class, "createClientCache3",
        new Object[] { getServerHostName(server.getHost()),
            getServerHostName(server2.getHost()), new Integer(PORT1),
            new Integer(PORT2) });
  }
  
  /*
   * find out primary and stop it
   * register CQ whould happened on other server
   */
  public void validationForFailOver(){
    assertTrue(" primary server is not detected ",primary.intValue() != -1);
    if (primary.intValue() == PORT1) {
      server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServer");
      server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "stopCacheServer");
      server2
          .invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServer");
    }
    else {
      server2
          .invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServer");
      server2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "stopCacheServer");
      server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServer");
    }
  }
  
  /*
   * find out primary and stop it
   * register CQ whould happened on other server
   */
  public void validationForCQFiredFromPoolFailOver(){
    assertTrue(" primary server is not detected ",primary.intValue() != -1);
    if (primary.intValue() == PORT1) {
      server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServerForCqRegistrationFromPool");
      server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "stopCacheServer");
      server2
          .invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServerForCqRegistrationFromPool");
    }
    else {
      server2
          .invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServerForCqRegistrationFromPool");
      server2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "stopCacheServer");
      server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "validationOnServerForCqRegistrationFromPool");
    }
  }
  /*
   * kind of teardown for testcase with failover
   */
  public void tearDownForFailOver() {
    // close the clients first
    client2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "closeCache");
    // then close the servers
    server.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "closeCache");
    server2.invoke(DeltaToRegionRelationCQRegistrationDUnitTest.class, "closeCache");
  }
  /*
   * create cache with properties
   */
  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }
  
 
}
