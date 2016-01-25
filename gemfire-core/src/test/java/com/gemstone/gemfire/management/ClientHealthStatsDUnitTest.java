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
package com.gemstone.gemfire.management;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.ha.Bug48571DUnitTest;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

/**
 * Client health stats check
 * 
 * @author rishim
 * 
 */
public class ClientHealthStatsDUnitTest extends DistributedTestCase {

  private static final String k1 = "k1";

  private static final String k2 = "k2";

  private static final String client_k1 = "client-k1";

  private static final String client_k2 = "client-k2";

  /** name of the test region */
  private static final String REGION_NAME = "ClientHealthStatsDUnitTest_Region";

  private static VM server = null;

  private static VM client = null;
  
  private static VM client2 = null;

  private static VM managingNode = null;

  private static ManagementTestBase helper = new ManagementTestBase("ClientHealthStatsDUnitTest_Helper");
  
  private static int numOfCreates = 0;
  private static int numOfUpdates = 0;
  private static int numOfInvalidates = 0;
  private static boolean lastKeyReceived = false;
  
  private static GemFireCacheImpl cache = null;

  public ClientHealthStatsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    disconnectAllFromDS();
    super.setUp();
    final Host host = Host.getHost(0);    
    managingNode = host.getVM(0);
    server = host.getVM(1);
    client = host.getVM(2);
    client2 = host.getVM(3);
    addExpectedException("Connection reset");
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    reset();
    helper.closeCache(managingNode);
    helper.closeCache(client);
    helper.closeCache(client2);
    helper.closeCache(server);

    disconnectAllFromDS();
  }
  
  public static void reset() throws Exception {
    lastKeyReceived = false;
    numOfCreates = 0;
    numOfUpdates = 0;
    numOfInvalidates = 0;
  }

  private static final long serialVersionUID = 1L;

  public void testClientHealthStats_SubscriptionEnabled() throws Exception {

    helper.createManagementCache(managingNode);
    helper.startManagingNode(managingNode);

    int port = (Integer) server.invoke(ClientHealthStatsDUnitTest.class, "createServerCache");

    DistributedMember serverMember = helper.getMember(server);

    client.invoke(ClientHealthStatsDUnitTest.class, "createClientCache", new Object[] {server.getHost(), port, 1, true, false});
    
    client2.invoke(ClientHealthStatsDUnitTest.class, "createClientCache", new Object[] {server.getHost(), port, 2, true, false});

    client.invoke(ClientHealthStatsDUnitTest.class, "put");
    client2.invoke(ClientHealthStatsDUnitTest.class, "put");
    
    managingNode.invoke(ClientHealthStatsDUnitTest.class, "verifyClientStats", new Object[] {serverMember, port, 2});
    helper.stopManagingNode(managingNode);
  }
  
  public void testClientHealthStats_SubscriptionDisabled() throws Exception {

    helper.createManagementCache(managingNode);
    helper.startManagingNode(managingNode);

    int port = (Integer) server.invoke(ClientHealthStatsDUnitTest.class, "createServerCache");

    DistributedMember serverMember = helper.getMember(server);

    client.invoke(ClientHealthStatsDUnitTest.class, "createClientCache", new Object[] {server.getHost(), port, 1, false, false});
    
    client2.invoke(ClientHealthStatsDUnitTest.class, "createClientCache", new Object[] {server.getHost(), port, 2, false, false});

    client.invoke(ClientHealthStatsDUnitTest.class, "put");
    client2.invoke(ClientHealthStatsDUnitTest.class, "put");
    
    managingNode.invoke(ClientHealthStatsDUnitTest.class, "verifyClientStats", new Object[] {serverMember, port, 0});
    helper.stopManagingNode(managingNode);
  }
  
  public void testClientHealthStats_DurableClient() throws Exception {

    helper.createManagementCache(managingNode);
    helper.startManagingNode(managingNode);

    int port = (Integer) server.invoke(ClientHealthStatsDUnitTest.class, "createServerCache");

    DistributedMember serverMember = helper.getMember(server);

    client.invoke(ClientHealthStatsDUnitTest.class, "createClientCache", new Object[] {server.getHost(), port, 1, true, true});
    
    client2.invoke(ClientHealthStatsDUnitTest.class, "createClientCache", new Object[] {server.getHost(), port, 2, true, true});

    client.invoke(ClientHealthStatsDUnitTest.class, "put");
    client2.invoke(ClientHealthStatsDUnitTest.class, "put");
    
    client.invoke(ClientHealthStatsDUnitTest.class, "closeClientCache");
    
    client2.invoke(ClientHealthStatsDUnitTest.class, "closeClientCache");
    
    managingNode.invoke(ClientHealthStatsDUnitTest.class, "verifyClientStats", new Object[] {serverMember, port, 2});
    helper.stopManagingNode(managingNode);
  }
  
  public void testStatsMatchWithSize() throws Exception {
    // start a server
    int port = (Integer) server.invoke(ClientHealthStatsDUnitTest.class, "createServerCache");
    // create durable client, with durable RI
    client.invoke(ClientHealthStatsDUnitTest.class, "createClientCache", new Object[] {server.getHost(), port, 1, true, false});
    // do puts on server from three different threads, pause after 500 puts each.
    server.invoke(ClientHealthStatsDUnitTest.class, "doPuts");
    // close durable client
    client.invoke(ClientHealthStatsDUnitTest.class, "closeClientCache");
    // resume puts on server, add another 100.
    server.invokeAsync(ClientHealthStatsDUnitTest.class, "resumePuts");
    // start durable client
    client.invoke(ClientHealthStatsDUnitTest.class, "createClientCache", new Object[] {server.getHost(), port, 1, true, false});
    // wait for full queue dispatch
    client.invoke(ClientHealthStatsDUnitTest.class, "waitForLastKey");
    // verify the stats
    server.invoke(ClientHealthStatsDUnitTest.class, "verifyStats",new Object[] {port});
  }
  
  public static int createServerCache() throws Exception {
    Cache cache = helper.createCache(false);

    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setConcurrencyChecksEnabled(false);
    rf.create(REGION_NAME);

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.start();
    return server1.getPort();
  }

  
  
  public static void closeClientCache() throws Exception {
    cache.close(true);
  }

  public static void createClientCache(Host host, Integer port, int clientNum, boolean subscriptionEnabled, boolean durable) throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME, "durable-"+clientNum);
    props.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME, "300000");

    props.setProperty("log-file", testName+"_client_" + clientNum + ".log");
    props.setProperty("log-level", "info");
    props.setProperty("statistic-archive-file", testName+"_client_" + clientNum
        + ".gfs");
    props.setProperty("statistic-sampling-enabled", "true");

    ClientCacheFactory ccf = new ClientCacheFactory(props);
    if(subscriptionEnabled){
      ccf.setPoolSubscriptionEnabled(true);
      ccf.setPoolSubscriptionAckInterval(50);
      ccf.setPoolSubscriptionRedundancy(0);
    }
    
    if(durable){
      ccf.set("durable-client-id", "DurableClientId_"+clientNum);
      ccf.set("durable-client-timeout", "" + 300);
    }

    ccf.addPoolServer(host.getHostName(), port);
    cache = (GemFireCacheImpl) ccf.create();

    ClientRegionFactory<String, String> crf = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
    crf.setConcurrencyChecksEnabled(false);

    crf.addCacheListener(new CacheListenerAdapter<String, String>() {
      public void afterInvalidate(EntryEvent<String, String> event) {
        cache.getLoggerI18n().fine(
            "Invalidate Event: " + event.getKey() + ", " + event.getNewValue());
        numOfInvalidates++;
      }
      public void afterCreate(EntryEvent<String, String> event) {
        if (((String) event.getKey()).equals("last_key")) {
          lastKeyReceived = true;
        }
        cache.getLoggerI18n().fine(
            "Create Event: " + event.getKey() + ", " + event.getNewValue());
        numOfCreates++;
      }
      public void afterUpdate(EntryEvent<String, String> event) {
        cache.getLoggerI18n().fine(
            "Update Event: " + event.getKey() + ", " + event.getNewValue());
        numOfUpdates++;
      }
    });

    Region<String, String> r = crf.create(REGION_NAME);
    if(subscriptionEnabled){
      r.registerInterest("ALL_KEYS", true);
      cache.readyForEvents();
    }

  }

  public static void doPuts() throws Exception {
    Cache cache = GemFireCacheImpl.getInstance();
    final Region<String, String> r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    Thread t1 = new Thread(new Runnable() {
      public void run() {
        for (int i = 0; i < 500; i++) {
          r.put("T1_KEY_"+i, "VALUE_"+i);
        }
      }
    });
    Thread t2 = new Thread(new Runnable() {
      public void run() {
        for (int i = 0; i < 500; i++) {
          r.put("T2_KEY_"+i, "VALUE_"+i);
        }
      }
    });
    Thread t3 = new Thread(new Runnable() {
      public void run() {
        for (int i = 0; i < 500; i++) {
          r.put("T3_KEY_"+i, "VALUE_"+i);
        }
      }
    });

    t1.start();
    t2.start();
    t3.start();

    t1.join();
    t2.join();
    t3.join();
  }
  
  public static void resumePuts() {
    Cache cache = GemFireCacheImpl.getInstance();
    Region<String, String> r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    for (int i = 0; i < 100; i++) {
      r.put("NEWKEY_"+i, "NEWVALUE_"+i);
    }
    r.put("last_key", "last_value");
  }

  public static void waitForLastKey() {
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return lastKeyReceived;
      }
      @Override
      public String description() {
        return "Did not receive last key.";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60*1000, 500, true);
  }


  @SuppressWarnings("serial")
  protected static DistributedMember getMember() throws Exception {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    return cache.getDistributedSystem().getDistributedMember();
  }

  protected static void verifyClientStats(DistributedMember serverMember, int serverPort, int numSubscriptions) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    try {
      ManagementService service = ManagementService.getExistingManagementService(cache);
      CacheServerMXBean bean = MBeanUtil.getCacheServerMbeanProxy(serverMember, serverPort);

      String[] clientIds = bean.getClientIds();
      assertTrue(clientIds.length == 2);
      getLogWriter().info("<ExpectedString> ClientId-1 of the Server is  " + clientIds[0] + "</ExpectedString> ");
      getLogWriter().info("<ExpectedString> ClientId-2 of the Server is  " + clientIds[1] + "</ExpectedString> ");
      
      ClientHealthStatus[] clientStatuses = bean.showAllClientStats();

 
      
      ClientHealthStatus clientStatus1 = bean.showClientStats(clientIds[0]);
      ClientHealthStatus clientStatus2 = bean.showClientStats(clientIds[1]);
      assertNotNull(clientStatus1);
      assertNotNull(clientStatus2);
      getLogWriter().info("<ExpectedString> ClientStats-1 of the Server is  " + clientStatus1 + "</ExpectedString> ");
      getLogWriter().info("<ExpectedString> ClientStats-2 of the Server is  " + clientStatus2 + "</ExpectedString> ");

      getLogWriter().info("<ExpectedString> clientStatuses " + clientStatuses + "</ExpectedString> ");
      assertNotNull(clientStatuses);
      
      assertTrue(clientStatuses.length == 2);
      for (ClientHealthStatus status : clientStatuses) {
        getLogWriter().info("<ExpectedString> ClientStats of the Server is  " + status + "</ExpectedString> ");

      }


      DistributedSystemMXBean dsBean = service.getDistributedSystemMXBean();
      assertEquals(2, dsBean.getNumClients());
      assertEquals(numSubscriptions, dsBean.getNumSubscriptions());

    } catch (Exception e) {
      e.printStackTrace();
      fail("Error while verifying cache server from remote member " + e);
    }

  }

  protected static void put() {
    Cache cache = GemFireCacheImpl.getInstance();
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);

    r1.put(k1, client_k1);
    assertEquals(r1.getEntry(k1).getValue(), client_k1);
    r1.put(k2, client_k2);
    assertEquals(r1.getEntry(k2).getValue(), client_k2);
    try {
      Thread.sleep(10000);
    } catch (Exception e) {
      // sleep
    }
    r1.clear();

    r1.put(k1, client_k1);
    assertEquals(r1.getEntry(k1).getValue(), client_k1);
    r1.put(k2, client_k2);
    assertEquals(r1.getEntry(k2).getValue(), client_k2);
    r1.clear();
    try {
      Thread.sleep(10000);
    } catch (Exception e) {
      // sleep
    }

  }


  public static void verifyStats(int serverPort) throws Exception {
    Cache cache = GemFireCacheImpl.getInstance();
    ManagementService service = ManagementService.getExistingManagementService(cache);
    CacheServerMXBean serverBean = service.getLocalCacheServerMXBean(serverPort);
    CacheClientNotifier ccn = CacheClientNotifier.getInstance();
    CacheClientProxy ccp = ccn.getClientProxies().iterator().next();
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getQueueSize() " + ccp.getQueueSize());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getQueueSizeStat() " + ccp.getQueueSizeStat());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getEventsEnqued() " + ccp.getHARegionQueue().getStatistics().getEventsEnqued());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getEventsDispatched() " + ccp.getHARegionQueue().getStatistics().getEventsDispatched());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getEventsRemoved() " + ccp.getHARegionQueue().getStatistics().getEventsRemoved());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getNumVoidRemovals() " + ccp.getHARegionQueue().getStatistics().getNumVoidRemovals());
    assertEquals(ccp.getQueueSize(), ccp.getQueueSizeStat());
    ClientQueueDetail queueDetails = serverBean.showClientQueueDetails()[0];
    assertEquals(queueDetails.getQueueSize(), ccp.getQueueSizeStat());
  }

}
