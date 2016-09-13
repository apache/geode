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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Client health stats check
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class ClientHealthStatsDUnitTest extends JUnit4DistributedTestCase {

  private static final String k1 = "k1";
  private static final String k2 = "k2";
  private static final String client_k1 = "client-k1";
  private static final String client_k2 = "client-k2";

  /** name of the test region */
  private static final String REGION_NAME = "ClientHealthStatsDUnitTest_Region";

  private static VM client = null;
  private static VM client2 = null;
  private static VM managingNode = null;

  private static ManagementTestBase helper = new ManagementTestBase(){};
  
  private static int numOfCreates = 0;
  private static int numOfUpdates = 0;
  private static int numOfInvalidates = 0;
  private static boolean lastKeyReceived = false;
  
  private static GemFireCacheImpl cache = null;

  private VM server = null;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    managingNode = host.getVM(0);
    server = host.getVM(1);
    client = host.getVM(2);
    client2 = host.getVM(3);

    IgnoredException.addIgnoredException("Connection reset");
  }

  @Override
  public final void preTearDown() throws Exception {
    reset();
    helper.closeCache(managingNode);
    helper.closeCache(client);
    helper.closeCache(client2);
    helper.closeCache(server);

    disconnectAllFromDS();
  }

  private static void reset() throws Exception {
    lastKeyReceived = false;
    numOfCreates = 0;
    numOfUpdates = 0;
    numOfInvalidates = 0;
  }

  @Test
  public void testClientHealthStats_SubscriptionEnabled() throws Exception {
    helper.createManagementCache(managingNode);
    helper.startManagingNode(managingNode);

    int port = (Integer) server.invoke(() -> ClientHealthStatsDUnitTest.createServerCache());

    DistributedMember serverMember = helper.getMember(server);

    client.invoke(() -> ClientHealthStatsDUnitTest.createClientCache(server.getHost(), port, 1, true, false));
    
    client2.invoke(() -> ClientHealthStatsDUnitTest.createClientCache(server.getHost(), port, 2, true, false));

    client.invoke(() -> ClientHealthStatsDUnitTest.put());
    client2.invoke(() -> ClientHealthStatsDUnitTest.put());
    
    managingNode.invoke(() -> ClientHealthStatsDUnitTest.verifyClientStats(serverMember, port, 2));
    helper.stopManagingNode(managingNode);
  }
  
  @Test
  public void testClientHealthStats_SubscriptionDisabled() throws Exception {
    helper.createManagementCache(managingNode);
    helper.startManagingNode(managingNode);

    int port = (Integer) server.invoke(() -> ClientHealthStatsDUnitTest.createServerCache());

    DistributedMember serverMember = helper.getMember(server);

    client.invoke(() -> ClientHealthStatsDUnitTest.createClientCache(server.getHost(), port, 1, false, false));
    
    client2.invoke(() -> ClientHealthStatsDUnitTest.createClientCache(server.getHost(), port, 2, false, false));

    client.invoke(() -> ClientHealthStatsDUnitTest.put());
    client2.invoke(() -> ClientHealthStatsDUnitTest.put());
    
    managingNode.invoke(() -> ClientHealthStatsDUnitTest.verifyClientStats(serverMember, port, 0));
    helper.stopManagingNode(managingNode);
  }
  
  @Test
  public void testClientHealthStats_DurableClient() throws Exception {
    helper.createManagementCache(managingNode);
    helper.startManagingNode(managingNode);

    int port = (Integer) server.invoke(() -> ClientHealthStatsDUnitTest.createServerCache());

    DistributedMember serverMember = helper.getMember(server);

    client.invoke(() -> ClientHealthStatsDUnitTest.createClientCache(server.getHost(), port, 1, true, true));
    
    client2.invoke(() -> ClientHealthStatsDUnitTest.createClientCache(server.getHost(), port, 2, true, true));

    client.invoke(() -> ClientHealthStatsDUnitTest.put());
    client2.invoke(() -> ClientHealthStatsDUnitTest.put());
    
    client.invoke(() -> ClientHealthStatsDUnitTest.closeClientCache());
    
    client2.invoke(() -> ClientHealthStatsDUnitTest.closeClientCache());
    
    managingNode.invoke(() -> ClientHealthStatsDUnitTest.verifyClientStats(serverMember, port, 2));
    helper.stopManagingNode(managingNode);
  }
  
  @Test
  public void testStatsMatchWithSize() throws Exception {
    // start a server
    int port = (Integer) server.invoke(() -> ClientHealthStatsDUnitTest.createServerCache());
    // create durable client, with durable RI
    client.invoke(() -> ClientHealthStatsDUnitTest.createClientCache(server.getHost(), port, 1, true, false));
    // do puts on server from three different threads, pause after 500 puts each.
    server.invoke(() -> ClientHealthStatsDUnitTest.doPuts());
    // close durable client
    client.invoke(() -> ClientHealthStatsDUnitTest.closeClientCache());
    
    server.invoke("verifyProxyHasBeenPaused", () -> verifyProxyHasBeenPaused() );
    // resume puts on server, add another 100.
    server.invokeAsync(() -> ClientHealthStatsDUnitTest.resumePuts());
    // start durable client
    client.invoke(() -> ClientHealthStatsDUnitTest.createClientCache(server.getHost(), port, 1, true, false));
    // wait for full queue dispatch
    client.invoke(() -> ClientHealthStatsDUnitTest.waitForLastKey());
    // verify the stats
    server.invoke(() -> ClientHealthStatsDUnitTest.verifyStats(port));
  }
  
  private static void verifyProxyHasBeenPaused() {	  
	  
	  WaitCriterion criterion = new WaitCriterion() {
      
      @Override
      public boolean done() {
        CacheClientNotifier ccn = CacheClientNotifier.getInstance();
        Collection<CacheClientProxy> ccProxies = ccn.getClientProxies();
        
        Iterator<CacheClientProxy> itr =  ccProxies.iterator();
        
        while(itr.hasNext()) {
          CacheClientProxy ccp = itr.next(); 
          System.out.println("proxy status " + ccp.getState());
          if(ccp.isPaused())
            return true;
        }
        return false;
      }
      
      @Override
      public String description() {
        return "Proxy has not paused yet";
      }
    };
    
    Wait.waitForCriterion(criterion, 15 * 1000, 200, true);	  
  }

  private static int createServerCache() throws Exception {
    Cache cache = helper.createCache(false);

    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setConcurrencyChecksEnabled(false);
    rf.create(REGION_NAME);

    CacheServer server1 = cache.addCacheServer();
    server1.setPort(0);
    server1.start();
    return server1.getPort();
  }

  private static void closeClientCache() throws Exception {
    cache.close(true);
  }

  private static void createClientCache(Host host, Integer port, int clientNum, boolean subscriptionEnabled, boolean durable) throws Exception {
    Properties props = new Properties();
    props.setProperty(DURABLE_CLIENT_ID, "durable-"+clientNum);
    props.setProperty(DURABLE_CLIENT_TIMEOUT, "300000");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(STATISTIC_ARCHIVE_FILE, getTestMethodName() + "_client_" + clientNum + ".gfs");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");

    ClientCacheFactory ccf = new ClientCacheFactory(props);
    if(subscriptionEnabled){
      ccf.setPoolSubscriptionEnabled(true);
      ccf.setPoolSubscriptionAckInterval(50);
      ccf.setPoolSubscriptionRedundancy(0);
    }
    
    if(durable){
      ccf.set(DURABLE_CLIENT_ID, "DurableClientId_" + clientNum);
      ccf.set(DURABLE_CLIENT_TIMEOUT, "" + 300);
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

  private static void doPuts() throws Exception {
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

  private static void resumePuts() {
    Cache cache = GemFireCacheImpl.getInstance();
    Region<String, String> r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    for (int i = 0; i < 100; i++) {
      r.put("NEWKEY_"+i, "NEWVALUE_"+i);
    }
    r.put("last_key", "last_value");
  }

  private static void waitForLastKey() {
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
    Wait.waitForCriterion(wc, 60*1000, 500, true);
  }

  private static DistributedMember getMember() throws Exception {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    return cache.getDistributedSystem().getDistributedMember();
  }

  private static void verifyClientStats(DistributedMember serverMember, int serverPort, int numSubscriptions) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    try {
      ManagementService service = ManagementService.getExistingManagementService(cache);
      CacheServerMXBean bean = MBeanUtil.getCacheServerMbeanProxy(serverMember, serverPort);

      String[] clientIds = bean.getClientIds();
      assertTrue(clientIds.length == 2);
      System.out.println("<ExpectedString> ClientId-1 of the Server is  " + clientIds[0] + "</ExpectedString> ");
      System.out.println("<ExpectedString> ClientId-2 of the Server is  " + clientIds[1] + "</ExpectedString> ");
      
      ClientHealthStatus[] clientStatuses = bean.showAllClientStats();

      ClientHealthStatus clientStatus1 = bean.showClientStats(clientIds[0]);
      ClientHealthStatus clientStatus2 = bean.showClientStats(clientIds[1]);
      assertNotNull(clientStatus1);
      assertNotNull(clientStatus2);
      System.out.println("<ExpectedString> ClientStats-1 of the Server is  " + clientStatus1 + "</ExpectedString> ");
      System.out.println("<ExpectedString> ClientStats-2 of the Server is  " + clientStatus2 + "</ExpectedString> ");

      System.out.println("<ExpectedString> clientStatuses " + clientStatuses + "</ExpectedString> ");
      assertNotNull(clientStatuses);
      
      assertTrue(clientStatuses.length == 2);
      for (ClientHealthStatus status : clientStatuses) {
        System.out.println("<ExpectedString> ClientStats of the Server is  " + status + "</ExpectedString> ");
      }

      DistributedSystemMXBean dsBean = service.getDistributedSystemMXBean();
      assertEquals(2, dsBean.getNumClients());
      assertEquals(numSubscriptions, dsBean.getNumSubscriptions());

    } catch (Exception e) {
      fail("Error while verifying cache server from remote member", e);
    }
  }

  private static void put() {
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

  private static void verifyStats(int serverPort) throws Exception {
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
