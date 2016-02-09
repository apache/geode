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
package com.gemstone.gemfire.internal.cache.ha;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

public class Bug48571DUnitTest extends DistributedTestCase {

  private static VM server = null;
  private static VM client = null;
  private static GemFireCacheImpl cache = null;
  
  private static final String region = "Bug48571DUnitTest_region";
  private static int numOfCreates = 0;
  private static int numOfUpdates = 0;
  private static int numOfInvalidates = 0;
  private static boolean lastKeyReceived = false;

  public Bug48571DUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    client = host.getVM(1);
  }
  
  @Override
  protected final void preTearDown() throws Exception {
    reset();
    server.invoke(Bug48571DUnitTest.class, "reset");
    client.invoke(Bug48571DUnitTest.class, "reset");
  }

  public static void reset() throws Exception {
    lastKeyReceived = false;
    numOfCreates = 0;
    numOfUpdates = 0;
    numOfInvalidates = 0;
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public void testStatsMatchWithSize() throws Exception {
    IgnoredException.addIgnoredException("Unexpected IOException||Connection reset");
    // start a server
    int port = (Integer) server.invoke(Bug48571DUnitTest.class, "createServerCache");
    // create durable client, with durable RI
    client.invoke(Bug48571DUnitTest.class, "createClientCache", new Object[] {client.getHost(), port});
    // do puts on server from three different threads, pause after 500 puts each.
    server.invoke(Bug48571DUnitTest.class, "doPuts");
    // close durable client
    client.invoke(Bug48571DUnitTest.class, "closeClientCache");
    // resume puts on server, add another 100.
    server.invokeAsync(Bug48571DUnitTest.class, "resumePuts");
    // start durable client
    client.invoke(Bug48571DUnitTest.class, "createClientCache", new Object[] {client.getHost(), port});
    // wait for full queue dispatch
    client.invoke(Bug48571DUnitTest.class, "waitForLastKey");
    // verify the stats
    server.invoke(Bug48571DUnitTest.class, "verifyStats");
  }


  public static int createServerCache() throws Exception {
    Properties props = new Properties();
    props.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
    props.setProperty("log-file", "server_" + OSProcess.getId() + ".log");
    props.setProperty("log-level", "info");
    props.setProperty("statistic-archive-file", "server_" + OSProcess.getId()
        + ".gfs");
    props.setProperty("statistic-sampling-enabled", "true");
    CacheFactory cf = new CacheFactory(props);

    DistributedSystem ds = new Bug48571DUnitTest("Bug48571DUnitTest").getSystem(props);
    ds.disconnect();

    cache = (GemFireCacheImpl)cf.create();

    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setConcurrencyChecksEnabled(false);
    rf.create(region);

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.start();
    return server1.getPort();
  }

  public static void closeClientCache() throws Exception {
    cache.close(true);
  }

  public static void createClientCache(Host host, Integer port) throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    props.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME, "durable-48571");
    props.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME, "300000");

    props.setProperty("log-file", "client_" + OSProcess.getId() + ".log");
    props.setProperty("log-level", "info");
    props.setProperty("statistic-archive-file", "client_" + OSProcess.getId()
        + ".gfs");
    props.setProperty("statistic-sampling-enabled", "true");

    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.setPoolSubscriptionEnabled(true);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionRedundancy(0);
    ccf.addPoolServer(host.getHostName(), port);

    DistributedSystem ds = new Bug48571DUnitTest("Bug48571DUnitTest").getSystem(props);
    ds.disconnect();

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

    Region<String, String> r = crf.create(region);
    r.registerInterest("ALL_KEYS", true);
    cache.readyForEvents();
  }

  public static void doPuts() throws Exception {
    final Region<String, String> r = cache.getRegion(region);
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
    Region<String, String> r = cache.getRegion(region);
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
    Wait.waitForCriterion(wc, 60*1000, 500, true);
  }


  public static void verifyStats() throws Exception {
    CacheClientNotifier ccn = CacheClientNotifier.getInstance();
    CacheClientProxy ccp = ccn.getClientProxies().iterator().next();
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getQueueSize() " + ccp.getQueueSize());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getQueueSizeStat() " + ccp.getQueueSizeStat());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getEventsEnqued() " + ccp.getHARegionQueue().getStatistics().getEventsEnqued());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getEventsDispatched() " + ccp.getHARegionQueue().getStatistics().getEventsDispatched());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getEventsRemoved() " + ccp.getHARegionQueue().getStatistics().getEventsRemoved());
    cache.getLoggerI18n().info(LocalizedStrings.DEBUG, "getNumVoidRemovals() " + ccp.getHARegionQueue().getStatistics().getNumVoidRemovals());
    assertEquals(ccp.getQueueSize(), ccp.getQueueSizeStat());
  }
}
