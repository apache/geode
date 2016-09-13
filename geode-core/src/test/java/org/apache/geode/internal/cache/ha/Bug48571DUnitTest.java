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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class Bug48571DUnitTest extends JUnit4DistributedTestCase {

  private static VM server = null;
  private VM client = null;
  private static GemFireCacheImpl cache = null;
  
  private static final String region = Bug48571DUnitTest.class.getSimpleName() + "_region";
  private static int numOfCreates = 0;
  private static int numOfUpdates = 0;
  private static int numOfInvalidates = 0;
  private static boolean lastKeyReceived = false;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    client = host.getVM(1);
  }
  
  @Override
  public final void preTearDown() throws Exception {
    reset();
    server.invoke(() -> Bug48571DUnitTest.reset());
    client.invoke(() -> Bug48571DUnitTest.reset());
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

  private static void verifyProxyHasBeenPaused() {
    WaitCriterion criterion = new WaitCriterion() {
      @Override
      public boolean done() {
        CacheClientNotifier ccn = CacheClientNotifier.getInstance();
        Collection<CacheClientProxy> ccProxies = ccn.getClientProxies();

        Iterator<CacheClientProxy> itr = ccProxies.iterator();

        while (itr.hasNext()) {
          CacheClientProxy ccp = itr.next();
          System.out.println("proxy status " + ccp.getState());
          if (ccp.isPaused())
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
  
  @Test
  public void testStatsMatchWithSize() throws Exception {
    IgnoredException.addIgnoredException("Unexpected IOException||Connection reset");
    // start a server
    int port = (Integer) server.invoke(() -> Bug48571DUnitTest.createServerCache());
    // create durable client, with durable RI
    client.invoke(() -> Bug48571DUnitTest.createClientCache(client.getHost(), port));
    // do puts on server from three different threads, pause after 500 puts each.
    server.invoke(() -> Bug48571DUnitTest.doPuts());
    // close durable client
    client.invoke(() -> Bug48571DUnitTest.closeClientCache());
    
    server.invoke("verifyProxyHasBeenPaused", () -> verifyProxyHasBeenPaused() );
    // resume puts on server, add another 100.
    server.invokeAsync(() -> Bug48571DUnitTest.resumePuts()); // TODO: join or await result
    // start durable client
    client.invoke(() -> Bug48571DUnitTest.createClientCache(client.getHost(), port));
    // wait for full queue dispatch
    client.invoke(() -> Bug48571DUnitTest.waitForLastKey());
    // verify the stats
    server.invoke(() -> Bug48571DUnitTest.verifyStats());
  }

  public static int createServerCache() throws Exception {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    props.setProperty(LOG_FILE, "server_" + OSProcess.getId() + ".log");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(STATISTIC_ARCHIVE_FILE, "server_" + OSProcess.getId()
        + ".gfs");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    CacheFactory cf = new CacheFactory(props);

    DistributedSystem ds = new Bug48571DUnitTest().getSystem(props);
    ds.disconnect();

    cache = (GemFireCacheImpl)cf.create();

    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setConcurrencyChecksEnabled(false);
    rf.create(region);

    CacheServer server1 = cache.addCacheServer();
    server1.setPort(0);
    server1.start();
    return server1.getPort();
  }

  public static void closeClientCache() throws Exception {
    cache.close(true);
  }

  public static void createClientCache(Host host, Integer port) throws Exception {

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(DURABLE_CLIENT_ID, "durable-48571");
    props.setProperty(DURABLE_CLIENT_TIMEOUT, "300000");

    props.setProperty(LOG_FILE, "client_" + OSProcess.getId() + ".log");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(STATISTIC_ARCHIVE_FILE, "client_" + OSProcess.getId()
        + ".gfs");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");

    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.setPoolSubscriptionEnabled(true);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionRedundancy(0);
    ccf.addPoolServer(host.getHostName(), port);

    DistributedSystem ds = new Bug48571DUnitTest().getSystem(props);
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
