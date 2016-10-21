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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class Bug48879DUnitTest extends JUnit4DistributedTestCase {

  private static VM vm0 = null;
  private static VM vm1 = null;

  private static GemFireCacheImpl cache;

  public static final String REGION_NAME = "Bug48879DUnitTest_region";

  public static final int SLEEP_TIME = 40000;

  public Bug48879DUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    vm0 = host.getVM(0); // server1
    vm1 = host.getVM(1); // server2

    int port0 = (Integer) vm0.invoke(() -> Bug48879DUnitTest.createCacheServer());
    int port1 = (Integer) vm1.invoke(() -> Bug48879DUnitTest.createCacheServer());

    createClientCache(host, new Integer[] {port0, port1}, Boolean.TRUE);
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();

    vm0.invoke(() -> Bug48879DUnitTest.closeCache());
    vm1.invoke(() -> Bug48879DUnitTest.closeCache());
  }

  public static void closeCache() throws Exception {
    HARegionQueue.threadIdExpiryTime = 300;
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "MessageTimeToLive", "180");
    if (cache != null) {
      cache.close();
    }
  }

  @SuppressWarnings({"unused", "deprecation"})
  public static Integer createCacheServer() throws Exception {
    Bug48879DUnitTest test = new Bug48879DUnitTest();
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "MessageTimeToLive", "30");
    cache = (GemFireCacheImpl) CacheFactory.create(test.getSystem());
    HARegionQueue.threadIdExpiryTime = (SLEEP_TIME / 1000) - 10;
    cache.setMessageSyncInterval(SLEEP_TIME / 500);

    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);

    Region<String, String> region = rf.create(REGION_NAME);

    CacheServer server = cache.addCacheServer();
    server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    server.start();
    return server.getPort();
  }

  @SuppressWarnings("deprecation")
  public static void createClientCache(Host host, Integer[] ports, Boolean doRI) throws Exception {

    Properties props = new Properties();
    props.setProperty(STATISTIC_ARCHIVE_FILE, "client_" + OSProcess.getId() + ".gfs");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");

    DistributedSystem ds = new Bug48879DUnitTest().getSystem(props);
    ds.disconnect();
    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.setPoolSubscriptionEnabled(doRI);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionRedundancy(1);
    for (int port : ports) {
      ccf.addPoolServer(host.getHostName(), port);
    }
    cache = (GemFireCacheImpl) ccf.create();

    ClientRegionFactory<String, String> crf =
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    Region<String, String> region = crf.create(REGION_NAME);

    if (doRI) {
      region.registerInterest("ALL_KEYS");
    }

  }

  @SuppressWarnings({"unused", "unchecked"})
  public static void doPuts(Integer numOfThreads, Integer puts) throws Exception {
    Region<String, String> region = cache.getRegion(REGION_NAME);
    final int putsPerThread = puts;

    Thread[] threads = new Thread[numOfThreads];

    for (int i = 0; i < numOfThreads; i++) {
      final String threadId = "Thread_" + i + "X";
      threads[i] = new Thread(new Runnable() {
        @SuppressWarnings("rawtypes")
        public void run() {
          Region region = cache.getRegion(REGION_NAME);
          for (int i = 0; i < putsPerThread; i++) {
            region.put(threadId + i, "VALUE_" + i);
          }
        }
      });
      threads[i].start();
    }

    for (int i = 0; i < numOfThreads; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException ie) {
      }
    }
    Thread.sleep(5000);
    region.put("LAST", "LAST");
  }

  public static Boolean isPrimaryServer() {
    return ((CacheClientProxy) CacheClientNotifier.getInstance().getClientProxies().toArray()[0])
        .isPrimary();
  }

  public static void verifyStats(Integer numOfEvents, Integer expectedTids) throws Exception {
    HARegionQueueStats stats =
        ((CacheClientProxy) CacheClientNotifier.getInstance().getClientProxies().toArray()[0])
            .getHARegionQueue().getStatistics();

    long actualExpiry = stats.getEventsExpired();
    long expectedExpiry = isPrimaryServer() ? 0 : numOfEvents + 1; // +1 for LAST key
    assertEquals(
        "Expected eventsExpired: " + expectedExpiry + " but actual eventsExpired: " + actualExpiry
            + (isPrimaryServer() ? " at primary." : " at secondary."),
        expectedExpiry, actualExpiry);

    int actualTids = stats.getThreadIdentiferCount();
    assertTrue("Expected ThreadIdentifier count <= 1 but actual: " + actualTids
        + (isPrimaryServer() ? " at primary." : " at secondary."), actualTids <= 1); // Sometimes we
                                                                                     // may see 1
                                                                                     // threadIdentifier
                                                                                     // due to slow
                                                                                     // machines,
                                                                                     // but never
                                                                                     // equal to
                                                                                     // expectedTids
  }

  public static void verifyThreadsBeforeExpiry(Integer expectedTids) throws Exception {
    HARegionQueueStats stats =
        ((CacheClientProxy) CacheClientNotifier.getInstance().getClientProxies().toArray()[0])
            .getHARegionQueue().getStatistics();

    int actualTids = stats.getThreadIdentiferCount();
    assertTrue("Expected ThreadIdentifier count >= " + expectedTids + " but actual: " + actualTids
        + (isPrimaryServer() ? " at primary." : " at secondary."), actualTids >= expectedTids);
  }

  @Test
  public void testThreadIdentfiersExpiry() throws Exception {
    // create server1 and server2
    // create client with redundancy = 1
    // put events in region
    int threads = 10;
    int putsPerThread = 1;
    vm0.invoke(() -> Bug48879DUnitTest.doPuts(threads, putsPerThread));
    vm0.invoke(() -> Bug48879DUnitTest.verifyThreadsBeforeExpiry(threads));
    vm1.invoke(() -> Bug48879DUnitTest.verifyThreadsBeforeExpiry(threads));
    // sleep till expiry time elapses
    Thread.sleep(SLEEP_TIME * 2 + 30000);

    // Assert that threadidentifiers are expired and region events are retained on primary server
    vm0.invoke(() -> Bug48879DUnitTest.verifyStats(threads * putsPerThread, threads));
    // Assert that region events and threadidentifiers are expired on secondary server.
    vm1.invoke(() -> Bug48879DUnitTest.verifyStats(threads * putsPerThread, threads));
  }
}

