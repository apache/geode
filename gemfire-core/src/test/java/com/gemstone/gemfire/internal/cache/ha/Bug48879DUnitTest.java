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

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

@SuppressWarnings("serial")
public class Bug48879DUnitTest extends DistributedTestCase {

  private static VM vm0 = null;
  private static VM vm1 = null;

  private static GemFireCacheImpl cache;

  public static final String REGION_NAME = "Bug48879DUnitTest_region";

  public static final int SLEEP_TIME = 40000;

  public Bug48879DUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    vm0 = host.getVM(0); // server1
    vm1 = host.getVM(1); // server2

    int port0 = (Integer) vm0.invoke(Bug48879DUnitTest.class,
        "createCacheServer", new Object[] { });
    int port1 = (Integer) vm1.invoke(Bug48879DUnitTest.class,
        "createCacheServer", new Object[] { });

    createClientCache(host, new Integer[] {port0, port1}, Boolean.TRUE);
  }

  public void tearDown2() throws Exception {
    closeCache();

    vm0.invoke(Bug48879DUnitTest.class, "closeCache");
    vm1.invoke(Bug48879DUnitTest.class, "closeCache");
  }

  public static void closeCache() throws Exception {
    HARegionQueue.threadIdExpiryTime = 300;
    System.setProperty("gemfire.MessageTimeToLive", "180");
    if (cache != null) {
      cache.close();
    }
  }

  @SuppressWarnings({ "unused", "deprecation" })
  public static Integer createCacheServer()
      throws Exception {
    Bug48879DUnitTest test = new Bug48879DUnitTest("Bug48879DUnitTest");
    System.setProperty("gemfire.MessageTimeToLive", "30");
    cache = (GemFireCacheImpl)CacheFactory.create(test.getSystem());
    HARegionQueue.threadIdExpiryTime = (SLEEP_TIME/1000) - 10;
    cache.setMessageSyncInterval(SLEEP_TIME/500);

    RegionFactory<String, String> rf = cache
        .createRegionFactory(RegionShortcut.REPLICATE);

    Region<String, String> region = rf.create(REGION_NAME);

    CacheServer server = cache.addCacheServer();
    server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    server.start();
    return server.getPort();
  }

  @SuppressWarnings("deprecation")
  public static void createClientCache(Host host, Integer[] ports, Boolean doRI)
      throws Exception {

    Properties props = new Properties();
    props.setProperty("statistic-archive-file", "client_" + OSProcess.getId()
        + ".gfs");
    props.setProperty("statistic-sampling-enabled", "true");

    DistributedSystem ds = new Bug48879DUnitTest("Bug48879DUnitTest").getSystem(props);
    ds.disconnect();
    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.setPoolSubscriptionEnabled(doRI);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionRedundancy(1);
    for (int port : ports) {
      ccf.addPoolServer(host.getHostName(), port);
    }
    cache = (GemFireCacheImpl) ccf.create();

    ClientRegionFactory<String, String> crf = cache
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    Region<String, String> region = crf.create(REGION_NAME);

    if (doRI) {
      region.registerInterest("ALL_KEYS");
    }

  }

  @SuppressWarnings({ "unused", "unchecked" })
  public static void doPuts(Integer numOfThreads, Integer puts)
      throws Exception {
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
      } catch (InterruptedException ie) {}
    }
    Thread.sleep(5000);
    region.put("LAST", "LAST");
  }

  public static Boolean isPrimaryServer() {
    return ((CacheClientProxy) CacheClientNotifier.getInstance()
        .getClientProxies().toArray()[0]).isPrimary();
  }

  public static void verifyStats(Integer numOfEvents,
      Integer expectedTids) throws Exception {
    HARegionQueueStats stats = ((CacheClientProxy) CacheClientNotifier
        .getInstance().getClientProxies().toArray()[0]).getHARegionQueue()
        .getStatistics();

    long actualExpiry = stats.getEventsExpired();
    long expectedExpiry = isPrimaryServer() ? 0 : numOfEvents + 1; // +1 for LAST key
    assertEquals("Expected eventsExpired: " + expectedExpiry
        + " but actual eventsExpired: " + actualExpiry
        + (isPrimaryServer() ? " at primary." : " at secondary."),
        expectedExpiry, actualExpiry);

    int actualTids = stats.getThreadIdentiferCount();
    assertTrue("Expected ThreadIdentifier count <= 1 but actual: "
        + actualTids + (isPrimaryServer() ? " at primary." : " at secondary."),
        actualTids <= 1); // Sometimes we may see 1 threadIdentifier due to slow machines, but never equal to expectedTids
  }

  public static void verifyThreadsBeforeExpiry(Integer expectedTids) throws Exception {
    HARegionQueueStats stats = ((CacheClientProxy) CacheClientNotifier
        .getInstance().getClientProxies().toArray()[0]).getHARegionQueue()
        .getStatistics();

    int actualTids = stats.getThreadIdentiferCount();
    assertTrue("Expected ThreadIdentifier count >= " + expectedTids + " but actual: "
        + actualTids + (isPrimaryServer() ? " at primary." : " at secondary."),
        actualTids >= expectedTids);
  }

  public void testThreadIdentfiersExpiry() throws Exception {
    // create server1 and server2
    // create client with redundancy = 1
    // put events in region
    int threads = 10;
    int putsPerThread = 1;
    vm0.invoke(Bug48879DUnitTest.class, "doPuts", new Object[] {threads, putsPerThread});
    vm0.invoke(Bug48879DUnitTest.class, "verifyThreadsBeforeExpiry", new Object[] {threads});
    vm1.invoke(Bug48879DUnitTest.class, "verifyThreadsBeforeExpiry", new Object[] {threads});
    // sleep till expiry time elapses
    Thread.sleep(SLEEP_TIME*2 + 30000);

    // Assert that threadidentifiers are expired and region events are retained on primary server
    vm0.invoke(Bug48879DUnitTest.class, "verifyStats", new Object[] {threads*putsPerThread, threads});
    // Assert that region events and threadidentifiers are expired on secondary server.
    vm1.invoke(Bug48879DUnitTest.class, "verifyStats", new Object[] {threads*putsPerThread, threads});
  }
}

