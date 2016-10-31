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
package org.apache.geode.management;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Host.*;
import static org.apache.geode.test.dunit.IgnoredException.*;
import static org.apache.geode.test.dunit.Invoke.*;
import static org.apache.geode.test.dunit.NetworkUtils.*;
import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Properties;

import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Client health stats check
 */
@Category(DistributedTest.class)
@SuppressWarnings({"serial", "unused"})
public class ClientHealthStatsDUnitTest implements Serializable {

  private static final int NUMBER_PUTS = 100;

  private static final String KEY1 = "KEY1";
  private static final String KEY2 = "KEY2";
  private static final String VALUE1 = "VALUE1";
  private static final String VALUE2 = "VALUE2";

  private static final String REGION_NAME =
      ClientHealthStatsDUnitTest.class.getSimpleName() + "_Region";

  // client1VM and client2VM VM fields
  private static ClientCache clientCache;

  // TODO: assert following values in each client VM
  private static int numOfCreates;
  private static int numOfUpdates;
  private static int numOfInvalidates;
  private static boolean lastKeyReceived;

  private VM managerVM;
  private VM serverVM;
  private VM client1VM;
  private VM client2VM;

  private String hostName;

  @Rule
  public ManagementTestRule managementTestRule = ManagementTestRule.builder().build();

  @Before
  public void before() throws Exception {
    this.hostName = getServerHostName(getHost(0));

    this.managerVM = getHost(0).getVM(0);
    this.serverVM = getHost(0).getVM(1);
    this.client1VM = getHost(0).getVM(2);
    this.client2VM = getHost(0).getVM(3);

    addIgnoredException("Connection reset");
  }

  @After
  public void after() throws Exception {
    invokeInEveryVM(() -> {
      lastKeyReceived = false;
      numOfCreates = 0;
      numOfUpdates = 0;
      numOfInvalidates = 0;
      clientCache = null;
    });
  }

  @Test
  public void testClientHealthStats_SubscriptionEnabled() throws Exception {
    this.managementTestRule.createManager(this.managerVM, false);
    this.managementTestRule.startManager(this.managerVM);

    int port = this.serverVM.invoke(() -> createServerCache());

    this.client1VM.invoke(() -> createClientCache(this.hostName, port, 1, true));
    this.client2VM.invoke(() -> createClientCache(this.hostName, port, 2, true));

    this.client1VM.invoke(() -> put());
    this.client2VM.invoke(() -> put());

    DistributedMember serverMember = this.managementTestRule.getDistributedMember(this.serverVM);
    this.managerVM.invoke(() -> verifyClientStats(serverMember, port, 2));

    this.managementTestRule.stopManager(this.managerVM);
  }

  @Test
  public void testClientHealthStats_SubscriptionDisabled() throws Exception {
    this.managementTestRule.createManager(this.managerVM, false);
    this.managementTestRule.startManager(this.managerVM);

    int port = this.serverVM.invoke(() -> createServerCache());

    this.client1VM.invoke(() -> createClientCache(this.hostName, port, 1, false));
    this.client2VM.invoke(() -> createClientCache(this.hostName, port, 2, false));

    this.client1VM.invoke(() -> put());
    this.client2VM.invoke(() -> put());

    DistributedMember serverMember = this.managementTestRule.getDistributedMember(this.serverVM);
    this.managerVM.invoke(() -> verifyClientStats(serverMember, port, 0));
    this.managementTestRule.stopManager(this.managerVM);
  }

  @Test
  public void testClientHealthStats_DurableClient() throws Exception {
    this.managementTestRule.createManager(this.managerVM, false);
    this.managementTestRule.startManager(this.managerVM);

    int port = this.serverVM.invoke(() -> createServerCache());

    this.client1VM.invoke(() -> createClientCache(this.hostName, port, 1, true));
    this.client2VM.invoke(() -> createClientCache(this.hostName, port, 2, true));

    this.client1VM.invoke(() -> put());
    this.client2VM.invoke(() -> put());

    this.client1VM.invoke(() -> clientCache.close(true));
    this.client2VM.invoke(() -> clientCache.close(true));

    DistributedMember serverMember = this.managementTestRule.getDistributedMember(this.serverVM);
    this.managerVM.invoke(() -> verifyClientStats(serverMember, port, 2));
    this.managementTestRule.stopManager(this.managerVM);
  }

  @Test
  public void testStatsMatchWithSize() throws Exception {
    // start a serverVM
    int port = this.serverVM.invoke(() -> createServerCache());

    // create durable client1VM, with durable RI
    this.client1VM.invoke(() -> createClientCache(this.hostName, port, 1, true));

    // do puts on serverVM from three different threads, pause after 500 puts each.
    this.serverVM.invoke(() -> doPuts());

    // close durable client1VM
    this.client1VM.invoke(() -> clientCache.close(true));

    this.serverVM.invoke(() -> await().until(() -> cacheClientProxyHasBeenPause()));

    // resume puts on serverVM, add another 100.
    this.serverVM.invoke(() -> resumePuts());

    // start durable client1VM
    this.client1VM.invoke(() -> createClientCache(this.hostName, port, 1, true));

    // wait for full queue dispatch
    this.client1VM.invoke(() -> await().until(() -> lastKeyReceived));

    // verify the stats
    this.serverVM.invoke(() -> verifyStats(port));
  }

  /**
   * Invoked in serverVM
   */
  private boolean cacheClientProxyHasBeenPause() {
    CacheClientNotifier clientNotifier = CacheClientNotifier.getInstance();
    // TODO: CacheClientNotifier clientNotifier =
    // ((CacheServerImpl)this.managementTestRule.getCache().getCacheServers().get(0)).getAcceptor().getCacheClientNotifier();

    Collection<CacheClientProxy> clientProxies = clientNotifier.getClientProxies();

    for (CacheClientProxy clientProxy : clientProxies) {
      if (clientProxy.isPaused()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Invoked in serverVM
   */
  private int createServerCache() throws IOException {
    Cache cache = this.managementTestRule.getCache();

    RegionFactory<String, String> regionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.setConcurrencyChecksEnabled(false);
    regionFactory.create(REGION_NAME);

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  /**
   * Invoked in client1VM and client2VM
   */
  private void createClientCache(final String hostName, final Integer port, final int clientNum,
      final boolean subscriptionEnabled) {
    Properties props = new Properties();
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");

    ClientCacheFactory cacheFactory = new ClientCacheFactory(props);
    if (subscriptionEnabled) {
      cacheFactory.setPoolSubscriptionEnabled(true);
      cacheFactory.setPoolSubscriptionAckInterval(50);
      cacheFactory.setPoolSubscriptionRedundancy(0);
    }

    cacheFactory.set(DURABLE_CLIENT_ID, "DurableClientId_" + clientNum);
    cacheFactory.set(DURABLE_CLIENT_TIMEOUT, "" + 30000);

    cacheFactory.addPoolServer(hostName, port);
    clientCache = cacheFactory.create();

    ClientRegionFactory<String, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
    regionFactory.setConcurrencyChecksEnabled(false);

    regionFactory.addCacheListener(new CacheListenerAdapter<String, String>() {
      @Override
      public void afterInvalidate(final EntryEvent<String, String> event) {
        numOfInvalidates++;
      }

      @Override
      public void afterCreate(final EntryEvent<String, String> event) {
        if ("last_key".equals(event.getKey())) {
          lastKeyReceived = true;
        }
        numOfCreates++;
      }

      @Override
      public void afterUpdate(final EntryEvent<String, String> event) {
        numOfUpdates++;
      }
    });

    Region<String, String> region = regionFactory.create(REGION_NAME);
    if (subscriptionEnabled) {
      region.registerInterest("ALL_KEYS", true);
      clientCache.readyForEvents();
    }
  }

  /**
   * Invoked in serverVM
   */
  private void doPuts() throws InterruptedException {
    Cache cache = this.managementTestRule.getCache();
    Region<String, String> region = cache.getRegion(Region.SEPARATOR + REGION_NAME);

    Thread thread1 = new Thread(() -> {
      for (int i = 0; i < NUMBER_PUTS; i++) {
        region.put("T1_KEY_" + i, "VALUE_" + i);
      }
    });
    Thread thread2 = new Thread(() -> {
      for (int i = 0; i < NUMBER_PUTS; i++) {
        region.put("T2_KEY_" + i, "VALUE_" + i);
      }
    });
    Thread thread3 = new Thread(() -> {
      for (int i = 0; i < NUMBER_PUTS; i++) {
        region.put("T3_KEY_" + i, "VALUE_" + i);
      }
    });

    thread1.start();
    thread2.start();
    thread3.start();

    thread1.join();
    thread2.join();
    thread3.join();
  }

  /**
   * Invoked in serverVM
   */
  private void resumePuts() {
    Cache cache = this.managementTestRule.getCache();
    Region<String, String> region = cache.getRegion(Region.SEPARATOR + REGION_NAME);

    for (int i = 0; i < NUMBER_PUTS; i++) {
      region.put("NEWKEY_" + i, "NEWVALUE_" + i);
    }
    region.put("last_key", "last_value");
  }

  /**
   * Invoked in managerVM
   */
  private void verifyClientStats(final DistributedMember serverMember, final int serverPort,
      final int numSubscriptions) throws Exception {
    ManagementService service = this.managementTestRule.getManagementService();
    CacheServerMXBean cacheServerMXBean = awaitCacheServerMXBean(serverMember, serverPort);

    String[] clientIds = cacheServerMXBean.getClientIds();
    assertThat(clientIds).hasSize(2);

    ClientHealthStatus[] clientStatuses = cacheServerMXBean.showAllClientStats();

    ClientHealthStatus clientStatus1 = cacheServerMXBean.showClientStats(clientIds[0]);
    ClientHealthStatus clientStatus2 = cacheServerMXBean.showClientStats(clientIds[1]);
    assertThat(clientStatus1).isNotNull();
    assertThat(clientStatus2).isNotNull();

    assertThat(clientStatuses).isNotNull().hasSize(2);

    DistributedSystemMXBean dsBean = service.getDistributedSystemMXBean();
    assertThat(dsBean.getNumClients()).isEqualTo(2);
    assertThat(dsBean.getNumSubscriptions()).isEqualTo(numSubscriptions);
  }

  /**
   * Invoked in client1VM and client2VM
   */
  private void put() {
    Cache cache = (Cache) clientCache;
    Region<String, String> region = cache.getRegion(Region.SEPARATOR + REGION_NAME);

    region.put(KEY1, VALUE1);
    assertThat(region.getEntry(KEY1).getValue()).isEqualTo(VALUE1);

    region.put(KEY2, VALUE2);
    assertThat(region.getEntry(KEY2).getValue()).isEqualTo(VALUE2);

    region.clear();

    region.put(KEY1, VALUE1);
    assertThat(region.getEntry(KEY1).getValue()).isEqualTo(VALUE1);

    region.put(KEY2, VALUE2);
    assertThat(region.getEntry(KEY2).getValue()).isEqualTo(VALUE2);

    region.clear();
  }

  /**
   * Invoked in serverVM
   */
  private void verifyStats(final int serverPort) throws Exception {
    ManagementService service = this.managementTestRule.getManagementService();
    CacheServerMXBean serverBean = service.getLocalCacheServerMXBean(serverPort);

    CacheClientNotifier clientNotifier = CacheClientNotifier.getInstance();
    CacheClientProxy clientProxy = clientNotifier.getClientProxies().iterator().next();
    assertThat(clientProxy.getQueueSizeStat()).isEqualTo(clientProxy.getQueueSize());

    ClientQueueDetail queueDetails = serverBean.showClientQueueDetails()[0];
    assertThat(clientProxy.getQueueSizeStat()).isEqualTo((int) queueDetails.getQueueSize());
  }

  private CacheServerMXBean awaitCacheServerMXBean(final DistributedMember serverMember,
      final int port) {
    SystemManagementService service = this.managementTestRule.getSystemManagementService();
    ObjectName objectName = service.getCacheServerMBeanName(port, serverMember);

    await().until(
        () -> assertThat(service.getMBeanProxy(objectName, CacheServerMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, CacheServerMXBean.class);
  }

  private ConditionFactory await() {
    return Awaitility.await().atMost(2, MINUTES);
  }
}
