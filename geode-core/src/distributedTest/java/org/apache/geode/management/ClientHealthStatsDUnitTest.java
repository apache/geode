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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.assertThat;

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

/**
 * Distributed tests for client stats exposed via {@link CacheServerMXBean}:
 * <ul>
 * <li>{@link CacheServerMXBean#showClientStats}
 * <li>{@link CacheServerMXBean#showAllClientStats}
 * <li>{@link CacheServerMXBean#showClientQueueDetails}
 * </ul>
 */

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
  private static volatile ClientCache clientCache;
  private static volatile boolean lastKeyReceived;

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
      public void afterCreate(final EntryEvent<String, String> event) {
        if ("last_key".equals(event.getKey())) {
          lastKeyReceived = true;
        }
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

    await().until(() -> cacheServerMXBean.getClientIds().length == 2);
    String[] clientIds = cacheServerMXBean.getClientIds();

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
   * Invoked in serverVM
   */
  private void verifyStats(final int serverPort) throws Exception {
    ManagementService service = this.managementTestRule.getManagementService();
    CacheServerMXBean cacheServerMXBean = service.getLocalCacheServerMXBean(serverPort);

    CacheClientNotifier clientNotifier = CacheClientNotifier.getInstance();
    CacheClientProxy clientProxy = clientNotifier.getClientProxies().iterator().next();
    assertThat(clientProxy.getQueueSizeStat()).isEqualTo(clientProxy.getQueueSize());

    ClientQueueDetail queueDetails = cacheServerMXBean.showClientQueueDetails()[0];
    assertThat((int) queueDetails.getQueueSize()).isEqualTo(clientProxy.getQueueSizeStat());
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
