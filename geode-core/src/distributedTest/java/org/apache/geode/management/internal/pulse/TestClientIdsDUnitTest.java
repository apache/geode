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
package org.apache.geode.management.internal.pulse;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;

import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.ManagementTestRule;
import org.apache.geode.management.Manager;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

/**
 * This is for testing client IDs
 */

@SuppressWarnings({"serial", "unused"})
public class TestClientIdsDUnitTest implements Serializable {

  private static final String KEY1 = "KEY1";
  private static final String KEY2 = "KEY2";
  private static final String VALUE1 = "client-KEY1";
  private static final String VALUE2 = "client-KEY2";
  private static final String REGION_NAME =
      TestClientIdsDUnitTest.class.getSimpleName() + "_Region";

  @Manager
  private VM managerVM;

  private VM serverVM;
  private VM client1VM;
  private VM client2VM;

  @Rule
  public ManagementTestRule managementTestRule = ManagementTestRule.builder().start(false).build();

  @Before
  public void before() throws Exception {
    this.serverVM = Host.getHost(0).getVM(1);
    this.client1VM = Host.getHost(0).getVM(2);
    this.client2VM = Host.getHost(0).getVM(3);
  }

  @Test
  public void testClientIds() throws Exception {
    this.managementTestRule.createManagers();

    int port = this.serverVM.invoke(() -> createServerCache());
    String hostName = getServerHostName(this.serverVM.getHost());

    this.client1VM.invoke(() -> createClientCache(hostName, port));
    this.client2VM.invoke(() -> createClientCache(hostName, port));

    DistributedMember serverMember = this.managementTestRule.getDistributedMember(this.serverVM);

    this.managerVM.invoke(() -> verifyClientIds(serverMember, port));
  }

  private int createServerCache() throws IOException {
    Cache cache = this.managementTestRule.getCache();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.setNotifyBySubscription(true);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void createClientCache(final String host, final int serverPort) {
    ClientCache cache = this.managementTestRule.getClientCache();

    Pool pool = PoolManager.createFactory().addServer(host, serverPort)
        .setSubscriptionEnabled(false).setThreadLocalConnections(true).setMinConnections(1)
        .setReadTimeout(20000).setPingInterval(10000).setRetryAttempts(1)
        .setSubscriptionEnabled(true).setStatisticInterval(1000).create(getClass().getSimpleName());

    ClientRegionFactory factory =
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
    factory.setPoolName(pool.getName());
    factory.create(REGION_NAME);
  }

  private void verifyClientIds(final DistributedMember serverMember, final int serverPort)
      throws Exception {
    CacheServerMXBean cacheServerMXBean = awaitCacheServerMXBean(serverMember, serverPort);
    await().untilAsserted(() -> assertThat(cacheServerMXBean.getClientIds()).hasSize(2));
    assertThat(cacheServerMXBean.getClientIds()).hasSize(2); // TODO: write better assertions
  }

  private CacheServerMXBean awaitCacheServerMXBean(final DistributedMember serverMember,
      final int port) {
    SystemManagementService service = this.managementTestRule.getSystemManagementService();
    ObjectName objectName = service.getCacheServerMBeanName(port, serverMember);

    await().untilAsserted(
        () -> assertThat(service.getMBeanProxy(objectName, CacheServerMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, CacheServerMXBean.class);
  }
}
