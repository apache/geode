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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.GemFireCacheImpl.getInstance;
import static org.apache.geode.management.ManagementService.getExistingManagementService;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * This is for testing subscriptions
 */

@SuppressWarnings("serial")
public class TestSubscriptionsDUnitTest extends ManagementTestBase {

  private static final String REGION_NAME =
      TestSubscriptionsDUnitTest.class.getSimpleName() + "_Region";

  private static final String KEY1 = "k1";
  private static final String KEY2 = "k2";
  private static final String CLIENT_VALUE1 = "client-k1";
  private static final String CLIENT_VALUE2 = "client-k2";

  private static VM server = null;
  private static VM client = null;
  private static VM client2 = null;

  @Override
  public final void postSetUpManagementTestBase() throws Exception {
    server = getHost(0).getVM(1);
    client = getHost(0).getVM(2);
    client2 = getHost(0).getVM(3);
  }

  @Test
  public void testNumSubscriptions() throws Exception {
    createManagementCache(managingNode);
    startManagingNode(managingNode);

    int port = createServerCache(server);
    getMember(server);

    createClientCache(client, getServerHostName(server.getHost()), port);
    createClientCache(client2, getServerHostName(server.getHost()), port);

    put(client);
    put(client2);

    registerInterest(client);
    registerInterest(client2);

    verifyNumSubscriptions(managingNode);

    stopManagingNode(managingNode);
  }

  private int createServerCache(VM vm) {
    return vm.invoke("Create Server Cache in TestSubscriptionsDUnitTest", () -> {
      return createServerCache();
    });
  }

  private void createClientCache(VM vm, final String host, final int port1) {
    vm.invoke("Create Client Cache in TestSubscriptionsDUnitTest", () -> {
      createClientCache(host, port1);
    });
  }

  private Cache createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    Cache cache = CacheFactory.create(ds);
    return cache;
  }

  private int createServerCache(DataPolicy dataPolicy) throws Exception {
    Cache cache = createCache(false);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(dataPolicy);

    cache.createRegion(REGION_NAME, factory.create());

    CacheServer server1 = cache.addCacheServer();
    server1.setPort(0);
    server1.setNotifyBySubscription(true);
    server1.start();

    return server1.getPort();
  }

  private int createServerCache() throws Exception {
    return createServerCache(DataPolicy.REPLICATE);
  }

  private Cache createClientCache(String host, int port1) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    Cache cache = createCache(props);

    PoolImpl p =
        (PoolImpl) PoolManager.createFactory().addServer(host, port1).setSubscriptionEnabled(true)
            .setThreadLocalConnections(true).setMinConnections(1).setReadTimeout(20000)
            .setPingInterval(10000).setRetryAttempts(1).setSubscriptionEnabled(true)
            .setStatisticInterval(1000).create("TestSubscriptionsDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    return cache;
  }

  private void verifyNumSubscriptions(final VM vm) {
    vm.invoke("TestSubscriptionsDUnitTest Verify Cache Server Remote", () -> {
      final GemFireCacheImpl cache = getInstance();

      GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
        @Override
        public boolean done() {
          ManagementService service = getExistingManagementService(cache);
          DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();
          return distributedSystemMXBean != null
              & distributedSystemMXBean.getNumSubscriptions() > 1;
        }

        @Override
        public String description() {
          return "TestSubscriptionsDUnitTest wait for getDistributedSystemMXBean to complete and get results";
        }
      });

      DistributedSystemMXBean distributedSystemMXBean =
          getExistingManagementService(cache).getDistributedSystemMXBean();
      assertNotNull(distributedSystemMXBean);
      assertEquals(2, distributedSystemMXBean.getNumSubscriptions());
    });
  }

  private void registerInterest(final VM vm) {
    vm.invoke("TestSubscriptionsDUnitTest registerInterest", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      Region<Object, Object> region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);

      region.registerInterest(KEY1);
      region.registerInterest(KEY2);
    });
  }

  private void put(final VM vm) {
    vm.invoke("put", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);

      region.put(KEY1, CLIENT_VALUE1);
      assertEquals(CLIENT_VALUE1, region.getEntry(KEY1).getValue());

      region.put(KEY2, CLIENT_VALUE2);
      assertEquals(CLIENT_VALUE2, region.getEntry(KEY2).getValue());
    });
  }
}
