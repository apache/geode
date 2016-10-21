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
package org.apache.geode.cache;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * verifies the count of clear operation
 */
@Category(DistributedTest.class)
public class CacheRegionClearStatsDUnitTest extends JUnit4DistributedTestCase {

  /** the cache */
  private static GemFireCacheImpl cache = null;

  private VM server1 = null;

  private static VM client1 = null;

  /** name of the test region */
  private static final String REGION_NAME = "CacheRegionClearStatsDUnitTest_Region";

  private static final String k1 = "k1";

  private static final String k2 = "k2";

  private static final String client_k1 = "client-k1";

  private static final String client_k2 = "client-k2";

  private static final int clearOp = 2;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    client1 = host.getVM(1);
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = (GemFireCacheImpl) CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, Integer port1) throws Exception {
    new CacheRegionClearStatsDUnitTest();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new CacheRegionClearStatsDUnitTest().createCache(props);
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, port1.intValue())
        .setSubscriptionEnabled(false).setThreadLocalConnections(true).setMinConnections(1)
        .setReadTimeout(20000).setPingInterval(10000).setRetryAttempts(1)
        .create("CacheRegionClearStatsDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    // region.registerInterest("ALL_KEYS");
  }

  public static Integer createServerCacheDisk() throws Exception {
    return createCache(DataPolicy.PERSISTENT_REPLICATE);
  }

  private static Integer createCache(DataPolicy dataPolicy) throws Exception {
    new CacheRegionClearStatsDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(dataPolicy);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  public static Integer createServerCache() throws Exception {
    return createCache(DataPolicy.REPLICATE);
  }

  public static void createClientCacheDisk(String host, Integer port1) throws Exception {
    new CacheRegionClearStatsDUnitTest();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new CacheRegionClearStatsDUnitTest().createCache(props);
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, port1.intValue())
        .setSubscriptionEnabled(false).setThreadLocalConnections(true).setMinConnections(1)
        .setReadTimeout(20000).setPingInterval(10000).setRetryAttempts(1)
        .create("CacheRegionClearStatsDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    // region.registerInterest("ALL_KEYS");
  }

  /**
   * This test does the following (<b> clear stats counter </b>):<br>
   * 1)Verifies that clear operation count matches with stats count<br>
   */
  @Test
  public void testClearStatsWithNormalRegion() {
    Integer port1 =
        ((Integer) server1.invoke(() -> CacheRegionClearStatsDUnitTest.createServerCache()));

    client1.invoke(() -> CacheRegionClearStatsDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), port1));
    client1.invoke(() -> CacheRegionClearStatsDUnitTest.put());

    try {
      Thread.sleep(10000);
    } catch (Exception e) {
      // sleep
    }

    client1.invoke(() -> CacheRegionClearStatsDUnitTest.validationClearStat());

    server1.invoke(() -> CacheRegionClearStatsDUnitTest.validationClearStat());
  }

  /**
   * This test does the following (<b> clear stats counter when disk involved </b>):<br>
   * 1)Verifies that clear operation count matches with stats count <br>
   */
  @Test
  public void testClearStatsWithDiskRegion() {
    Integer port1 =
        ((Integer) server1.invoke(() -> CacheRegionClearStatsDUnitTest.createServerCacheDisk()));

    client1.invoke(() -> CacheRegionClearStatsDUnitTest
        .createClientCacheDisk(NetworkUtils.getServerHostName(server1.getHost()), port1));
    client1.invoke(() -> CacheRegionClearStatsDUnitTest.put());

    try {
      Thread.sleep(10000);
    } catch (Exception e) {
      // sleep
    }

    client1.invoke(() -> CacheRegionClearStatsDUnitTest.validationClearStat());

    server1.invoke(() -> CacheRegionClearStatsDUnitTest.validationClearStat());
  }

  @Override
  public final void preTearDown() throws Exception {
    client1.invoke(() -> CacheRegionClearStatsDUnitTest.closeCache());
    // then close the servers
    server1.invoke(() -> CacheRegionClearStatsDUnitTest.closeCache());
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void put() {
    try {
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
      try {
        Thread.sleep(10000);
      } catch (Exception e) {
        // sleep
      }
      r1.clear();
    } catch (Exception ex) {
      Assert.fail("failed while put", ex);
    }
  }

  public static void validationClearStat() {
    assertEquals(cache.getCachePerfStats().getClearCount(), clearOp);
  }


}
