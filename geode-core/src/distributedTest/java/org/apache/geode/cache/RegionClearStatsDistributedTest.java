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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * verifies the count of clear operation
 */
@SuppressWarnings("serial")
public class RegionClearStatsDistributedTest implements Serializable {

  private static final String REGION_NAME = RegionClearStatsDistributedTest.class.getSimpleName();
  private static final String KEY1 = "k1";
  private static final String KEY2 = "k2";
  private static final String VALUE1 = "client-k1";
  private static final String VALUE2 = "client-k2";
  private static final int EXPECTED_CLEAR_COUNT_STAT_VALUE = 2;

  private VM server1;
  private VM client1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() throws Exception {
    server1 = getVM(0);
    client1 = getVM(1);
  }

  /**
   * Verifies that clear operation count matches with stats count
   */
  @Test
  public void testClearStatsWithNormalRegion() {
    int port = server1.invoke(() -> createServerCache());
    client1.invoke(() -> createClientCache(getServerHostName(), port));

    client1.invoke(() -> doPutsAndClear());
    client1.invoke(() -> doPutsAndClear());

    client1.invoke(() -> validateClearCountStat());
    server1.invoke(() -> validateClearCountStat());
  }

  /**
   * Verifies that clear operation count matches with stats count with persistence
   */
  @Test
  public void testClearStatsWithDiskRegion() {
    int port = server1.invoke(() -> createServerCacheWithPersistence());
    client1.invoke(() -> createClientCacheWithPersistence(getServerHostName(), port));

    client1.invoke(() -> doPutsAndClear());
    client1.invoke(() -> doPutsAndClear());

    client1.invoke(() -> validateClearCountStat());
    server1.invoke(() -> validateClearCountStat());
  }

  private int createCache(DataPolicy dataPolicy) throws IOException {
    cacheRule.createCache();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(dataPolicy);

    cacheRule.getCache().createRegion(REGION_NAME, factory.create());

    CacheServer server1 = cacheRule.getCache().addCacheServer();
    server1.setPort(0);
    server1.setNotifyBySubscription(true);
    server1.start();
    return server1.getPort();
  }

  private int createServerCacheWithPersistence() throws IOException {
    return createCache(DataPolicy.PERSISTENT_REPLICATE);
  }

  private int createServerCache() throws IOException {
    return createCache(DataPolicy.REPLICATE);
  }

  private void createClientCache(String host, int port) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");

    cacheRule.createCache(config);

    PoolImpl pool =
        (PoolImpl) PoolManager.createFactory().addServer(host, port).setSubscriptionEnabled(false)
            .setThreadLocalConnections(true).setMinConnections(1).setReadTimeout(20000)
            .setPingInterval(10000).setRetryAttempts(1).create(getClass().getSimpleName());

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(pool.getName());

    cacheRule.getCache().createRegion(REGION_NAME, factory.create());
  }

  private void createClientCacheWithPersistence(String host, int port) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    cacheRule.createCache(props);

    PoolImpl pool =
        (PoolImpl) PoolManager.createFactory().addServer(host, port).setSubscriptionEnabled(false)
            .setThreadLocalConnections(true).setMinConnections(1).setReadTimeout(20000)
            .setPingInterval(10000).setRetryAttempts(1).create(getClass().getSimpleName());

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(pool.getName());
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);

    cacheRule.getCache().createRegion(REGION_NAME, factory.create());
  }

  private void doPutsAndClear() {
    Region region = cacheRule.getCache().getRegion(REGION_NAME);

    region.put(KEY1, VALUE1);
    region.put(KEY2, VALUE2);

    assertThat(region.getEntry(KEY1).getValue()).isEqualTo(VALUE1);
    assertThat(region.getEntry(KEY2).getValue()).isEqualTo(VALUE2);

    region.clear();
  }

  private void validateClearCountStat() {
    assertThat(cacheRule.getCache().getCachePerfStats().getClearCount())
        .isEqualTo(EXPECTED_CLEAR_COUNT_STAT_VALUE);
  }
}
