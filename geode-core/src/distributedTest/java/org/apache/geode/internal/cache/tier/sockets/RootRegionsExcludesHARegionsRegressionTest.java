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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Verifies that {@link RegionService#rootRegions()} does not return any {@link HARegion}s.
 *
 * <p>
 * TRAC #37805: Cache level API is required, which will return rootRegions() excluding all Meta
 * Regions and HA Regions
 */
@Category({ClientServerTest.class})
public class RootRegionsExcludesHARegionsRegressionTest implements Serializable {

  private static final String DURABLE_CLIENT_TIMEOUT_VALUE = String.valueOf(600);

  private String uniqueName;
  private String regionName;
  private String hostName;

  private int port;

  private VM server;
  private VM client;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server = getVM(0);
    client = getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    hostName = getHostName();

    port = server.invoke(() -> createServer());
    client.invoke(() -> createCacheClient());
  }

  @Test
  public void rootRegionsExcludesHARegions() {
    server.invoke(() -> validateRootRegions());
  }

  private int createServer() throws IOException {
    cacheRule.createCache();

    RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
    regionFactory.setEnableSubscriptionConflation(true);
    regionFactory.create(regionName);

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(port);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void createCacheClient() {
    Properties config = new Properties();
    config.setProperty(DURABLE_CLIENT_ID, uniqueName);
    config.setProperty(DURABLE_CLIENT_TIMEOUT, DURABLE_CLIENT_TIMEOUT_VALUE);

    clientCacheRule.createClientCache(config);

    PoolFactoryImpl poolFactory = (PoolFactoryImpl) PoolManager.createFactory();
    poolFactory.addServer(hostName, port).setSubscriptionEnabled(true).setSubscriptionRedundancy(0);

    ClientRegionFactory<?, ?> clientRegionFactory =
        clientCacheRule.getClientCache().createClientRegionFactory(LOCAL);
    clientRegionFactory.setPoolName(poolFactory.create(uniqueName).getName());

    clientRegionFactory.create(regionName);

    clientCacheRule.getClientCache().readyForEvents();
  }

  private void validateRootRegions() {
    Set<Region<?, ?>> regions = cacheRule.getCache().rootRegions();
    if (regions != null) {
      for (Region<?, ?> region : regions) {
        assertThat(region).isNotInstanceOf(HARegion.class);
      }
    }
  }
}
