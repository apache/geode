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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Test for bug 41957. Basic idea is to have a client with a region with a low eviction limit do a
 * register interest with key and values and see if we end up with more entries in the client than
 * the eviction limit.
 *
 * <p>
 * TRAC #41957: Interaction between registerInterest and eviction produces incorrect number of
 * entries in region
 *
 * @since GemFire 6.5
 */
@Category({ClientServerTest.class})
public class RegisterInterestWithEvictionRegressionTest extends ClientServerTestCase {

  private static final int ENTRIES_ON_SERVER = 10;
  private static final int ENTRIES_ON_CLIENT = 2;

  private String uniqueName;
  private String hostName;
  private int serverPort;

  private VM server;
  private VM client;

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server = getHost(0).getVM(0);
    client = getHost(0).getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    hostName = NetworkUtils.getServerHostName(server.getHost());
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void registerInterestKeysValuesShouldHonorEvictionLimit() {
    serverPort = server.invoke(this::createServer);
    client.invoke(this::createClient);

    client.invoke(() -> {
      Region region = getRootRegion(uniqueName);
      for (int i = 1; i <= ENTRIES_ON_SERVER; i++) {
        region.registerInterest("k" + i, InterestResultPolicy.KEYS_VALUES);
      }
      assertThat(region.size()).isEqualTo(2);
    });

    server.invoke(this::stopServer);
  }

  private int createServer() throws IOException {
    AttributesFactory factory = new AttributesFactory();
    factory.setCacheLoader(new CacheServerCacheLoader());
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setScope(Scope.DISTRIBUTED_ACK);

    Region region = createRootRegion(uniqueName, factory.create());

    for (int i = 1; i <= ENTRIES_ON_SERVER; i++) {
      region.create("k" + i, "v" + i);
    }

    return startBridgeServer(0);
  }

  private void createClient() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    getCache(config);

    PoolFactory pf = PoolManager.createFactory();
    pf.setSubscriptionEnabled(true);
    pf.addServer(hostName, serverPort);
    pf.create(uniqueName);

    // Create Region
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    factory.setPoolName(uniqueName);
    factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
    factory.setEvictionAttributes(createLRUEntryAttributes(ENTRIES_ON_CLIENT));
    createRootRegion(uniqueName, factory.create());
  }

  private void stopServer() {
    stopBridgeServers(getCache());
  }
}
