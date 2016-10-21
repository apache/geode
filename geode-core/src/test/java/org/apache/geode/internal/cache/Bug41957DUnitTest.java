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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Test for bug 41957. Basic idea is to have a client with a region with a low eviction limit do a
 * register interest with key&values and see if we end up with more entries in the client than the
 * eviction limit.
 *
 * @since GemFire 6.5
 */
@Category(DistributedTest.class)
public class Bug41957DUnitTest extends ClientServerTestCase {

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void testBug41957() {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String serverHost = NetworkUtils.getServerHostName(server.getHost());

    createBridgeServer(server, regionName, serverPort, false);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort});

    client.invoke(new CacheSerializableRunnable("register interest") {
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        int ENTRIES_ON_SERVER = 10;
        for (int i = 1; i <= ENTRIES_ON_SERVER; i++) {
          region.registerInterest("k" + i, InterestResultPolicy.KEYS_VALUES);
        }
        assertEquals(2, region.size());
      }
    });

    stopBridgeServer(server);
  }

  private void createBridgeServer(VM server, final String regionName, final int serverPort,
      final boolean createPR) {
    server.invoke(new CacheSerializableRunnable("Create server") {
      public void run2() throws CacheException {
        // Create DS
        Properties config = new Properties();
        config.setProperty(LOCATORS,
            "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        factory.setCacheLoader(new CacheServerCacheLoader());
        if (createPR) {
          factory.setDataPolicy(DataPolicy.PARTITION);
          factory.setPartitionAttributes((new PartitionAttributesFactory()).create());
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
        }
        Region region = createRootRegion(regionName, factory.create());
        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        }
        int ENTRIES_ON_SERVER = 10;
        for (int i = 1; i <= ENTRIES_ON_SERVER; i++) {
          region.create("k" + i, "v" + i);
        }
        try {
          startBridgeServer(serverPort);
        } catch (Exception e) {
          Assert.fail("While starting CacheServer", e);
        }
      }
    });
  }

  private void createBridgeClient(VM client, final String regionName, final String serverHost,
      final int[] serverPorts) {
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        // Create DS
        Properties config = new Properties();
        config.setProperty(MCAST_PORT, "0");
        config.setProperty(LOCATORS, "");
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        {
          PoolFactory pf = PoolManager.createFactory();
          pf.setSubscriptionEnabled(true);
          for (int i = 0; i < serverPorts.length; i++) {
            pf.addServer(serverHost, serverPorts[i]);
          }
          pf.create("myPool");
        }
        int ENTRIES_ON_CLIENT = 2;
        factory.setPoolName("myPool");
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        factory
            .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(ENTRIES_ON_CLIENT));
        createRootRegion(regionName, factory.create());
      }
    });
  }

  private void stopBridgeServer(VM server) {
    server.invoke(new CacheSerializableRunnable("Stop Server") {
      public void run2() throws CacheException {
        stopBridgeServers(getCache());
      }
    });
  }
}

