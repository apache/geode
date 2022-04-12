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

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class DurableClientBug39997DUnitTest extends JUnit4CacheTestCase {

  @Override
  public final void postTearDownCacheTestCase() {
    VM.getVM(0).invoke(JUnit4DistributedTestCase::disconnectFromDS);
  }

  @Test
  public void testNoServerAvailableOnStartup() {
    Host host = Host.getHost(0);
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    final String hostName = VM.getHostName();
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    vm0.invoke("create cache", () -> {
      getSystem(getClientProperties());
      PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(hostName, port)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
          .create("DurableClientReconnectDUnitTestPool");
      Cache cache = getCache();
      RegionFactory regionFactory = cache.createRegionFactory();
      regionFactory.setScope(Scope.LOCAL);
      regionFactory.setPoolName(p.getName());
      Region region1 = regionFactory.create("region");

      try {
        region1.registerInterestForAllKeys();
        fail("Should have received an exception trying to register interest");
      } catch (NoSubscriptionServersAvailableException expected) {
        // this is expected
      }
      cache.readyForEvents();

    });

    vm1.invoke(() -> {
      Cache cache = getCache();
      RegionFactory regionFactory = cache.createRegionFactory();
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);
      regionFactory.create("region");
      CacheServer server = cache.addCacheServer();
      server.setPort(port);
      try {
        server.start();
      } catch (IOException e) {
        fail("couldn't start server", e);
      }
    });

    vm0.invoke(() -> {
      Cache cache = getCache();
      final Region region = cache.getRegion("region");
      GeodeAwaitility.await("Wait for register interest to succeed").until(() -> {
        try {
          region.registerInterestForAllKeys();
        } catch (NoSubscriptionServersAvailableException e) {
          return false;
        }
        return true;
      });
    });
  }

  public Properties getClientProperties() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(DURABLE_CLIENT_ID, "my_id");
    return props;
  }
}
