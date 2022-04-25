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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

@Tag("ClientSubscriptionTest")
public class DurableClientNoServerAvailabileDUnitTest extends JUnit4CacheTestCase {

  @Override
  public final void postTearDownCacheTestCase() {
    VM.getVM(0).invoke(JUnit4DistributedTestCase::disconnectFromDS);
  }

  @Test
  public void testNoServerAvailableOnStartup() {
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
      RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
      regionFactory.setScope(Scope.LOCAL);
      regionFactory.setPoolName(p.getName());
      Region<Object, Object> region1 = regionFactory.create("region");
      assertThrows(NoSubscriptionServersAvailableException.class,
          () -> region1.registerInterestForAllKeys());

    });

    vm1.invoke(() -> {
      Cache cache = getCache();
      RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);
      regionFactory.create("region");
      CacheServer server = cache.addCacheServer();
      server.setPort(port);
      assertDoesNotThrow(server::start);
    });

    vm0.invoke(() -> {
      Cache cache = getCache();
      final Region<Object, Object> region = cache.getRegion("region");
      GeodeAwaitility.await("Wait for register interest to succeed")
          .untilAsserted(() -> assertDoesNotThrow(() -> region.registerInterestForAllKeys()));
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
