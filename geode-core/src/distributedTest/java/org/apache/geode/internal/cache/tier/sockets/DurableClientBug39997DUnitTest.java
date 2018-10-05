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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class DurableClientBug39997DUnitTest extends JUnit4CacheTestCase {

  public final void postTearDownCacheTestCase() {
    Host.getHost(0).getVM(0).invoke(() -> disconnectFromDS());
  }

  @Test
  public void testNoServerAvailableOnStartup() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final String hostName = NetworkUtils.getServerHostName(host);
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    vm0.invoke(new SerializableRunnable("create cache") {
      public void run() {
        getSystem(getClientProperties());
        PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(hostName, port)
            .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
            .create("DurableClientReconnectDUnitTestPool");
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        factory.setPoolName(p.getName());
        Cache cache = getCache();
        Region region1 = cache.createRegion("region", factory.create());
        cache.readyForEvents();

        try {
          region1.registerInterest("ALL_KEYS");
          fail("Should have received an exception trying to register interest");
        } catch (NoSubscriptionServersAvailableException expected) {
          // this is expected
        }
      }
    });

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        cache.createRegion("region", factory.create());
        CacheServer server = cache.addCacheServer();
        server.setPort(port);
        try {
          server.start();
        } catch (IOException e) {
          Assert.fail("couldn't start server", e);
        }
      }
    });

    vm0.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        final Region region = cache.getRegion("region");
        GeodeAwaitility.await().untilAsserted(new WaitCriterion() {

          public String description() {
            return "Wait for register interest to succeed";
          }

          public boolean done() {
            try {
              region.registerInterest("ALL_KEYS");
            } catch (NoSubscriptionServersAvailableException e) {
              return false;
            }
            return true;
          }

        });
      }
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
