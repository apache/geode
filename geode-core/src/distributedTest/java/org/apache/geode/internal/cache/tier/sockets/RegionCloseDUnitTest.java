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

import static org.apache.geode.cache.CacheFactory.getAnyInstance;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Test to verify that client side region.close() should unregister the client with the server. It
 * also checks that client region queue also gets removed properly.
 */
@Category({ClientSubscriptionTest.class})
public class RegionCloseDUnitTest extends JUnit4DistributedTestCase {

  VM server1 = null;

  VM client1 = null;

  private int PORT1;

  private static final String REGION_NAME = "RegionCloseDUnitTest_region";

  protected static String clientMembershipId;

  private static Cache cache = null;

  /** constructor */
  public RegionCloseDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);
    // Client 1 VM
    client1 = host.getVM(1);

    PORT1 = ((Integer) server1.invoke(() -> RegionCloseDUnitTest.createServerCache())).intValue();
    client1.invoke(() -> RegionCloseDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(host), new Integer(PORT1)));
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }


  @Test
  public void testCloseRegionOnClient() {
    server1.invoke(() -> RegionCloseDUnitTest.VerifyClientProxyOnServerBeforeClose());
    client1.invoke(() -> RegionCloseDUnitTest.closeRegion());
    // pause(10000);
    server1.invoke(() -> RegionCloseDUnitTest.VerifyClientProxyOnServerAfterClose());
  }

  public static void createClientCache(String host, Integer port1) throws Exception {
    int PORT1 = port1.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new RegionCloseDUnitTest().createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, PORT1).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1).setReadTimeout(2000).setThreadLocalConnections(true)
        .setSocketBufferSize(1000).setMinConnections(2)
        // .setRetryAttempts(2)
        // .setRetryInterval(250)
        .create("RegionCloseDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();

    cache.createRegion(REGION_NAME, attrs);
  }

  public static Integer createServerCache() throws Exception {
    new RegionCloseDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  public static void VerifyClientProxyOnServerBeforeClose() {
    Cache c = getAnyInstance();
    assertEquals("More than one CacheServer", 1, c.getCacheServers().size());


    final CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return bs.getAcceptor().getCacheClientNotifier().getClientProxies().size() == 1;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    assertEquals(1, bs.getAcceptor().getCacheClientNotifier().getClientProxies().size());

    Iterator iter = bs.getAcceptor().getCacheClientNotifier().getClientProxies().iterator();
    if (iter.hasNext()) {
      CacheClientProxy proxy = (CacheClientProxy) iter.next();
      clientMembershipId = proxy.getProxyID().toString();
      assertNotNull(proxy.getHARegion());
    }
  }

  public static void closeRegion() {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);
      String poolName = r.getAttributes().getPoolName();
      assertNotNull(poolName);
      Pool pool = PoolManager.find(poolName);
      assertNotNull(pool);
      r.close();
      pool.destroy();
    } catch (Exception ex) {
      Assert.fail("failed while region close", ex);
    }
  }

  public static void VerifyClientProxyOnServerAfterClose() {
    final Cache c = getAnyInstance();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return c.getCacheServers().size() == 1;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);

    final CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
    ev = new WaitCriterion() {
      public boolean done() {
        return c.getRegion("/" + clientMembershipId) == null;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);

    ev = new WaitCriterion() {
      public boolean done() {
        return bs.getAcceptor().getCacheClientNotifier().getClientProxies().size() != 1;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    // assertNull(c.getRegion("/"+clientMembershipId));
    assertEquals(0, bs.getAcceptor().getCacheClientNotifier().getClientProxies().size());
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    // close client
    client1.invoke(() -> RegionCloseDUnitTest.closeCache());
    // close server
    server1.invoke(() -> RegionCloseDUnitTest.closeCache());
  }
}
