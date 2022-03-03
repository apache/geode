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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This Dunit test is to verify the bug in put() operation. When the put is invoked on the server
 * and NotifyBySubscription is false then it follows normal path and then again calls put of region
 * on which region queue is based. so recurssion is happening.
 */
@Category({ClientSubscriptionTest.class})
public class HABugInPutDUnitTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME = HABugInPutDUnitTest.class.getSimpleName() + "_region";

  private VM server1 = null;
  private VM server2 = null;
  private VM client1 = null;
  private VM client2 = null;

  static final String KEY1 = "KEY1";
  static final String VALUE1 = "VALUE1";

  protected static Cache cache = null;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);

    // Server2 VM
    server2 = host.getVM(1);

    // Client 1 VM
    client1 = host.getVM(2);

    // client 2 VM
    client2 = host.getVM(3);

    // System.setProperty())
    int PORT1 =
        server1.invoke(HABugInPutDUnitTest::createServerCache);
    int PORT2 =
        server2.invoke(HABugInPutDUnitTest::createServerCache);

    client1.invoke(() -> HABugInPutDUnitTest.createClientCache(NetworkUtils.getServerHostName(host),
        PORT1, PORT2));
    client2.invoke(() -> HABugInPutDUnitTest.createClientCache(NetworkUtils.getServerHostName(host),
        PORT1, PORT2));
  }

  @Override
  public final void preTearDown() throws Exception {
    client1.invoke(HABugInPutDUnitTest::closeCache);
    client2.invoke(HABugInPutDUnitTest::closeCache);
    // close server
    server1.invoke(HABugInPutDUnitTest::closeCache);
    server2.invoke(HABugInPutDUnitTest::closeCache);
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static Integer createServerCache() throws Exception {
    new HABugInPutDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServerImpl server = (CacheServerImpl) cache.addCacheServer();
    assertNotNull(server);
    int port = getRandomAvailableTCPPort();
    server.setPort(port);
    server.setNotifyBySubscription(false);
    server.start();
    return server.getPort();
  }

  public static void createClientCache(String hostName, Integer port1, Integer port2)
      throws Exception {
    int PORT1 = port1;
    int PORT2 = port2;
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HABugInPutDUnitTest().createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ClientServerTestCase.configureConnectionPool(factory, hostName, new int[] {PORT1, PORT2}, true,
        -1, 2, null);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.registerInterest(KEY1);
  }

  @Test
  public void testBugInPut() throws Exception {
    client1.invoke(new CacheSerializableRunnable("putFromClient1") {

      @Override
      public void run2() throws CacheException {
        Region region = cache.getRegion(SEPARATOR + REGION_NAME);
        assertNotNull(region);
        region.put(KEY1, VALUE1);
        cache.getLogger().info("Put done successfully");

      }
    });
  }
}
