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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.test.dunit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientCacheConnection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class WANLocatorServerDUnitTest extends WANTestBase {

  static PoolImpl proxy;

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    final Host host = Host.getHost(0);
  }

  @Test
  public void test_3Locators_2Servers() {

    int port1 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();

    int port2 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();

    int port3 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();

    vm0.invoke(() -> WANLocatorServerDUnitTest.createLocator(port1, port2, port3, port1));

    vm1.invoke(() -> WANLocatorServerDUnitTest.createLocator(port1, port2, port3, port2));

    vm2.invoke(() -> WANLocatorServerDUnitTest.createLocator(port1, port2, port3, port3));

    vm3.invoke(() -> WANLocatorServerDUnitTest.createReceiver(port1, port2, port3));
    vm5.invoke(() -> WANLocatorServerDUnitTest.createClient(port1, port2, port3));

    vm0.invoke(() -> WANLocatorServerDUnitTest.disconnect());
    vm1.invoke(() -> WANLocatorServerDUnitTest.disconnect());
    vm2.invoke(() -> WANLocatorServerDUnitTest.disconnect());

    vm0.invoke(() -> WANLocatorServerDUnitTest.createLocator(port1, port2, port3, port1));

    vm1.invoke(() -> WANLocatorServerDUnitTest.createLocator(port1, port2, port3, port2));

    vm2.invoke(() -> WANLocatorServerDUnitTest.createLocator(port1, port2, port3, port3));

    vm5.invoke(() -> WANLocatorServerDUnitTest.tryNewConnection());
  }

  public static void createLocator(Integer port1, Integer port2, Integer port3,
      Integer startingPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    props.setProperty(LOCATORS,
        "localhost[" + port1 + "],localhost[" + port2 + "],localhost[" + port3 + "]");
    props.setProperty(START_LOCATOR,
        "localhost[" + startingPort + "],server=true,peer=true,hostname-for-clients=localhost");
    test.getSystem(props);
  }

  public static void createReceiver(Integer port1, Integer port2, Integer port3) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS,
        "localhost[" + port1 + "],localhost[" + port2 + "],localhost[" + port3 + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + port, e);
    }
  }

  public static void createServer(Integer port1, Integer port2, Integer port3) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS,
        "localhost[" + port1 + "],localhost[" + port2 + "],localhost[" + port3 + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    CacheServer server = cache.addCacheServer();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    server.setPort(port);
    try {
      server.start();
    } catch (IOException e) {
      fail("Test " + test.getName() + " failed to start CacheServer on port " + port, e);
    }
    LogWriterUtils.getLogWriter()
        .info("Server Started on port : " + port + " : server : " + server);
  }

  public static void disconnect() {
    WANTestBase test = new WANTestBase();
    test.getSystem().disconnect();
  }

  public static void createClient(Integer port1, Integer port2, Integer port3) {
    ClientCacheFactory cf = new ClientCacheFactory();
    cache = (Cache) cf.create();
    PoolFactoryImpl pf = (PoolFactoryImpl) PoolManager.createFactory();
    pf.setReadTimeout(0);
    pf.setIdleTimeout(-1);
    pf.setMinConnections(4);
    pf.setServerGroup(GatewayReceiver.RECEIVER_GROUP);
    pf.addLocator("localhost", port1);
    pf.addLocator("localhost", port2);
    pf.addLocator("localhost", port3);
    pf.init((GatewaySender) null);
    proxy = ((PoolImpl) pf.create("KISHOR_POOL"));
    ClientCacheConnection con1 = proxy.acquireConnection();
    try {
      con1.close(true);
    } catch (Exception e) {
      fail("createClient failed", e);
    }
  }

  public static void tryNewConnection() {
    ClientCacheConnection con1 = null;
    try {
      con1 = proxy.acquireConnection();
    } catch (Exception e) {
      Assert.fail("No Exception expected", e);
    }

  }
}
