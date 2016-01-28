/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.wan.misc;

import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.Host;

public class WANLocatorServerDUnitTest extends WANTestBase {

  static PoolImpl proxy;

  public WANLocatorServerDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);

  }

  public void test_3Locators_2Servers() {

    int port1 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();

    int port2 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();

    int port3 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();

    vm0.invoke(WANLocatorServerDUnitTest.class, "createLocator", new Object[] {
        port1, port2, port3, port1 });

    vm1.invoke(WANLocatorServerDUnitTest.class, "createLocator", new Object[] {
        port1, port2, port3, port2 });

    vm2.invoke(WANLocatorServerDUnitTest.class, "createLocator", new Object[] {
        port1, port2, port3, port3 });

    vm3.invoke(WANLocatorServerDUnitTest.class, "createReceiver", new Object[] {
        port1, port2, port3 });
    vm5.invoke(WANLocatorServerDUnitTest.class, "createClient", new Object[] {
        port1, port2, port3 });

    vm0.invoke(WANLocatorServerDUnitTest.class, "disconnect");
    vm1.invoke(WANLocatorServerDUnitTest.class, "disconnect");
    vm2.invoke(WANLocatorServerDUnitTest.class, "disconnect");

    vm0.invoke(WANLocatorServerDUnitTest.class, "createLocator", new Object[] {
        port1, port2, port3, port1 });

    vm1.invoke(WANLocatorServerDUnitTest.class, "createLocator", new Object[] {
        port1, port2, port3, port2 });

    vm2.invoke(WANLocatorServerDUnitTest.class, "createLocator", new Object[] {
        port1, port2, port3, port3 });

    vm5.invoke(WANLocatorServerDUnitTest.class, "tryNewConnection");

  }

  public static void createLocator(Integer port1, Integer port2, Integer port3,
      Integer startingPort) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "" + 1);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port1
        + "],localhost[" + port2 + "],localhost[" + port3 + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost["
        + startingPort
        + "],server=true,peer=true,hostname-for-clients=localhost");
    test.getSystem(props);
  }

  public static void createReceiver(Integer port1, Integer port2, Integer port3) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port1
        + "],localhost[" + port2 + "],localhost[" + port3 + "]");

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
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName()
          + " failed to start GatewayRecevier on port " + port);
    }
  }

  public static void createServer(Integer port1, Integer port2, Integer port3) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port1
        + "],localhost[" + port2 + "],localhost[" + port3 + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    CacheServer server = cache.addCacheServer();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    server.setPort(port);
    try {
      server.start();
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start CacheServer on port "
          + port);
    }
    getLogWriter().info(
        "Server Started on port : " + port + " : server : " + server);
  }

  public static void disconnect() {
    WANTestBase test = new WANTestBase(testName);
    test.getSystem().disconnect();
  }

  public static void createClient(Integer port1, Integer port2, Integer port3) {
    ClientCacheFactory cf = new ClientCacheFactory();
    cache = (Cache)cf.create();
    PoolFactoryImpl pf = (PoolFactoryImpl)PoolManager.createFactory();
    pf.setReadTimeout(0);
    pf.setIdleTimeout(-1);
    pf.setMinConnections(4);
    pf.setServerGroup(GatewayReceiver.RECEIVER_GROUP);
    pf.addLocator("localhost", port1);
    pf.addLocator("localhost", port2);
    pf.addLocator("localhost", port3);
    pf.init((GatewaySender)null);
    proxy = ((PoolImpl)pf.create("KISHOR_POOL"));
    Connection con1 = proxy.acquireConnection();
    try {
      con1.close(true);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void tryNewConnection() {
    Connection con1 = null;
    try {
      con1 = proxy.acquireConnection();
    }
    catch (Exception e) {
      fail("No Exception expected", e);
    }

  }
}
