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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.cache.*;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;


@Category({DistributedTest.class, ClientServerTest.class})
@RunWith(JUnitParamsRunner.class)
public class ClientServerMisc2DUnitTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();

  private static int PORT1;
  private static VM server1;
  private static Host host;

  @Override
  public final void postSetUp() throws Exception {
    host = Host.getHost(0);
    server1 = host.getVM(2);
  }

  @Test
  @Parameters(method = "regionShortcut")
  public void testProxyRegionClientServerOp(RegionShortcut shortcut) throws Exception {
    // start server first
    final String REGION_NAME = "proxyRegionClientServerOp";
    PORT1 = initServerCache(false);
    // Create regions on servers.
    server1.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region<Object, Object> region = cache.createRegionFactory(shortcut).create(REGION_NAME);
      assertNotNull(region);
    });

    String host = NetworkUtils.getServerHostName(server1.getHost());

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.addPoolServer(host, PORT1);

    ClientCache clientCache = ccf.create();
    Region<Object, Object> clientRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
    assertNotNull(clientRegion);

    // let us populate this region from client.
    for (int i = 0; i < 10; i++) {
      clientRegion.put(i, i * 10);
    }
    // Verify using gets
    for (int i = 0; i < 10; i++) {
      assertEquals(i * 10, clientRegion.get(i));
    }
    assertEquals(10, clientRegion.size());
    assertFalse(clientRegion.isEmpty());
    // delete all the entries from the server
    server1.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region<Object, Object> region = cache.getRegion(REGION_NAME);
      assertNotNull(region);
      for (int i = 0; i < 10; i++) {
        region.remove(i);
      }
    });
    assertEquals(0, clientRegion.size());
    assertTrue(clientRegion.isEmpty());

    clientRegion.destroyRegion();
    clientCache.close();
  }

  private RegionShortcut[] regionShortcut() {
    return new RegionShortcut[]{RegionShortcut.PARTITION, RegionShortcut.REPLICATE};
  }

  private int initServerCache(boolean notifyBySub) {
    return ((Integer) server1.invoke(ClientServerMisc2DUnitTest.class, "createServerCache"))
        .intValue();
  }

  public static Integer createServerCache()
      throws Exception {
    Cache cache = new ClientServerMisc2DUnitTest().createCacheV(new Properties());
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    logger.info("Starting server on port " + port);
    server.setPort(port);
    server.start();
    logger
        .info("Started server on port " + server.getPort());
    return new Integer(server.getPort());

  }

  protected Cache createCacheV(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    Cache cache = getCache();
    assertNotNull(cache);
    return cache;
  }

  protected int getMaxThreads() {
    return 0;
  }

}
