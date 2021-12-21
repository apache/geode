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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class RegionCreateDestroyDUnitTest extends JUnit4CacheTestCase {

  private static final String GOOD_REGION_NAME = "Good-Region";
  private static final String BAD_REGION_NAME = "Bad@Region";
  private static final String RESERVED_REGION_NAME = "__ReservedRegion";

  protected VM client1 = null;
  protected VM client2 = null;
  protected VM client3 = null;
  protected int serverPort;

  @Before
  public void before() throws Exception {
    final Host host = Host.getHost(0);
    client1 = host.getVM(1);
    client2 = host.getVM(2);
    client3 = host.getVM(3);

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    getSystem(props);

  }

  private void startServer(final Cache cache) throws IOException {
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(0);
    server1.start();

    serverPort = server1.getPort();
  }

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(() -> closeCache());
    closeCache();
  }

  protected Properties createClientProperties() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(SECURITY_LOG_LEVEL, "finest");
    return props;
  }

  @Test
  public void testCreateDestroyValidRegion() throws InterruptedException {
    Cache serverCache = getCache();
    serverCache.createRegionFactory(RegionShortcut.REPLICATE).create(GOOD_REGION_NAME);

    try {
      startServer(serverCache);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    client1.invoke(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties())
          .setPoolSubscriptionEnabled(true).addPoolServer("localhost", serverPort).create();
      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(GOOD_REGION_NAME);
      region.destroyRegion();
      assertThat(region.isDestroyed()).isTrue();
    });
  }

  @Test
  public void testCreateInvalidRegion() throws InterruptedException {
    Cache serverCache = getCache();
    try {
      serverCache.createRegionFactory(RegionShortcut.REPLICATE).create(BAD_REGION_NAME);
    } catch (IllegalArgumentException iae) {
      assertEquals(
          "Region names may only be alphanumeric and may contain hyphens or underscores: Bad@Region",
          iae.getMessage());
    }

    try {
      startServer(serverCache);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    client1.invoke(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties())
          .setPoolSubscriptionEnabled(true).addPoolServer("localhost", serverPort).create();
      try {
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(BAD_REGION_NAME);
        fail("Should have thrown an IllegalArgumentException");
      } catch (IllegalArgumentException iae) {
        assertEquals(
            "Region names may only be alphanumeric and may contain hyphens or underscores: Bad@Region",
            iae.getMessage());
      }
    });
  }

  @Test
  public void testCreateDestroyReservedRegion() throws InterruptedException {
    Cache serverCache = getCache();
    try {
      serverCache.createRegionFactory(RegionShortcut.REPLICATE).create(RESERVED_REGION_NAME);
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException arg) {
      assertEquals("Region names may not begin with a double-underscore: __ReservedRegion",
          arg.getMessage());
    }
    try {
      startServer(serverCache);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    try {
      client1.invoke(() -> {
        ClientCache cache = new ClientCacheFactory(createClientProperties())
            .setPoolSubscriptionEnabled(true).addPoolServer("localhost", serverPort).create();
        try {
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(RESERVED_REGION_NAME);
          fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
          assertEquals("Region names may not begin with a double-underscore: __ReservedRegion",
              e.getMessage());
        }
      });
    } catch (RMIException rmi) {
      rmi.getCause();
    }

  }
}
