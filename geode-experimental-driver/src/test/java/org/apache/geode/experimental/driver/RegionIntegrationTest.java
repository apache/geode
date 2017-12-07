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
package org.apache.geode.experimental.driver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.Locator;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class RegionIntegrationTest {
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  public static final String REGION = "region";
  private int locatorPort;
  private Locator locator;
  private CacheServer server;
  private Cache cache;

  @Before
  public void createServer() throws IOException {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    // Create a cache
    CacheFactory cf = new CacheFactory();
    cache = cf.create();

    // Start a locator
    locator = Locator.startLocatorAndDS(0, null, new Properties());
    locatorPort = locator.getPort();

    // Start a server
    server = cache.addCacheServer();
    server.setPort(0);
    server.start();

    // Create a region
    cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION);
  }

  @After
  public void cleanup() {
    locator.stop();
    cache.close();
  }

  @Test
  public void getShouldReturnPutValue() throws Exception {
    Driver driver = new DriverFactory().addLocator("localhost", locatorPort).create();
    Region region = driver.getRegion("region");

    region.put("key", "value");
    assertEquals("value", region.get("key"));

    region.remove("key");
    assertEquals(null, region.get("key"));
  }

  @Test
  public void putWithIntegerKey() throws Exception {
    Driver driver = new DriverFactory().addLocator("localhost", locatorPort).create();
    Region region = driver.getRegion("region");
    region.put(37, 42);
    assertEquals(42, region.get(37));
  }

  @Test
  public void removeWithIntegerKey() throws Exception {
    Driver driver = new DriverFactory().addLocator("localhost", locatorPort).create();
    Region region = driver.getRegion("region");
    region.put(37, 42);
    region.remove(37);
    assertEquals(null, region.get(37));
  }
}
