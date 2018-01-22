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

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

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
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class RegionIntegrationTest {
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  /** a JSON document */
  private static final String jsonDocument =
      "{" + System.lineSeparator() + "  \"name\" : \"Charlemagne\"," + System.lineSeparator()
          + "  \"age\" : 1276," + System.lineSeparator() + "  \"nationality\" : \"french\","
          + System.lineSeparator() + "  \"emailAddress\" : \"none\"" + System.lineSeparator() + "}";


  private static final String REGION = "region";
  private Locator locator;
  private Cache cache;
  private Driver driver;

  private org.apache.geode.cache.Region<Object, Object> serverRegion;

  @Before
  public void createServerAndDriver() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    // Create a cache
    CacheFactory cf = new CacheFactory();
    cf.set(ConfigurationProperties.MCAST_PORT, "0");
    cache = cf.create();

    // Start a locator
    locator = Locator.startLocatorAndDS(0, null, new Properties());
    int locatorPort = locator.getPort();

    // Start a server
    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.start();

    // Create a region
    serverRegion = cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION);

    // Create a driver connected to the server
    driver = new DriverFactory().addLocator("localhost", locatorPort).create();

  }

  @After
  public void cleanup() {
    locator.stop();
    cache.close();
  }



  @Test
  public void getShouldReturnPutValue() throws Exception {
    Region<String, String> region = driver.getRegion("region");

    region.put("key", "value");
    assertEquals("value", region.get("key"));

    region.remove("key");
    assertFalse(serverRegion.containsKey("key"));
    assertNull(region.get("key"));
  }

  @Test
  public void putWithIntegerKey() throws Exception {
    Region<Integer, Integer> region = driver.getRegion("region");
    region.put(37, 42);
    assertTrue(serverRegion.containsKey(37));
    assertEquals(42, region.get(37).intValue());
  }

  @Test
  public void removeWithIntegerKey() throws Exception {
    Region<Integer, Integer> region = driver.getRegion("region");
    region.put(37, 42);
    region.remove(37);
    assertFalse(serverRegion.containsKey(37));
    assertNull(region.get(37));
  }

  @Test
  public void putWithJSONKeyAndValue() throws Exception {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    JSONWrapper document = JSONWrapper.wrapJSON(jsonDocument);
    region.put(document, document);
    JSONWrapper value = region.get(document);
    assertEquals(document, value);

    assertEquals(1, serverRegion.size());
    org.apache.geode.cache.Region.Entry entry =
        (org.apache.geode.cache.Region.Entry) serverRegion.entrySet().iterator().next();
    assertTrue(PdxInstance.class.isAssignableFrom(entry.getKey().getClass()));
    assertTrue(PdxInstance.class.isAssignableFrom(entry.getValue().getClass()));
  }

  @Test
  public void removeWithJSONKey() throws Exception {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    JSONWrapper document = JSONWrapper.wrapJSON(jsonDocument);
    region.put(document, document);
    region.remove(document);
    assertEquals(0, serverRegion.size());
    assertNull(region.get(document));
  }

}
