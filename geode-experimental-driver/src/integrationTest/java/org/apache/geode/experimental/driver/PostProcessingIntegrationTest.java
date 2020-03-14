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
import java.util.Arrays;
import java.util.List;
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
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class PostProcessingIntegrationTest {
  public static final String SPAM = "Spam!";
  public static final String REGION_NAME = "region";
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private Locator locator;
  private Cache cache;
  private Driver driver;
  private org.apache.geode.cache.Region<Object, Object> serverRegion;

  @Before
  public void createServer() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    // Create a cache
    CacheFactory cf = new CacheFactory();
    cf.set(ConfigurationProperties.MCAST_PORT, "0");
    cf.setSecurityManager(new SimpleSecurityManager());
    cf.set(ConfigurationProperties.SECURITY_POST_PROCESSOR, SpamPostProcessor.class.getName());
    cache = cf.create();

    // Start a locator
    locator = Locator.startLocatorAndDS(0, null, new Properties());
    int locatorPort = locator.getPort();

    serverRegion = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");

    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.start();

    driver = new DriverFactory().addLocator("localhost", locatorPort).setUsername("cluster,data")
        .setPassword("cluster,data").create();
  }

  @After
  public void cleanup() {
    locator.stop();
    cache.close();
  }


  @Test
  public void getResultIsPostProcessed() throws Exception {
    serverRegion.put("key", "value");
    Region<String, String> region = driver.getRegion(REGION_NAME);
    assertEquals(SPAM, region.get("key"));
  }

  @Test
  public void queryResultIsPostProcessed() throws IOException {
    serverRegion.put("key1", "value1");
    serverRegion.put("key2", "value2");

    QueryService service = driver.getQueryService();

    Query<String> query = service.newQuery("select value from /region value order by value");
    final List<String> results = query.execute();

    assertEquals(Arrays.asList(SPAM, SPAM), results);
  }

  public static class SpamPostProcessor implements PostProcessor {

    @Override
    public Object processRegionValue(Object principal, String regionName, Object key,
        Object value) {
      return SPAM;
    }
  }
}
